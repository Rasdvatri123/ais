import asyncio
import json
import logging
import os
import threading
import urllib.request
from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
import websockets

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

app = Flask(__name__)
CORS(app)

vessels_data = {}
target_vessels = []

BOUNDING_BOXES = {
    # Расширенная Европа с охватом Атлантики
    "europe": [[[25.0, -25.0], [72.0, 45.0]]],
    "world": [[[-90.0, -180.0], [90.0, 180.0]]],
}

current_region = "europe"
ws_connection = None
async_loop = None

AISSTREAM_API_KEY = os.environ.get(
    "AISSTREAM_API_KEY", "8a2e1e9248eb6650a5d5e16db825de0cc74281e0"
).strip()


def fetch_fallback_vessel(query):
  """Мгновенный точечный поиск судна через публичный API с полными браузерными заголовками."""
  headers = {
      "User-Agent": (
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
          " (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
      ),
      "Accept": "application/json, text/javascript, */*; q=0.01",
      "X-Requested-With": "XMLHttpRequest",
      "Referer": "https://www.myshiptracking.com/",
  }

  url = f"https://www.myshiptracking.com/requests_vessel.php?type=search&term={query}"

  try:
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=5) as response:
      data = json.loads(response.read().decode("utf-8"))
      if data and isinstance(data, list) and len(data) > 0:
        item = data[0]
        mmsi = str(item.get("mmsi") or item.get("id") or query)

        lat = float(item.get("lat") or item.get("l") or 0)
        lon = float(item.get("lng") or item.get("lon") or 0)

        if lat != 0 and lon != 0:
          vessel_info = {
              "mmsi": mmsi,
              "name": (
                  item.get("name") or item.get("n") or f"MMSI: {mmsi}"
              ).upper(),
              "lat": lat,
              "lon": lon,
              "speed": float(item.get("speed") or item.get("s") or 0),
              "course": float(item.get("course") or item.get("c") or 0),
              "status": "Найдено в базе",
              "destination": item.get("dest") or item.get("d") or "Не указано",
          }
          vessels_data[mmsi] = vessel_info
          logging.info(
              f"Успешный поиск fallback: {vessel_info['name']} [{mmsi}] ->"
              f" [{lat}, {lon}]"
          )
          return vessel_info
  except Exception as e:
    logging.error(f"Сбой точечного поиска для '{query}': {e}")

  return None


@app.route("/")
def index():
  return render_template("index.html")


@app.route("/api/set_vessels", methods=["POST"])
def set_vessels():
  global target_vessels, current_region, ws_connection
  data = request.json or {}

  raw_vessels = data.get("vessels", [])
  if isinstance(raw_vessels, str):
    raw_vessels = raw_vessels.split(",")

  target_vessels = [
      str(v).lower().strip() for v in raw_vessels if str(v).strip()
  ]

  new_region = data.get("region", "europe")
  region_changed = new_region != current_region
  current_region = new_region

  # Выполняем быстрый поиск для каждого введенного судна/MMSI
  for item in target_vessels:
    existing = next(
        (
            v
            for v in vessels_data.values()
            if v["mmsi"] == item or item in v["name"].lower()
        ),
        None,
    )
    if not existing:
      # Запускаем точечный поиск в отдельном потоке, чтобы не блокировать ответ
      threading.Thread(
          target=fetch_fallback_vessel, args=(item,), daemon=True
      ).start()

  if region_changed and ws_connection and async_loop:
    asyncio.run_coroutine_threadsafe(ws_connection.close(), async_loop)

  return jsonify({
      "status": "success",
      "tracking": target_vessels,
      "vessels": list(vessels_data.values()),
  })


@app.route("/api/vessels", methods=["GET"])
def get_vessels():
  return jsonify(list(vessels_data.values()))


async def ais_stream_loop():
  global ws_connection
  url = "wss://stream.aisstream.io/v0/stream"

  while True:
    try:
      logging.info("Подключение к живой трансляции AISStream...")
      async with websockets.connect(
          url, ping_interval=20, ping_timeout=20, compression=None
      ) as websocket:
        ws_connection = websocket

        subscribe_msg = {
            "APIKey": AISSTREAM_API_KEY,
            "BoundingBoxes": BOUNDING_BOXES.get(
                current_region, BOUNDING_BOXES["europe"]
            ),
            "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
        }

        await websocket.send(json.dumps(subscribe_msg))

        async for message in websocket:
          try:
            msg = json.loads(message)
            if "error" in msg or "Error" in msg:
              continue

            msg_type = msg.get("MessageType")
            meta = msg.get("MetaData", {})
            mmsi = str(
                meta.get("MMSI")
                or msg.get("Message", {}).get(msg_type, {}).get("UserID", "")
            )

            if not mmsi:
              continue

            ship_name = meta.get("ShipName", "").strip()

            is_match = not target_vessels or any(
                v == mmsi or (ship_name and v in ship_name.lower())
                for v in target_vessels
            )

            if is_match:
              if msg_type == "PositionReport":
                pos = msg["Message"]["PositionReport"]
                lat, lon = pos["Latitude"], pos["Longitude"]
                if lat == 181 or lon == 181:
                  continue

                existing = vessels_data.get(mmsi, {})
                vessels_data[mmsi] = {
                    "mmsi": mmsi,
                    "name": (
                        ship_name or existing.get("name") or f"MMSI: {mmsi}"
                    ).upper(),
                    "lat": lat,
                    "lon": lon,
                    "speed": pos.get("Sog", existing.get("speed", 0)),
                    "course": pos.get("Cog", existing.get("course", 0)),
                    "status": meta.get(
                        "NavigationalStatus",
                        existing.get("status", "В пути"),
                    ),
                    "destination": meta.get(
                        "Destination",
                        existing.get("destination", "Не указано"),
                    ),
                }

              elif msg_type == "ShipStaticData":
                static = msg["Message"]["ShipStaticData"]
                new_name = static.get("Name", "").strip() or ship_name
                if mmsi in vessels_data:
                  if new_name:
                    vessels_data[mmsi]["name"] = new_name.upper()
                  vessels_data[mmsi]["destination"] = static.get(
                      "Destination", vessels_data[mmsi]["destination"]
                  )

          except Exception:
            pass

    except Exception as e:
      logging.error(f"Переподключение к AISStream через 5 сек: {e}")
      await asyncio.sleep(5)


def start_async_loop():
  global async_loop
  async_loop = asyncio.new_event_loop()
  asyncio.set_event_loop(async_loop)
  async_loop.run_until_complete(ais_stream_loop())


thread = threading.Thread(target=start_async_loop, daemon=True)
thread.start()

if __name__ == "__main__":
  port = int(os.environ.get("PORT", 5000))
  app.run(host="0.0.0.0", port=port)
