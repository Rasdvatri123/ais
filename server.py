import asyncio
import json
import logging
import threading
from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
import websockets

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
CORS(app)

vessels_data = {}
target_vessels = []

# Пресеты координат BoundingBoxes [[[lat_min, lon_min], [lat_max, lon_max]]]
BOUNDING_BOXES = {
    # Европа: от Марокко/Черного моря до Шпицбергена и Исландии (25°N - 82°N, 25°W - 45°E)
    "europe": [[[25.0, -25.0], [82.0, 45.0]]],
    # Весь мир
    "world": [[[-90.0, -180.0], [90.0, 180.0]]],
}

current_region = "europe"
ws_connection = None
AISSTREAM_API_KEY = "b833e22c3f4002f8f9a95b533a66433dd7463512"


@app.route("/")
def index():
  return render_template("index.html")


@app.route("/api/set_vessels", methods=["POST"])
def set_vessels():
  global target_vessels, current_region, ws_connection
  data = request.json
  target_vessels = [v.lower() for v in data.get("vessels", [])]

  new_region = data.get("region", "europe")
  region_changed = new_region != current_region
  current_region = new_region

  logging.info(
      f"Обновлен список: {target_vessels}, Район: {current_region}"
  )

  # Если сменился район, принудительно закрываем WebSocket для переподключения с новыми BoundingBoxes
  if region_changed and ws_connection:
    asyncio.run_coroutine_threadsafe(ws_connection.close(), async_loop)

  return jsonify({"status": "success", "tracking": target_vessels})


@app.route("/api/vessels", methods=["GET"])
def get_vessels():
  return jsonify(list(vessels_data.values()))


async def ais_stream_loop():
  global ws_connection
  url = "wss://stream.aisstream.io/v0/stream"

  while True:
    try:
      async with websockets.connect(
          url, ping_interval=20, ping_timeout=20, max_size=None
      ) as websocket:
        ws_connection = websocket
        logging.info(
            f"Успешное подключение к AISStream (Район: {current_region})!"
        )

        subscribe_msg = {
            "APIKey": AISSTREAM_API_KEY,
            "BoundingBoxes": BOUNDING_BOXES.get(
                current_region, BOUNDING_BOXES["europe"]
            ),
            "FilterMessageTypes": ["PositionReport"],
        }

        await websocket.send(json.dumps(subscribe_msg))

        async for message in websocket:
          try:
            msg = json.loads(message)

            if "error" in msg or "Error" in msg:
              logging.error(f"Ошибка от AISStream: {msg}")
              continue

            if msg.get("MessageType") == "PositionReport":
              pos = msg["Message"]["PositionReport"]
              meta = msg.get("MetaData", {})
              ship_name = meta.get("ShipName", "").strip()

              if ship_name and any(
                  v in ship_name.lower() for v in target_vessels
              ):
                mmsi = pos["UserID"]
                vessels_data[mmsi] = {
                    "mmsi": mmsi,
                    "name": ship_name,
                    "lat": pos["Latitude"],
                    "lon": pos["Longitude"],
                    "speed": pos.get("Sog", 0),
                    "course": pos.get("Cog", 0),
                    "status": meta.get("NavigationalStatus", "Неизвестно"),
                    "destination": meta.get("Destination", "Не указано"),
                }
                logging.info(
                    f"Найдено судно: {ship_name} ({pos['Latitude']},"
                    f" {pos['Longitude']})"
                )
          except Exception:
            pass

    except Exception as e:
      logging.error(f"Переподключение WebSocket: {e}")
      await asyncio.sleep(3)


def start_async_loop():
  global async_loop
  async_loop = asyncio.new_event_loop()
  asyncio.set_event_loop(async_loop)
  async_loop.run_until_complete(ais_stream_loop())


thread = threading.Thread(target=start_async_loop, daemon=True)
thread.start()

if __name__ == "__main__":
  app.run(host="0.0.0.0", port=5000)
