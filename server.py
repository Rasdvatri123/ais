import asyncio
import json
import logging
import os
import threading
from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
import websockets

# Настройка логирования
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

app = Flask(__name__)
CORS(app)

vessels_data = {}
target_vessels = []

# Расширенные координаты BoundingBoxes [[[South, West], [North, East]]]
BOUNDING_BOXES = {
    # Расширенная Европа (от -15.0 W в Атлантике до +45.0 E, включая Кельтское море, Великобританию и Ирландию)
    "europe": [
        [[30.0, -15.0], [65.0, 45.0]]
    ],
    # Весь мир
    "world": [
        [[-90.0, -180.0], [90.0, 180.0]]
    ]
}


current_region = "europe"
ws_connection = None

AISSTREAM_API_KEY = os.environ.get(
    "AISSTREAM_API_KEY", "8a2e1e9248eb6650a5d5e16db825de0cc74281e0"
).strip()


@app.route("/")
def index():
  return render_template("index.html")


@app.route("/api/set_vessels", methods=["POST"])
def set_vessels():
  global target_vessels, current_region, ws_connection
  data = request.json or {}

  # Очищаем ввод: сохраняем названия и MMSI
  target_vessels = [
      str(v).lower().strip() for v in data.get("vessels", []) if str(v).strip()
  ]

  new_region = data.get("region", "europe")
  region_changed = new_region != current_region
  current_region = new_region

  logging.info(
      f"Обновлен фильтр поиска: {target_vessels}, Район: {current_region}"
  )

  # Переподключаем WebSocket при смене региона
  if region_changed and ws_connection and async_loop:
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
      logging.info(
          f"Подключение к AISStream (Ключ: {AISSTREAM_API_KEY[:8]}...)..."
      )

      async with websockets.connect(
          url, ping_interval=20, ping_timeout=20, compression=None
      ) as websocket:
        ws_connection = websocket
        logging.info(
            f"Соединение установлено. Подписка на район: {current_region}"
        )

        subscribe_msg = {
            "APIKey": AISSTREAM_API_KEY,
            "BoundingBoxes": BOUNDING_BOXES.get(
                current_region, BOUNDING_BOXES["europe"]
            ),
            # Подписываемся на координаты И статические данные судна (имя/дестинация)
            "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
        }

        await websocket.send(json.dumps(subscribe_msg))
        logging.info("Подписка отправлена.")

        async for message in websocket:
          try:
            msg = json.loads(message)

            if "error" in msg or "Error" in msg:
              logging.error(f"Ошибка от AISStream: {msg}")
              continue

            msg_type = msg.get("MessageType")
            meta = msg.get("MetaData", {})
            mmsi = str(meta.get("MMSI") or msg.get("Message", {}).get(msg_type, {}).get("UserID", ""))

            if not mmsi:
              continue

            ship_name = meta.get("ShipName", "").strip()

            # Проверка совпадений: проверяем и по MMSI, и по Nazvaniyu
            is_match = not target_vessels or any(
                v == mmsi or (ship_name and v in ship_name.lower())
                for v in target_vessels
            )

            if is_match:
              if msg_type == "PositionReport":
                pos = msg["Message"]["PositionReport"]
                lat = pos["Latitude"]
                lon = pos["Longitude"]

                # Не сохраняем некорректные координаты
                if lat == 181 or lon == 181:
                  continue

                existing = vessels_data.get(mmsi, {})
                vessels_data[mmsi] = {
                    "mmsi": mmsi,
                    "name": ship_name or existing.get("name") or f"MMSI: {mmsi}",
                    "lat": lat,
                    "lon": lon,
                    "speed": pos.get("Sog", existing.get("speed", 0)),
                    "course": pos.get("Cog", existing.get("course", 0)),
                    "status": meta.get(
                        "NavigationalStatus",
                        existing.get("status", "Неизвестно"),
                    ),
                    "destination": meta.get(
                        "Destination",
                        existing.get("destination", "Не указано"),
                    ),
                }
                logging.info(
                    f"Обновлена позиция: {vessels_data[mmsi]['name']} ({mmsi}) -> [{lat}, {lon}]"
                )

              elif msg_type == "ShipStaticData":
                static = msg["Message"]["ShipStaticData"]
                new_name = static.get("Name", "").strip() or ship_name

                if mmsi in vessels_data:
                  if new_name:
                    vessels_data[mmsi]["name"] = new_name
                  vessels_data[mmsi]["destination"] = static.get(
                      "Destination", vessels_data[mmsi]["destination"]
                  )
                elif is_match:
                  # Сохраняем имя заранее, даже если позиция ещё не пришла
                  vessels_data[mmsi] = {
                      "mmsi": mmsi,
                      "name": new_name or f"MMSI: {mmsi}",
                      "lat": None,
                      "lon": None,
                      "speed": 0,
                      "course": 0,
                      "status": "Ожидание позиционирования",
                      "destination": static.get("Destination", "Не указано"),
                  }
                logging.info(
                    f"Обновлены данные судна: {new_name or mmsi} ({mmsi})"
                )

          except Exception as parse_err:
            logging.debug(f"Ошибка разбора сообщения: {parse_err}")

    except websockets.exceptions.ConnectionClosedError as e:
      logging.error(f"Соединение закрыто (Код: {e.code}). Переподключение через 5 сек...")
      await asyncio.sleep(5)
    except Exception as e:
      logging.error(f"Ошибка сокета: {e}. Переподключение через 5 сек...")
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
