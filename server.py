import asyncio
import json
import logging
import os
import threading
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

# Исправленные BoundingBoxes по стандарту AISStream: [[Lat_Min, Lon_Min], [Lat_Max, Lon_Max]]
BOUNDING_BOXES = {
    # Европа и Северная Атлантика
    "europe": [[[30.0, -10.0], [65.0, 40.0]]],
    # Глобальное покрытие (2 зоны для исключения ошибок валидации)
    "world": [
        [[-90.0, -180.0], [90.0, 0.0]],
        [[-90.0, 0.0], [90.0, 180.0]],
    ],
}

current_region = "europe"
ws_connection = None

# Считываем ключ из переменных окружения или берем значение по умолчанию
AISSTREAM_API_KEY = os.environ.get(
    "AISSTREAM_API_KEY", "b833e22c3f4002f8f9a95b533a66433dd7463512"
).strip()


@app.route("/")
def index():
  return render_template("index.html")


@app.route("/api/set_vessels", methods=["POST"])
def set_vessels():
  global target_vessels, current_region, ws_connection
  data = request.json or {}
  target_vessels = [
      v.lower().strip() for v in data.get("vessels", []) if v.strip()
  ]

  new_region = data.get("region", "europe")
  region_changed = new_region != current_region
  current_region = new_region

  logging.info(f"Обновлен трекинг: {target_vessels}, Район: {current_region}")

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
      logging.info(
          f"Попытка подключения к AISStream с ключом: {AISSTREAM_API_KEY[:6]}..."
      )

      async with websockets.connect(
          url, ping_interval=20, ping_timeout=20, compression=None
      ) as websocket:
        ws_connection = websocket
        logging.info(
            f"Соединение установлено. Отправка подписки на район:"
            f" {current_region}..."
        )

        subscribe_msg = {
            "APIKey": AISSTREAM_API_KEY,
            "BoundingBoxes": BOUNDING_BOXES.get(
                current_region, BOUNDING_BOXES["europe"]
            ),
            "FilterMessageTypes": ["PositionReport"],
        }

        await websocket.send(json.dumps(subscribe_msg))
        logging.info("Подписка успешно отправлена в AISStream.")

        async for message in websocket:
          try:
            msg = json.loads(message)

            if "error" in msg or "Error" in msg:
              logging.error(f"AISStream вернул ошибку: {msg}")
              continue

            if msg.get("MessageType") == "PositionReport":
              pos = msg["Message"]["PositionReport"]
              meta = msg.get("MetaData", {})
              ship_name = meta.get("ShipName", "").strip()

              # Если список отслеживаемых судов пуст — собираем все суда из входящего потока
              if not target_vessels or any(
                  v in ship_name.lower() for v in target_vessels
              ):
                mmsi = pos["UserID"]
                vessels_data[mmsi] = {
                    "mmsi": mmsi,
                    "name": ship_name or f"MMSI: {mmsi}",
                    "lat": pos["Latitude"],
                    "lon": pos["Longitude"],
                    "speed": pos.get("Sog", 0),
                    "course": pos.get("Cog", 0),
                    "status": meta.get("NavigationalStatus", "Неизвестно"),
                    "destination": meta.get("Destination", "Не указано"),
                }
                logging.info(
                    f"Данные обновлены: {ship_name or mmsi} ({pos['Latitude']}, {pos['Longitude']})"
                )
          except Exception as parse_err:
            logging.debug(f"Ошибка разбора пакета: {parse_err}")

    except websockets.exceptions.ConnectionClosedError as e:
      logging.error(
          f"Сервер закрыл соединение. Код: {e.code}, Причина: '{e.reason}'"
      )
      await asyncio.sleep(10)
    except Exception as e:
      logging.error(f"Сбой подключения: {e}. Повтор через 10 сек...")
      await asyncio.sleep(10)


def start_async_loop():
  global async_loop
  async_loop = asyncio.new_event_loop()
  asyncio.set_event_loop(async_loop)
  async_loop.run_until_complete(ais_stream_loop())


# Запуск асинхронного потока в фоновом режиме
thread = threading.Thread(target=start_async_loop, daemon=True)
thread.start()

if __name__ == "__main__":
  app.run(host="0.0.0.0", port=5000)
