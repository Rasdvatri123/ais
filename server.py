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

# Стандартные валидные BoundingBoxes для AISStream
BOUNDING_BOXES = {
    # Европа (Средиземное море, Северное море, Балтика, Черное море)
    "europe": [
        [[30.0, -10.0], [65.0, 40.0]]
    ],
    # Весь мир (разбит на 2 валидных полушария, чтобы AISStream корректно принял подписку)
    "world": [
        [[-90.0, -180.0], [90.0, 0.0]],
        [[-90.0, 0.0], [90.0, 180.0]]
    ]
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
  data = request.json or {}
  target_vessels = [v.lower() for v in data.get("vessels", [])]

  new_region = data.get("region", "europe")
  region_changed = new_region != current_region
  current_region = new_region

  logging.info(
      f"Обновлен список: {target_vessels}, Район: {current_region}"
  )

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
      # Отключаем сжатие deflate, так как оно часто вызывает 'no close frame' на серверах AIS
      async with websockets.connect(
          url,
          ping_interval=20,
          ping_timeout=20,
          compression=None
      ) as websocket:
        ws_connection = websocket
        logging.info(
            f"Успешное подключение к AISStream (Район: {current_region})!"
        )

        subscribe_msg = {
            "APIKey": AISSTREAM_API_KEY.strip(),
            "BoundingBoxes": BOUNDING_BOXES.get(
                current_region, BOUNDING_BOXES["europe"]
            ),
            "FilterMessageTypes": ["PositionReport"]
        }

        # Отправляем сообщение подписки
        await websocket.send(json.dumps(subscribe_msg))

        async for message in websocket:
          try:
            msg = json.loads(message)

            if "error" in msg or "Error" in msg:
              logging.error(f"Ответ с ошибкой от AISStream: {msg}")
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
                logging.info(f"Найдено судно: {ship_name}")
          except Exception:
            pass

    except websockets.exceptions.ConnectionClosedError as e:
      logging.error(f"Сервер закрыл соединение. Код: {e.code}, Причина: {e.reason}")
      await asyncio.sleep(10)
    except Exception as e:
      logging.error(f"Ошибка соединения: {e}. Ожидание 10 секунд...")
      await asyncio.sleep(10)


def start_async_loop():
  global async_loop
  async_loop = asyncio.new_event_loop()
  asyncio.set_event_loop(async_loop)
  async_loop.run_until_complete(ais_stream_loop())


thread = threading.Thread(target=start_async_loop, daemon=True)
thread.start()

if __name__ == "__main__":
  app.run(host="0.0.0.0", port=5000)
