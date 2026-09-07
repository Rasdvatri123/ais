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

AISSTREAM_API_KEY = "b833e22c3f4002f8f9a95b533a66433dd7463512"


@app.route("/")
def index():
  return render_template("index.html")


@app.route("/api/set_vessels", methods=["POST"])
def set_vessels():
  global target_vessels
  data = request.json
  target_vessels = [v.lower() for v in data.get("vessels", [])]
  logging.info(f"Обновлен список отслеживания: {target_vessels}")
  return jsonify({"status": "success", "tracking": target_vessels})


@app.route("/api/vessels", methods=["GET"])
def get_vessels():
  return jsonify(list(vessels_data.values()))


async def ais_stream_loop():
  url = "wss://stream.aisstream.io/v0/stream"

  while True:
    try:
      # Настраиваем максимальный размер сообщения и увеличенные таймауты
      async with websockets.connect(
          url, ping_interval=20, ping_timeout=20, max_size=None
      ) as websocket:
        logging.info("Успешное асинхронное подключение к AISStream!")

        # Попробуем зону с частым трафиком или весь мир
        subscribe_msg = {
            "APIKey": AISSTREAM_API_KEY,
            "BoundingBoxes": [[[-90, -180], [90, 180]]],
            "FilterMessageTypes": ["PositionReport"],
        }

        await websocket.send(json.dumps(subscribe_msg))

        async for message in websocket:
          try:
            msg = json.loads(message)
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
      logging.error(f"Ошибка соединения WebSocket: {e}. Переподключение...")
      await asyncio.sleep(5)


def start_async_loop():
  loop = asyncio.new_event_loop()
  asyncio.set_event_loop(loop)
  loop.run_until_complete(ais_stream_loop())


# Запускаем асинхронный поток
thread = threading.Thread(target=start_async_loop, daemon=True)
thread.start()

if __name__ == "__main__":
  app.run(host="0.0.0.0", port=5000)
