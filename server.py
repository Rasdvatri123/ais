import json
import logging
import threading
import time
from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
import websocket

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


def ais_worker():
  def on_message(ws, message):
    try:
      msg = json.loads(message)
      if msg.get("MessageType") == "PositionReport":
        pos = msg["Message"]["PositionReport"]
        meta = msg.get("MetaData", {})
        ship_name = meta.get("ShipName", "").strip()

        if ship_name and any(v in ship_name.lower() for v in target_vessels):
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
    except Exception as e:
      pass

  def on_open(ws):
    logging.info("Успешное подключение к WebSocket AISStream!")
    # Отправляем оптимизированный запрос подписки
    subscribe_msg = {
        "APIKey": AISSTREAM_API_KEY,
        "BoundingBoxes": [[[-90, -180], [90, 180]]],
        "FilterMessageTypes": ["PositionReport"],  # Фильтруем только координаты
    }
    ws.send(json.dumps(subscribe_msg))

  def on_error(ws, error):
    logging.error(f"Ошибка WebSocket: {error}")

  def on_close(ws, close_status_code, close_msg):
    logging.warning("Соединение AISStream закрыто. Переподключение через 3с...")

  while True:
    try:
      ws = websocket.WebSocketApp(
          "wss://stream.aisstream.io/v0/stream",
          on_open=on_open,
          on_message=on_message,
          on_error=on_error,
          on_close=on_close,
      )
      # Добавлен ping_interval=20 для поддержания живого соединения
      ws.run_forever(ping_interval=20, ping_timeout=10)
    except Exception as e:
      logging.error(f"Сбой цикла WebSocket: {e}")

    time.sleep(3)


# Запуск фонового потока AIS
thread = threading.Thread(target=ais_worker, daemon=True)
thread.start()

if __name__ == "__main__":
  app.run(host="0.0.0.0", port=5000)
