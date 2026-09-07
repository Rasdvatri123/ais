import json
import threading
from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
import websocket

app = Flask(__name__)
CORS(app)

# Хранилище данных о судах
vessels_data = {}
target_vessels = []

# ==========================================
# 1. ГЛАВНЫЙ МАРШРУТ (Отдает ваш index.html)
# ==========================================


@app.route("/")
def index():
  return render_template("index.html")


# ==========================================
# 2. МАРШРУТЫ API
# ==========================================


@app.route("/api/set_vessels", methods=["POST"])
def set_vessels():
  global target_vessels
  data = request.json
  target_vessels = [v.lower() for v in data.get("vessels", [])]
  return jsonify({"status": "success", "tracking": target_vessels})


@app.route("/api/vessels", methods=["GET"])
def get_vessels():
  return jsonify(list(vessels_data.values()))


# ==========================================
# 3. ФОНОВЫЙ ПОТОК ДЛЯ AISSTREAM
# ==========================================


def ais_worker():
  api_key = "b833e22c3f4002f8f9a95b553a66433dd7463512"  # Укажите ваш API Key

  def on_message(ws, message):
    try:
      msg = json.loads(message)
      if msg.get("MessageType") == "PositionReport":
        pos = msg["Message"]["PositionReport"]
        meta = msg.get("MetaData", {})
        ship_name = meta.get("ShipName", "").strip()

        if any(v in ship_name.lower() for v in target_vessels):
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
    except Exception as e:
      print("Ошибка обработки сообщения:", e)

  def on_open(ws):
    subscribe_msg = {
        "APIKey": api_key,
        "BoundingBoxes": [[[-90, -180], [90, 180]]],
    }
    ws.send(json.dumps(subscribe_msg))

  ws = websocket.WebSocketApp(
      "wss://stream.aisstream.io/v0/stream",
      on_open=on_open,
      on_message=on_message,
  )
  ws.run_forever()


threading.Thread(target=ais_worker, daemon=True).start()

if __name__ == "__main__":
  app.run(host="0.0.0.0", port=5000)
