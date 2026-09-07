import json
import threading
from flask import Flask, jsonify, render_template, request  # <-- Проверьте render_template
from flask_cors import CORS
import websocket

app = Flask(__name__)
CORS(app)

# 1. ГЛАВНЫЙ МАРШРУТ (Отдает страницу с картой)


@app.route("/")
def index():
  return render_template("index.html")


# 2. МАРШРУТЫ API
@app.route("/api/set_vessels", methods=["POST"])
def set_vessels():
  # ваш существующий код
  ...


@app.route("/api/vessels", methods=["GET"])
def get_vessels():
  # ваш существующий код
  ...

API_KEY = "b833e22c3f4002f8f9a95b553a66433dd7463512"  # Укажите ваш API Key

app = Flask(__name__)
CORS(app)

tracked_vessels = {}
mmsi_to_name = {}
mmsi_to_dest = {}

# Множество искомых названий судов в нижнем регистре
target_names = set()

NAV_STATUS_MAP = {
    0: "На ходу (под двигателем)",
    1: "На якоре",
    2: "Не управляется",
    3: "Ограничен в возможности маневрировать",
    4: "Стеснен своей осадкой",
    5: "Пришвартован",
    6: "На мели",
    7: "Занят ловом рыбы",
    8: "Идет под парусом",
    14: "AIS-SART (Спасательный сигнал)",
    15: "Не определено",
}


def on_message(ws, message):
  try:
    data = json.loads(message)
    msg_type = data.get("MessageType")
    meta = data.get("MetaData", {})

    mmsi = str(
        meta.get("MMSI")
        or data.get("Message", {}).get("PositionReport", {}).get("UserId")
    )
    ship_name = meta.get("ShipName", "").strip()

    if mmsi and ship_name:
      mmsi_to_name[mmsi] = ship_name

    # Извлекаем порт назначения из метаданных или статических сообщений
    if meta.get("Destination"):
      mmsi_to_dest[mmsi] = meta.get("Destination").strip()

    if msg_type == "ShipStaticData":
      static_data = data.get("Message", {}).get("ShipStaticData", {})
      dest = static_data.get("Destination", "").strip()
      if dest:
        mmsi_to_dest[mmsi] = dest

    current_name = mmsi_to_name.get(mmsi, ship_name)

    if not current_name or not target_names:
      return

    name_lower = current_name.lower()

    # Проверяем совпадение с любым судно из списка
    if any(target in name_lower for target in target_names):
      if msg_type == "PositionReport":
        pos = data["Message"]["PositionReport"]
        lat = pos.get("Latitude")
        lon = pos.get("Longitude")
        speed = pos.get("Sog", 0)
        course = pos.get("Cog", 0)

        status_code = pos.get("NavigationalStatus", 15)
        status_text = NAV_STATUS_MAP.get(status_code, "Неизвестно")
        destination = mmsi_to_dest.get(mmsi, "Ожидание пакета (до 6 мин)")

        tracked_vessels[mmsi] = {
            "mmsi": mmsi,
            "name": current_name,
            "lat": lat,
            "lon": lon,
            "speed": speed,
            "course": course,
            "status": status_text,
            "destination": destination,
        }
        print(f"[НАЙДЕНО] {current_name} -> {lat}, {lon} | Порт: {destination}")

  except Exception as e:
    print(f"Ошибка обработки: {e}")


def start_ais_stream():
  while True:
    try:
      print("Подключение к AISStream...")

      def on_open(ws):
        print("Подключение к AISStream успешно установлено!")
        sub_msg = {
            "APIKey": API_KEY,
            "BoundingBoxes": [[[-90.0, -180.0], [90.0, 180.0]]],
        }
        ws.send(json.dumps(sub_msg))

      ws = websocket.WebSocketApp(
          "wss://stream.aisstream.io/v0/stream",
          on_open=on_open,
          on_message=on_message,
          on_error=lambda ws, err: print(f"Ошибка WebSocket: {err}"),
          on_close=lambda ws, code, msg: print("Соединение закрыто."),
      )
      ws.run_forever(ping_interval=30, ping_timeout=10)

    except Exception as e:
      print(f"Сбой потока: {e}")

    time.sleep(5)


@app.route("/api/set_vessels", methods=["POST"])
def set_vessels():
  global target_names, tracked_vessels
  data = request.json or {}
  names = data.get("vessels", [])

  target_names = set(n.strip().lower() for n in names if n.strip())
  tracked_vessels.clear()

  print(f"Обновлен список судов: {target_names}")
  return jsonify({"status": "ok", "count": len(target_names)})


@app.route("/api/vessels", methods=["GET"])
def get_vessels():
  return jsonify(list(tracked_vessels.values()))


if __name__ == "__main__":
  threading.Thread(target=start_ais_stream, daemon=True).start()
  app.run(host="127.0.0.1", port=5000)
