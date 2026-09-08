import asyncio
import logging
import json
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
import httpx
import websockets

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AIS_Backend")

LIVE_CACHE = {}
FAVORITE_MMSIS = ["220338000", "205404090"]
AISSTREAM_KEY = os.getenv("AISSTREAM_API_KEY", "")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json"
}

async def ais_websocket_listener():
    if not AISSTREAM_KEY:
        logger.warning("AISSTREAM_API_KEY не задан!")
        return

    url = "wss://stream.aisstream.io/v0/stream"
    
    while True:
        try:
            logger.info("Подключение к AISStream WebSocket...")
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                sub_msg = {
                    "APIKey": AISSTREAM_KEY,
                    "BoundingBoxes": [[[-90, -180], [90, 180]]],
                    "FiltersShipMMSI": FAVORITE_MMSIS,
                    "FilterMessageTypes": ["PositionReport"]
                }
                await ws.send(json.dumps(sub_msg))
                logger.info("Подписка отправлена. Слушаем эфир...")

                async for message in ws:
                    data = json.loads(message)
                    if data.get("MessageType") == "PositionReport":
                        pos = data["Message"]["PositionReport"]
                        mmsi = str(pos["UserID"])
                        LIVE_CACHE[mmsi] = {
                            "mmsi": mmsi,
                            "lat": pos["Latitude"],
                            "lon": pos["Longitude"],
                            "sog": pos.get("Sog", 0),
                            "cog": pos.get("Cog", 0)
                        }
        except Exception as e:
            logger.error(f"Ошибка WebSocket: {e}. Переподключение через 10 сек...")
            await asyncio.sleep(10)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(ais_websocket_listener())
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Главная страница (Возвращает HTML-интерфейс с картой и таблицей)
@app.get("/", response_class=HTMLResponse)
async def serve_ui():
    return """
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>AIS Vessel Tracker</title>
        <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; margin: 0; padding: 15px; background: #f4f6f8; }
            .layout { display: flex; flex-direction: column; gap: 15px; max-width: 1200px; margin: 0 auto; }
            .card { background: #fff; padding: 15px; border-radius: 10px; box-shadow: 0 2px 5px rgba(0,0,0,0.05); }
            #map { height: 350px; width: 100%; border-radius: 8px; }
            .search-form { display: flex; gap: 10px; margin-top: 10px; }
            input[type="text"] { flex: 1; padding: 10px; border: 1px solid #ccc; border-radius: 6px; font-size: 15px; }
            button { padding: 10px 18px; background: #007bff; color: #fff; border: none; border-radius: 6px; cursor: pointer; font-weight: bold; }
            button:hover { background: #0056b3; }
            table { width: 100%; border-collapse: collapse; margin-top: 10px; }
            th, td { padding: 10px; text-align: left; border-bottom: 1px solid #eee; }
            th { background: #f8f9fa; }
            .status-online { color: #28a745; font-weight: bold; }
            .status-waiting { color: #fd7e14; font-weight: bold; }
            .result-box { margin-top: 10px; padding: 10px; background: #e9ecef; border-radius: 6px; font-family: monospace; white-space: pre-wrap; }
        </style>
    </head>
    <body>
    <div class="layout">
        <div class="card">
            <h2>Поиск судна по MMSI</h2>
            <div class="search-form">
                <input type="text" id="mmsiInput" placeholder="Введите MMSI">
                <button onclick="searchVessel()">Найти</button>
            </div>
            <div id="searchResult"></div>
        </div>

        <div class="card">
            <h2>Карта судов (Realtime)</h2>
            <div id="map"></div>
        </div>

        <div class="card">
            <h2>Избранные суда</h2>
            <table>
                <thead>
                    <tr>
                        <th>MMSI</th>
                        <th>Статус</th>
                        <th>Широта</th>
                        <th>Долгота</th>
                        <th>Скорость</th>
                        <th>Курс</th>
                    </tr>
                </thead>
                <tbody id="favoritesTable">
                    <tr><td colspan="6">Загрузка данных...</td></tr>
                </tbody>
            </table>
        </div>
    </div>

    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <script>
        const map = L.map('map').setView([54.0, 10.0], 5);
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '© OpenStreetMap'
        }).addTo(map);

        const markers = {};

        async function fetchFavorites() {
            try {
                const res = await fetch('/favorites');
                const data = await res.json();
                
                const tbody = document.getElementById('favoritesTable');
                tbody.innerHTML = '';

                data.forEach(item => {
                    const row = document.createElement('tr');
                    
                    if (item.status === 'online' && item.data) {
                        const { lat, lon, sog, cog, mmsi } = item.data;

                        row.innerHTML = `
                            <td><b>${mmsi}</b></td>
                            <td class="status-online">ONLINE</td>
                            <td>${lat}</td>
                            <td>${lon}</td>
                            <td>${sog} kn</td>
                            <td>${cog}°</td>
                        `;

                        if (markers[mmsi]) {
                            markers[mmsi].setLatLng([lat, lon]);
                        } else {
                            markers[mmsi] = L.marker([lat, lon]).addTo(map)
                                .bindPopup(`<b>MMSI: ${mmsi}</b><br>Скорость: ${sog} kn<br>Курс: ${cog}°`);
                        }
                    } else {
                        row.innerHTML = `
                            <td><b>${item.mmsi}</b></td>
                            <td class="status-waiting">WAITING</td>
                            <td>—</td><td>—</td><td>—</td><td>—</td>
                        `;
                    }
                    tbody.appendChild(row);
                });
            } catch (err) {
                console.error("Ошибка:", err);
            }
        }

        async function searchVessel() {
            const mmsi = document.getElementById('mmsiInput').value.trim();
            const resultDiv = document.getElementById('searchResult');
            if (!mmsi) return;

            resultDiv.innerHTML = "<p>Поиск...</p>";

            try {
                const res = await fetch(`/search/${mmsi}`);
                const data = await res.json();

                if (data.data && data.data.lat && data.data.lon) {
                    map.setView([data.data.lat, data.data.lon], 9);
                    L.marker([data.data.lat, data.data.lon]).addTo(map)
                        .bindPopup(`<b>Найденное судно MMSI: ${mmsi}</b>`).openPopup();
                }

                resultDiv.innerHTML = `
                    <div class="result-box">
                        <b>Источник: ${data.source}</b><br>
                        ${JSON.stringify(data.data, null, 2)}
                    </div>
                `;
            } catch (err) {
                resultDiv.innerHTML = `<p style="color:red;">Ошибка поиска: ${err.message}</p>`;
            }
        }

        setInterval(fetchFavorites, 3000);
        fetchFavorites();
    </script>
    </body>
    </html>
    """

# API-эндпоинты для работы фронтенда
@app.get("/favorites")
async def get_favorites():
    result = []
    for mmsi in FAVORITE_MMSIS:
        if mmsi in LIVE_CACHE:
            result.append({"mmsi": mmsi, "status": "online", "data": LIVE_CACHE[mmsi]})
        else:
            result.append({"mmsi": mmsi, "status": "waiting_data", "data": None})
    return JSONResponse(content=result)

@app.get("/search/{mmsi}")
async def search_vessel(mmsi: str):
    clean_mmsi = str(mmsi).strip()
    
    if clean_mmsi in LIVE_CACHE:
        return {"source": "cache", "data": LIVE_CACHE[clean_mmsi]}

    async with httpx.AsyncClient(headers=HEADERS, timeout=8.0, follow_redirects=True) as client:
        try:
            url = f"https://data.hub.ais.org/api/v1/vessel/{clean_mmsi}"
            response = await client.get(url)
            if response.status_code == 200:
                return {"source": "rest_api", "data": response.json()}
            else:
                return {"source": "rest_api", "status": response.status_code, "data": None}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
            
