import asyncio
import logging
import json
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
import httpx
import websockets

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AIS_Backend")

# Кэш текущих координат и метаданных
LIVE_CACHE = {} 
# История точек для треков: { "mmsi": [[lat, lon], [lat, lon], ...] }
TRACKS_CACHE = {} 
FAVORITE_MMSIS = {"220338000", "205404090"}
AISSTREAM_KEY = os.getenv("AISSTREAM_API_KEY", "")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json"
}

ws_restart_event = asyncio.Event()

def update_vessel_data(mmsi: str, lat: float, lon: float, sog: float = 0, cog: float = 0, destination: str = "N/A", eta: str = "N/A", name: str = ""):
    mmsi = str(mmsi).strip()
    if mmsi not in LIVE_CACHE:
        LIVE_CACHE[mmsi] = {"mmsi": mmsi, "name": name or mmsi, "destination": destination, "eta": eta}
    
    LIVE_CACHE[mmsi].update({
        "lat": lat,
        "lon": lon,
        "sog": sog,
        "cog": cog
    })
    if destination != "N/A": LIVE_CACHE[mmsi]["destination"] = destination
    if eta != "N/A": LIVE_CACHE[mmsi]["eta"] = eta
    if name: LIVE_CACHE[mmsi]["name"] = name

    # Обновление трека
    if mmsi not in TRACKS_CACHE:
        TRACKS_CACHE[mmsi] = []
    
    new_point = [lat, lon]
    if not TRACKS_CACHE[mmsi] or TRACKS_CACHE[mmsi][-1] != new_point:
        TRACKS_CACHE[mmsi].append(new_point)
        if len(TRACKS_CACHE[mmsi]) > 100:
            TRACKS_CACHE[mmsi].pop(0)

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
                    "FiltersShipMMSI": list(FAVORITE_MMSIS),
                    "FilterMessageTypes": ["PositionReport", "ShipStaticData"]
                }
                await ws.send(json.dumps(sub_msg))
                logger.info(f"Подписка оформлена на MMSI: {list(FAVORITE_MMSIS)}")
                ws_restart_event.clear()

                while not ws_restart_event.is_set():
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        data = json.loads(message)
                        msg_type = data.get("MessageType")
                        
                        if msg_type == "PositionReport":
                            pos = data["Message"]["PositionReport"]
                            update_vessel_data(
                                mmsi=pos["UserID"],
                                lat=pos["Latitude"],
                                lon=pos["Longitude"],
                                sog=pos.get("Sog", 0),
                                cog=pos.get("Cog", 0)
                            )
                        elif msg_type == "ShipStaticData":
                            stat = data["Message"]["ShipStaticData"]
                            mmsi = str(stat["UserID"])
                            dest = stat.get("Destination", "N/A")
                            name = stat.get("Name", "")
                            if mmsi in LIVE_CACHE:
                                LIVE_CACHE[mmsi]["destination"] = dest
                                if name: LIVE_CACHE[mmsi]["name"] = name
                    except asyncio.TimeoutError:
                        continue
        except Exception as e:
            logger.error(f"Ошибка WebSocket: {e}. Переподключение через 5 сек...")
            await asyncio.sleep(5)

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

@app.get("/", response_class=HTMLResponse)
async def serve_ui():
    return """
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>AIS Tracker Pro</title>
        <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; margin: 0; padding: 12px; background: #f4f6f8; color: #333; }
            .layout { display: flex; flex-direction: column; gap: 12px; max-width: 1100px; margin: 0 auto; }
            .card { background: #fff; padding: 16px; border-radius: 10px; box-shadow: 0 2px 6px rgba(0,0,0,0.06); }
            h2 { margin-top: 0; font-size: 1.1rem; color: #1a1a1a; }
            #map { height: 380px; width: 100%; border-radius: 8px; }
            .search-form { display: flex; gap: 8px; }
            input[type="text"] { flex: 1; padding: 10px; border: 1px solid #ccc; border-radius: 6px; font-size: 14px; }
            button { padding: 10px 16px; background: #007bff; color: #fff; border: none; border-radius: 6px; cursor: pointer; font-weight: bold; }
            button:hover { background: #0056b3; }
            .btn-danger { background: #dc3545; padding: 4px 8px; font-size: 12px; }
            .btn-danger:hover { background: #bb2d3b; }
            .btn-success { background: #198754; padding: 4px 8px; font-size: 12px; }
            .btn-success:hover { background: #157347; }
            table { width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 13px; }
            th, td { padding: 8px; text-align: left; border-bottom: 1px solid #eee; }
            th { background: #f8f9fa; }
            .status-online { color: #198754; font-weight: bold; }
            .status-waiting { color: #fd7e14; font-weight: bold; }
            .result-box { margin-top: 10px; padding: 10px; background: #f8f9fa; border: 1px solid #e9ecef; border-radius: 6px; font-size: 13px; }
            .controls { display: flex; align-items: center; gap: 10px; margin-bottom: 8px; font-size: 13px; }
        </style>
    </head>
    <body>
    <div class="layout">
        <div class="card">
            <h2>Поиск судна по MMSI</h2>
            <div class="search-form">
                <input type="text" id="mmsiInput" placeholder="Введите MMSI (например, 220338000)">
                <button onclick="searchVessel()">Найти</button>
            </div>
            <div id="searchResult"></div>
        </div>

        <div class="card">
            <h2>Карта судов (Realtime)</h2>
            <div class="controls">
                <label><input type="checkbox" id="toggleTracks" checked onchange="drawMapElements()"> Показывать треки движения</label>
            </div>
            <div id="map"></div>
        </div>

        <div class="card">
            <h2>Избранные суда</h2>
            <table>
                <thead>
                    <tr>
                        <th>MMSI / Имя</th>
                        <th>Статус</th>
                        <th>Широта / Долгота</th>
                        <th>Скорость / Курс</th>
                        <th>Порт назначения</th>
                        <th>Действия</th>
                    </tr>
                </thead>
                <tbody id="favoritesTable">
                    <tr><td colspan="6">Загрузка...</td></tr>
                </tbody>
            </table>
        </div>
    </div>

    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <script>
        const map = L.map('map').setView([54.0, 10.0], 5);
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { attribution: '© OpenStreetMap' }).addTo(map);

        let markers = {};
        let polylines = {};
        let globalFavorites = [];
        let globalTracks = {};

        async function fetchFavorites() {
            try {
                const res = await fetch('/favorites');
                const data = await res.json();
                globalFavorites = data.favorites || [];
                globalTracks = data.tracks || {};
                
                renderTable();
                drawMapElements();
            } catch (err) {
                console.error("Ошибка обновления:", err);
            }
        }

        function renderTable() {
            const tbody = document.getElementById('favoritesTable');
            tbody.innerHTML = '';

            globalFavorites.forEach(item => {
                const row = document.createElement('tr');
                const mmsi = item.mmsi;

                if (item.status === 'online' && item.data) {
                    const d = item.data;
                    row.innerHTML = `
                        <td><b>${d.name || mmsi}</b><br><small style="color:#777">${mmsi}</small></td>
                        <td class="status-online">ONLINE</td>
                        <td>${d.lat}, ${d.lon}</td>
                        <td>${d.sog} kn / ${d.cog}°</td>
                        <td>${d.destination || 'N/A'}</td>
                        <td><button class="btn-danger" onclick="removeFavorite('${mmsi}')">Удалить</button></td>
                    `;
                } else {
                    row.innerHTML = `
                        <td><b>${mmsi}</b></td>
                        <td class="status-waiting">WAITING</td>
                        <td>—</td><td>—</td><td>—</td>
                        <td><button class="btn-danger" onclick="removeFavorite('${mmsi}')">Удалить</button></td>
                    `;
                }
                tbody.appendChild(row);
            });
        }

        function drawMapElements() {
            const showTracks = document.getElementById('toggleTracks').checked;

            globalFavorites.forEach(item => {
                const mmsi = item.mmsi;
                if (item.status === 'online' && item.data) {
                    const d = item.data;
                    
                    // Обновление/создание маркера
                    if (markers[mmsi]) {
                        markers[mmsi].setLatLng([d.lat, d.lon]);
                    } else {
                        markers[mmsi] = L.marker([d.lat, d.lon]).addTo(map)
                            .bindPopup(`<b>${d.name || mmsi}</b><br>MMSI: ${mmsi}<br>Назначение: ${d.destination || 'N/A'}<br>Скорость: ${d.sog} kn`);
                    }

                    // Обновление/создание линии трека
                    const trackPoints = globalTracks[mmsi] || [];
                    if (showTracks && trackPoints.length > 1) {
                        if (polylines[mmsi]) {
                            polylines[mmsi].setLatLngs(trackPoints);
                        } else {
                            polylines[mmsi] = L.polyline(trackPoints, { color: '#007bff', weight: 3, opacity: 0.7 }).addTo(map);
                        }
                    } else if (polylines[mmsi]) {
                        map.removeLayer(polylines[mmsi]);
                        delete polylines[mmsi];
                    }
                }
            });
        }

        async function searchVessel() {
            const mmsi = document.getElementById('mmsiInput').value.trim();
            const resultDiv = document.getElementById('searchResult');
            if (!mmsi) return;

            resultDiv.innerHTML = "<p>Поиск...</p>";

            try {
                const res = await fetch(`/search/${mmsi}`);
                const data = await res.json();

                if (data.status === "success" && data.data) {
                    const d = data.data;
                    resultDiv.innerHTML = `
                        <div class="result-box">
                            <b>Источник:</b> ${data.source}<br>
                            <b>MMSI:</b> ${d.mmsi}<br>
                            <b>Координаты:</b> ${d.lat}, ${d.lon}<br>
                            <b>Скорость / Курс:</b> ${d.sog} kn / ${d.cog}°<br>
                            <b>Порт назначения:</b> ${d.destination || 'N/A'}<br><br>
                            <button class="btn-success" onclick="addFavorite('${d.mmsi}')">+ Добавить в Избранное</button>
                        </div>
                    `;
                    if (d.lat && d.lon) {
                        map.setView([d.lat, d.lon], 8);
                        L.marker([d.lat, d.lon]).addTo(map).bindPopup(`<b>MMSI: ${d.mmsi}</b>`).openPopup();
                    }
                } else {
                    resultDiv.innerHTML = `<div class="result-box" style="color:red;">Сообщение: ${data.message || 'Судно не найдено'}</div>`;
                }
            } catch (err) {
                resultDiv.innerHTML = `<div class="result-box" style="color:red;">Ошибка поиска: ${err.message}</div>`;
            }
        }

        async function addFavorite(mmsi) {
            await fetch('/favorites/add', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({mmsi: mmsi})
            });
            fetchFavorites();
        }

        async function removeFavorite(mmsi) {
            await fetch('/favorites/remove', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({mmsi: mmsi})
            });
            if (markers[mmsi]) { map.removeLayer(markers[mmsi]); delete markers[mmsi]; }
            if (polylines[mmsi]) { map.removeLayer(polylines[mmsi]); delete polylines[mmsi]; }
            fetchFavorites();
        }

        setInterval(fetchFavorites, 3000);
        fetchFavorites();
    </script>
    </body>
    </html>
    """

@app.get("/favorites")
async def get_favorites():
    result = []
    for mmsi in list(FAVORITE_MMSIS):
        if mmsi in LIVE_CACHE:
            result.append({"mmsi": mmsi, "status": "online", "data": LIVE_CACHE[mmsi]})
        else:
            result.append({"mmsi": mmsi, "status": "waiting_data", "data": None})
    return JSONResponse(content={"favorites": result, "tracks": TRACKS_CACHE})

@app.post("/favorites/add")
async def add_favorite(payload: dict = Body(...)):
    mmsi = str(payload.get("mmsi", "")).strip()
    if mmsi:
        FAVORITE_MMSIS.add(mmsi)
        ws_restart_event.set()
        return {"status": "success", "favorites": list(FAVORITE_MMSIS)}
    raise HTTPException(status_code=400, detail="Invalid MMSI")

@app.post("/favorites/remove")
async def remove_favorite(payload: dict = Body(...)):
    mmsi = str(payload.get("mmsi", "")).strip()
    if mmsi in FAVORITE_MMSIS:
        FAVORITE_MMSIS.remove(mmsi)
        ws_restart_event.set()
        return {"status": "success", "favorites": list(FAVORITE_MMSIS)}
    raise HTTPException(status_code=404, detail="MMSI not in favorites")

@app.get("/search/{mmsi}")
async def search_vessel(mmsi: str):
    clean_mmsi = str(mmsi).strip()
    
    if clean_mmsi in LIVE_CACHE:
        return {"status": "success", "source": "live_cache", "data": LIVE_CACHE[clean_mmsi]}

    async with httpx.AsyncClient(headers=HEADERS, timeout=8.0, follow_redirects=True) as client:
        try:
            url = f"https://data.hub.ais.org/api/v1/vessel/{clean_mmsi}"
            response = await client.get(url)
            if response.status_code == 200:
                raw_data = response.json()
                lat = raw_data.get("lat") or raw_data.get("latitude")
                lon = raw_data.get("lon") or raw_data.get("longitude")
                
                if lat is not None and lon is not None:
                    update_vessel_data(
                        mmsi=clean_mmsi,
                        lat=float(lat),
                        lon=float(lon),
                        sog=float(raw_data.get("speed", 0) or raw_data.get("sog", 0)),
                        cog=float(raw_data.get("course", 0) or raw_data.get("cog", 0)),
                        destination=raw_data.get("destination", "N/A")
                    )
                    return {"status": "success", "source": "hub_ais_api", "data": LIVE_CACHE[clean_mmsi]}
            
            return {"status": "not_found", "source": "hub_ais_api", "message": "Судно не найдено или нет координат"}
        except Exception as e:
            return {"status": "error", "message": str(e)}
            
