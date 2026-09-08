import asyncio
import logging
import json
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import httpx
import websockets

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AIS_Backend")

# Хранилище в памяти
LIVE_CACHE = {}

FAVORITE_MMSIS = ["220338000", "205404090"]  # Укажите ваши MMSI
AISSTREAM_KEY = os.getenv("AISSTREAM_API_KEY", "")

# Реалистичные заголовки для REST Fallback
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept": "application/json"
}

async def ais_websocket_listener():
    """Фоновый поток для подписки на AISStream"""
    if not AISSTREAM_KEY:
        logger.warning("AISSTREAM_API_KEY не задан! WebSocket запущен не будет.")
        return

    url = "wss://stream.aisstream.io/v0/stream"
    
    while True:
        try:
            logger.info("Установка WebSocket соединения с AISStream...")
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
                            "cog": pos.get("Cog", 0),
                            "timestamp": pos.get("Timestamp")
                        }
        except Exception as e:
            logger.error(f"Ошибка WebSocket: {e}. Reconnect через 5 сек...")
            await asyncio.sleep(5)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Запуск фонового WebSocket при старте сервера
    task = asyncio.create_task(ais_websocket_listener())
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)

# Включаем CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"status": "ok", "cached_vessels_count": len(LIVE_CACHE)}

# 1. Получение избранных судов
@app.get("/favorites")
async def get_favorites():
    result = []
    for mmsi in FAVORITE_MMSIS:
        if mmsi in LIVE_CACHE:
            result.append({"mmsi": mmsi, "status": "online", "data": LIVE_CACHE[mmsi]})
        else:
            result.append({"mmsi": mmsi, "status": "waiting_data", "data": None})
    return result

# 2. Прямой REST поиск судна по MMSI (без ожидания WebSocket)
@app.get("/search/{mmsi}")
async def search_vessel(mmsi: str):
    mmsi = str(mmsi).strip()
    
    # Если данные уже пришли по WebSocket — отдаем из памяти
    if mmsi in LIVE_CACHE:
        return {"source": "cache", "data": LIVE_CACHE[mmsi]}

    # Запрос напрямую к публичным REST API
    async with httpx.AsyncClient(headers=HEADERS, timeout=10.0, follow_redirects=True) as client:
        try:
            # Публичный эндпоинт для мгновенного поиска по MMSI
            response = await client.get(f"https://data.hub.ais.org/api/v1/vessel/{mmsi}")
            
            if response.status_code == 200:
                data = response.json()
                return {"source": "rest_api", "data": data}
            else:
                raise HTTPException(
                    status_code=response.status_code, 
                    detail=f"REST API ответил со статусом {response.status_code}"
                )
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502, detail=f"Ошибка соединения с REST API: {exc}")
