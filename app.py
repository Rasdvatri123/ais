import asyncio
import logging
import json
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import httpx
import websockets

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AIS_Backend")

LIVE_CACHE = {}
FAVORITE_MMSIS = ["220338000", "205404090"]  # Укажите нужные MMSI
AISSTREAM_KEY = os.getenv("AISSTREAM_API_KEY", "")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
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
            await asyncio.sleep(10)  # Пауза 10 сек, чтобы избежать ошибки 429

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

# Корневой маршрут - общая информация
@app.get("/")
async def root():
    return {
        "status": "online",
        "cached_vessels": list(LIVE_CACHE.keys()),
        "favorites_configured": FAVORITE_MMSIS
    }

# Роут 1: Избранное
@app.get("/favorites")
async def get_favorites():
    result = []
    for mmsi in FAVORITE_MMSIS:
        if mmsi in LIVE_CACHE:
            result.append({"mmsi": mmsi, "status": "online", "data": LIVE_CACHE[mmsi]})
        else:
            result.append({"mmsi": mmsi, "status": "waiting_data", "data": None})
    return JSONResponse(content=result)

# Роут 2: Поиск по MMSI
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
