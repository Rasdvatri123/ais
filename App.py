import asyncio
import logging
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
import httpx
import websockets
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AIS_Server")

# Кэш в памяти: mmsi -> data
vessels_cache = {}

# Список MMSI для фонового мониторинга (Favorites)
FAVORITE_MMSIS = ["220338000", "205404090"]  # Замените на ваши MMSI
AISSTREAM_API_KEY = os.getenv("AISSTREAM_API_KEY", "YOUR_KEY_HERE")

# Заголовки браузера, чтобы REST-запросы не блокировались антибот-защитой
REAL_BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

# 1. Фоновая задача для постоянного мониторинга Избранных судов через WebSocket
async def aisstream_worker():
    uri = "wss://stream.aisstream.io/v0/stream"
    while True:
        try:
            logger.info("Подключение к AISStream WebSocket...")
            async with websockets.connect(uri) as websocket:
                subscribe_message = {
                    "APIKey": AISSTREAM_API_KEY,
                    "BoundingBoxes": [[[-90, -180], [90, 180]]],
                    "FiltersShipMMSI": FAVORITE_MMSIS,
                    "FilterMessageTypes": ["PositionReport", "ShipStaticData"]
                }
                await websocket.send(json.dumps(subscribe_message))
                logger.info("Успешно подсоединились к AISStream, слушаем Favorites...")

                async for message in websocket:
                    data = json.loads(message)
                    msg_type = data.get("MessageType")
                    
                    if msg_type == "PositionReport":
                        pos = data["Message"]["PositionReport"]
                        mmsi = str(pos["UserID"])
                        vessels_cache[mmsi] = {
                            "mmsi": mmsi,
                            "lat": pos["Latitude"],
                            "lon": pos["Longitude"],
                            "sog": pos.get("Sog"),
                            "cog": pos.get("Cog"),
                            "source": "aisstream_live"
                        }
        except Exception as e:
            logger.error(f"Ошибка в WebSocket AISStream: {e}. Переподключение через 5 сек...")
            await asyncio.sleep(5)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Запуск фонового воркера при старте сервера
    asyncio.create_task(aisstream_worker())
    yield

app = FastAPI(lifespan=lifespan)

# 2. Эндпоинт для мгновенного получения избранных судов из кэша
@app.get("/favorites")
async def get_favorites():
    return [vessels_cache.get(mmsi, {"mmsi": mmsi, "status": "no_data_yet"}) for mmsi in FAVORITE_MMSIS]

# 3. Эндпоинт мгновенного поиска по MMSI (Сначала кэш, затем REST Fallback)
@app.get("/search/{mmsi}")
async def search_vessel(mmsi: str):
    mmsi = str(mmsi).strip()
    
    # Шаг А: Проверяем локальный кэш
    if mmsi in vessels_cache:
        return {"status": "found_in_cache", "data": vessels_cache[mmsi]}
    
    # Шаг Б: Если в кэше нет — делаем REST Fallback с подставным User-Agent
    # (Пример REST fallback запроса)
    async with httpx.AsyncClient(headers=REAL_BROWSER_HEADERS, timeout=8.0) as client:
        try:
            # Для теста используется публичный REST API (замените URL на ваш рабочий REST fallback)
            url = f"https://data.hub.ais.org/api/v1/vessel/{mmsi}" 
            response = await client.get(url)
            
            logger.info(f"REST Fallback статус: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                vessels_cache[mmsi] = data  # Кэшируем результат
                return {"status": "found_via_rest", "data": data}
            else:
                raise HTTPException(status_code=404, detail=f"Судно не найдено. Статус ответа: {response.status_code}")
        except Exception as e:
            logger.error(f"Ошибка REST fallback: {e}")
            raise HTTPException(status_code=500, detail=f"Ошибка выполнения REST запроса: {str(e)}")
