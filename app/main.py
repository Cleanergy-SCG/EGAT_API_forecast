from fastapi import FastAPI, Request
import os
from dotenv import load_dotenv
from app.routes import router
import logging
from starlette.middleware.base import BaseHTTPMiddleware
from datetime import datetime
from app.config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

class CustomLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        client_ip = request.client.host
        method = request.method
        url = request.url.path
        
        # Log the incoming request
        logging.info(f"{timestamp} - {client_ip} - \"{method} {url}\"")

        response = await call_next(request)
        return response




load_dotenv()

app = FastAPI()
app.add_middleware(CustomLoggingMiddleware)

@app.get("/health")
def checkhealth():
    return {"status": "ok"}

@app.get("/")
def read_root():
    return {
        "environment": settings.ENVIRONMENT,
        "database_url": settings.DATABASE_URL,
        "debug_mode": settings.DEBUG
    }
# Include Routes
app.include_router(router)

