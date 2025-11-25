# src/main.py
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from src.utils.db import init_metadata_tables
from src.utils.logger import get_logger
from .web.routes import router as web_router  # adjust if your structure differs

# --------------------------
# Paths Setup
# --------------------------
BASE_DIR = Path(__file__).resolve().parent.parent  # project_root/src -> project_root
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

# App logger (separate name from ETL, but same config)
logger = get_logger(__name__)
logger.info("Staring ETL BUILder Application")

# --------------------------
# FastAPI App
# --------------------------
app = FastAPI(title="ETL Builder")


# Init sources table
init_metadata_tables()

# --------------------------
# Jinja Templates
# --------------------------
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
app.state.templates = templates


# --------------------------
# Request Logging Middleware
# --------------------------
@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info("Incoming request: %s %s", request.method, request.url.path)

    response = await call_next(request)

    logger.info(
        "Completed request: %s %s -> %s",
        request.method,
        request.url.path,
        response.status_code,
    )

    return response


# --------------------------
# Static Files
# --------------------------
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# --------------------------
# Routers
# --------------------------
app.include_router(web_router)


# --------------------------
# Health Check Route
# --------------------------
@app.get("/health")
def health_check():
    logger.info("Health endpoint accessed")
    return {"status": "ok"}
