# src/web/routes.py
from fastapi import APIRouter, Request, UploadFile, File
from fastapi.responses import HTMLResponse
import pandas as pd
from io import BytesIO
from pathlib import Path

from ..utils.logger import get_logger
from ..utils.db import get_all_data_sources,insert_data_source,update_source_filepath
router = APIRouter()
logger = get_logger(__name__)

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_SOURCES_DIR = BASE_DIR / "data" / "sources"
DATA_SOURCES_DIR.mkdir(parents=True, exist_ok=True)

@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    logger.info("Rendering home page")
    templates = request.app.state.templates
    return templates.TemplateResponse("index.html", {"request": request})


@router.get("/sources", response_class=HTMLResponse)
async def sources_page(request: Request):
    """
    Page where user can manage data sources (starting with CSV upload).
    """

    templates = request.app.state.templates
    try:
        sources = get_all_data_sources()
    except Exception:
        logger.exception("Error fetching data sources for sources page")
        sources = []
    logger.info("Rendering sources page with %d existing sources", len(sources))

    return templates.TemplateResponse(
        "sources.html",
        {
            "request": request,
            "sources": sources
            })


@router.post("/sources/upload", response_class=HTMLResponse)
async def upload_csv_source(request: Request, file: UploadFile = File(...)):
    """
    Handle CSV upload, read into pandas, and return an HTML preview
    (first 10 rows) as a partial template.

    This endpoint is called via HTMX from the form on sources.html.
    """
    templates = request.app.state.templates

    logger.info("Received CSV upload: filename=%s, content_type=%s",
                file.filename, file.content_type)

    content = await file.read()
    logger.debug("CSV file %s size: %d bytes", file.filename, len(content))

    try:
        df = pd.read_csv(BytesIO(content))
        row_count, col_count = df.shape
        logger.info(
            "CSV parsed successfully: filename=%s, shape=(%d, %d)",
            file.filename, df.shape[0], df.shape[1]
        )
    except Exception as e:
        logger.exception("Failed to read CSV file: %s", file.filename)
        return templates.TemplateResponse(
            "partials/source_preview.html",
            {
                "request": request,
                "filename": file.filename,
                "preview_html": f"<p>Failed to read CSV: {e}</p>",
            },
        )

    try:
        source_id = insert_data_source(
            name=file.filename,
            source_type="csv",
            orignial_name=file.filename,
            file_path=None,
            row_count=row_count,
            column_count=col_count,
            status = "ready"

        )
    except Exception:
        logger.exception("Failed to insert data source metadata")
        return templates.TemplateResponse(
            "partials/source_preview.html",
            {
                "request": request,
                "filename": file.filename,
                "preview_html": "<p>Failed to save data source metadata.</p>",
                "source_id": None,
            },
        )
        #insert metadata and save file
    source_filename = f"source_{source_id}.csv"
    target_path = DATA_SOURCES_DIR / source_filename

    try:
        with open(target_path, "wb") as f_out:
            f_out.write(content)
            logger.info("Saved CSV file for source_id=%s to %s", source_id, target_path)
    except Exception:
        logger.exception("Failed to save csv file to disk")
    
    #updated filepath now.
    update_source_filepath(source_id,target_path)

    #Build preview
    preview_df = df.head(10)
    table_html = preview_df.to_html(classes="preview-table", index=False)

    logger.debug("Generated preview for %s (10 rows)", file.filename)

    return templates.TemplateResponse(
        "partials/source_preview.html",
        {
            "request": request,
            "filename": f"{file.filename} (Source ID: {source_id})",
            "preview_html": table_html,
            "source_id": source_id
        },
    )
