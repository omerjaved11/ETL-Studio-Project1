# src/web/routes.py
import json
from fastapi import APIRouter, Request, UploadFile, File, HTTPException, Form
from fastapi.responses import HTMLResponse, FileResponse
import pandas as pd
from io import BytesIO
from pathlib import Path
from typing import List

from ..utils.pipeline import (
    add_step_drop_rows_with_nulls,
    add_step_drop_columns,
    get_steps_for_source,
    build_pipeline_config,
    apply_pipeline_to_df,
)

from ..utils.logger import get_logger
from ..utils.db import (
    get_all_data_sources,
    insert_data_source,
    update_data_source_shape,
    delete_data_sources,
    update_source_filepath,
    get_data_source_by_id,

)
router = APIRouter()
logger = get_logger(__name__)

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_SOURCES_DIR = BASE_DIR / "data" / "sources"
DATA_SOURCES_DIR.mkdir(parents=True, exist_ok=True)


def get_templates(request: Request):
    return request.app.state.templates
def get_df_store(request: Request):
    return request.app.state.df_store  # type: ignore[attr-defined]    

@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    logger.info("Rendering home page")
    templates = get_templates(request)
    return templates.TemplateResponse("index.html", {"request": request})

def get_preview(request : Request, df, source_id, preview_message = ""):
        #Build preview
    templates = get_templates(request)
    preview_df = df.head(10)
    table_html = preview_df.to_html(classes="preview-table", index=False)

    logger.debug("Generated preview for %s (10 rows)", source_id)

    return templates.TemplateResponse(
        "partials/source_preview.html",
        {
            "request": request,
            "filename": f"filename: (Source ID: {source_id})",
            "preview_html": preview_message + " " +table_html,
            "source_id": source_id
        },
    )
@router.get("/sources", response_class=HTMLResponse)
async def sources_page(request: Request):
    """
    Page where user can manage data sources (starting with CSV upload).
    """

    templates = get_templates(request)
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
async def upload_csv_source(request: Request,
                             file: UploadFile = File(...),
                               skip_rows: int = Form(0),):
    """
    Handle CSV upload, read into pandas, and return an HTML preview
    (first 10 rows) as a partial template.

    This endpoint is called via HTMX from the form on sources.html.
    """
    templates = get_templates(request)
    df_store = get_df_store(request)  # type: ignore[attr-defined]
    logger.info("Received CSV upload: filename=%s, content_type=%s",
                file.filename, file.content_type)

    content = await file.read()
    logger.debug("CSV file %s size: %d bytes", file.filename, len(content))

    try:
        if skip_rows and skip_rows > 0:
            df = pd.read_csv(BytesIO(content), skiprows=skip_rows)
        else:
            df = pd.read_csv(BytesIO(content))

        row_count, col_count = df.shape
        logger.info(
            "CSV parsed successfully: filename=%s, shape=(%d, %d), skip_rows=%d",
            file.filename, row_count, col_count, skip_rows
        )
    except Exception as e:
        logger.exception("Failed to read CSV file: %s skip_rows: %s", file.filename,skip_rows)
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

    # Put DataFrame into in-memory workspace
    df_store[source_id] = df
    # Initialize an empty pipeline for this source
    pipeline_store = request.app.state.pipeline_store  # type: ignore[attr-defined]
    pipeline_store[source_id] = []

    #Build preview
    return get_preview(request,df,source_id)
   
@router.post("/sources/{source_id}/open", response_class=HTMLResponse)
async def open_source(request: Request, source_id: int):
    templates = get_templates(request)
    df_store = get_df_store(request)

    df = df_store.get(source_id)

    if df is None:
        csv_path = DATA_SOURCES_DIR /f"source_id.csv"
        if not csv_path.exists():
            logger.error("Open requested for missing source_id=%s", source_id)
            return templates.TemplateResponse(
                "partials/source_preview.html",
                {
                    "request": request,
                    "filename": f"Source {source_id}",
                    "preview_html": "<p>Source file not found.</p>",
                    "source_id": source_id,
                },
            )
        try:
            df = pd.read_csv(csv_path)
            df_store[source_id] = df
        except Exception as e:
            logger.exception("Failed to read CSV for open, source_id=%s", source_id)
            return templates.TemplateResponse(
                "partials/source_preview.html",
                {
                    "request": request,
                    "filename": f"Source {source_id}",
                    "preview_html": f"<p>Failed to load CSV: {e}</p>",
                    "source_id": source_id,
                },
            )
    else:
        logger.info("Reusing in-memory DataFrame for source_id=%s", source_id)

    return get_preview(request,df,source_id)
    # preview_df = df.head(10)
    # table_html = preview_df.to_html(classes="preview-table", index=False)

    # return templates.TemplateResponse(
    #     "partials/source_preview.html",
    #     {
    #         "request": request,
    #         "filename": f"Source (ID: {source_id})",
    #         "preview_html": table_html,
    #         "source_id": source_id,
    #     },
    # )            

def get_df(request,source_id):
    templates = get_templates(request)
    df_store = get_df_store(request)
    df = df_store.get(source_id)
    if df is None:
        csv_path = DATA_SOURCES_DIR / f"source_{source_id}.csv"
        if not csv_path.exists():
            logger.error("Error loading csv file from datasource for source_id=%s with source path = %s", source_id, csv_path )
            return None
        try:
            df = pd.read_csv(csv_path)
            df_store[source_id] = df
            logger.info("Loaded source_id=%s into df_store for validation", source_id)
        except Exception:
            logger.exception("Failed to read CSV for validation, source_id=%s", source_id)
            return None
    return df
        
@router.post("/sources/{source_id}/validate",response_class=HTMLResponse)
async def validate_source(request: Request, source_id: int):
    """
    - Run basic validation on a stored CSV source:
    - null counts
    - null %
    - dtype
    - sample values
    """
    logger.info("Started source validation request for source id %s",source_id)
    templates = get_templates(request)
    df = get_df(request,source_id)
    if df is None:
        return templates.TemplateResponse(
                "partials/validation_report.html",
                {
                    "request": request,
                    "row_count": 0,
                    "report": [],
                    "source_id": source_id,
                },
            )

    row_count = len(df)
    report = []

    for col_name  in df.columns:
        series = df[col_name]
        null_count = int(series.isna().sum())
        total = len(series)
        null_pct = round((null_count / total) * 100,2) if total else 0.0
        dtype = str(series.dtype)
        non_null_samples = series.dropna().unique()[:3]
        sample_values = ", ".join(map(str, non_null_samples))

        report.append({
            "name": col_name,
            "dtype": dtype,
            "null_count": null_count,
            "null_pct": null_pct,
            "sample_values": sample_values,
        })

    logger.info("Validation done for source_id = %s",source_id)

    return templates.TemplateResponse(
        "partials/validation_report.html",
        {
            "request":request,
            "row_count": row_count,
            "report": report,
            "source_id": source_id,
        }
    )

@router.post("/sources/{source_id}/clean/drop-null-rows",response_class=HTMLResponse)
async def clean_source_drop_null_rows(request: Request, source_id = int):
    """
    Clean the dataset by dropping any rows that contain null values.
    Save cleaned CSV as source_<id>_clean.csv and return updated preview.
    """
    templates = get_templates(request)
   
    df = get_df(request,source_id)
    if df is None:
        return templates.TemplateResponse(
                "partials/source_preview.html",
                {
                    "request": request,
                    "filename": "Unknown source",
                    "preview_html": f"<p>Failed to read CSV for cleaning:</p>",
                    "source_id": source_id,
                },
            )
    
    before_rows = len(df)
    cleaned_df = df.dropna(how="any")
    after_rows = len(cleaned_df)
    removed = before_rows - after_rows
    df_store = get_df_store(request)  # type: ignore[attr-defined]
    df_store[source_id] = cleaned_df

    # Record this step in the pipeline
    pipeline_store = request.app.state.pipeline_store  # type: ignore[attr-defined]
    add_step_drop_rows_with_nulls(pipeline_store, source_id, subset=None)


    message_html = (
        f"<p>Cleaned by dropping rows with any nulls. "
        f"Removed {removed} rows (from {before_rows} to {after_rows}).</p>"
    )
    return get_preview(request,df,source_id,message_html)

@router.get("/sources/{source_id}/download")
async def download_source(source_id: int):
    """
    Download the cleaned CSV if available, otherwise the raw CSV.
    """
    cleaned_path = DATA_SOURCES_DIR / f"source_{source_id}_clean.csv"
    raw_path = DATA_SOURCES_DIR / f"source_{source_id}.csv"

    if cleaned_path.exists():
        logger.info("Download cleaned CSV for source_id=%s", source_id)
        return FileResponse(
            cleaned_path,media_type = "text/csv",
            filename = f"source_{source_id}_clean.csv"
        )
    elif  raw_path.exists():
        logger.info("Download raw CSV (no cleaned version) for source_id=%s", source_id)
        return FileResponse(
            raw_path,
            media_type="text/csv",
            filename=f"source_{source_id}.csv",
        )
    else:
        logger.error("Download requested for missing source_id=%s", source_id)
        raise HTTPException(status_code=404, detail="Source not found")        

@router.post("/sources/{source_id}/clean/drop-columns",response_class=HTMLResponse)
async def clean_source_drop_columns(
    request: Request, source_id: int, columns: List[str] = Form(None)
):
    logger.info("Started dropping NA columns for source_id %s",source_id)
    templates = get_templates(request)
    df = get_df(request,source_id)
    if df is None:
        return templates.TemplateResponse(
                "partials/source_preview.html",
                {
                    "request": request,
                    "filename": "Unknown source",
                    "preview_html": f"<p>Failed to read CSV for drop-columns:</p>",
                    "source_id": source_id,
                },
            )
  
    if not columns:
        # Nothing selected -> just re-show current preview
        logger.info("No columns selected to drop for source_id=%s", source_id)
        preview_message = "<p>No columns selected to drop.</p>"
        return get_preview(request,df,source_id,preview_message)

    before_cols = df.shape[1]
    remaining_cols = [c for c in df.columns if c not in columns]
    cleaned_df = df[remaining_cols]
    after_cols = cleaned_df.shape[1]
    df_store = get_df_store(request)  # type: ignore[attr-defined]
    df_store[source_id] = cleaned_df

    # Record this step in the pipeline
    pipeline_store = request.app.state.pipeline_store  # type: ignore[attr-defined]
    add_step_drop_columns(pipeline_store, source_id, columns)

    message_html = (
        f"<p>Dropped columns: {', '.join(columns)}. "
        f"Columns reduced from {before_cols} to {after_cols}.</p>"
    )
    return get_preview(request,cleaned_df,source_id,message_html)


@router.post("/sources/{source_id}/save", response_class=HTMLResponse)
async def save_source(request: Request, source_id: int):
    """
    Save the current in-memory DataFrame for this source back to disk
    and update row/column counts in the database.
    """
    templates = request.app.state.templates
    df_store = request.app.state.df_store  # type: ignore[attr-defined]

    df = df_store.get(source_id)
    if df is None:
        logger.error("Save requested but no in-memory DataFrame for source_id=%s", source_id)
        return templates.TemplateResponse(
            "partials/source_preview.html",
            {
                "request": request,
                "filename": f"Source (ID: {source_id})",
                "preview_html": "<p>No in-memory data to save. Open or upload first.</p>",
                "source_id": source_id,
            },
        )

    # Save to CSV (overwrite original)
    csv_path = DATA_SOURCES_DIR / f"source_{source_id}.csv"
    try:
        df.to_csv(csv_path, index=False)
        logger.info("Saved in-memory DataFrame to %s for source_id=%s", csv_path, source_id)
    except Exception:
        logger.exception("Failed to save CSV for source_id=%s", source_id)
        return templates.TemplateResponse(
            "partials/source_preview.html",
            {
                "request": request,
                "filename": f"Source (ID: {source_id})",
                "preview_html": "<p>Failed to save CSV to disk.</p>",
                "source_id": source_id,
            },
        )

    # Update metadata in DB
    row_count, col_count = df.shape
    try:
        update_data_source_shape(source_id, row_count, col_count)
    except Exception:
        # We already saved the file; just log DB error.
        logger.exception("Failed to update shape in DB for source_id=%s", source_id)

    # Rebuild preview with a success message
    preview_df = df.head(10)
    table_html = preview_df.to_html(classes="preview-table", index=False)

    message_html = (
        f"<p>Saved current state. Shape is now ({row_count}, {col_count}).</p>"
    )

    return templates.TemplateResponse(
        "partials/source_preview.html",
        {
            "request": request,
            "filename": f"Saved Source (ID: {source_id})",
            "preview_html": message_html + table_html,
            "source_id": source_id,
        },
    )


@router.post("/sources/delete", response_class=HTMLResponse)
async def delete_sources_route(
    request: Request,
    source_ids: List[int] = Form(None),
):
    """
    Delete one or more sources:
    - remove DB rows
    - delete CSV files
    - drop from in-memory df_store
    Then return the updated sources table partial.
    """
    templates = request.app.state.templates
    df_store = request.app.state.df_store  # type: ignore[attr-defined]

    ids_to_delete = source_ids or []
    logger.info("Requested deletion for source_ids=%s", ids_to_delete)

    # Delete from DB first
    if ids_to_delete:
        try:
            delete_data_sources(ids_to_delete)
        except Exception:
            logger.exception("Failed to delete sources in DB")

        # Delete files + in-memory dfs
        for sid in ids_to_delete:
            csv_path = DATA_SOURCES_DIR / f"source_{sid}.csv"
            try:
                if csv_path.exists():
                    csv_path.unlink()
                    logger.info("Deleted CSV file %s for source_id=%s", csv_path, sid)
            except Exception:
                logger.exception("Failed to delete CSV file for source_id=%s", sid)

            if sid in df_store:
                df_store.pop(sid, None)
                logger.info("Removed in-memory DataFrame for source_id=%s", sid)

    # Get refreshed list of sources and return the table partial
    try:
        sources = get_all_data_sources()
    except Exception:
        logger.exception("Error fetching data sources after delete")
        sources = []

    return templates.TemplateResponse(
        "partials/sources_table.html",
        {
            "request": request,
            "sources": sources,
        },
    )

@router.post("/sources/{source_id}/export-config", response_class=HTMLResponse)
async def export_pipeline_config(request: Request, source_id: int):
    pipeline_store = request.app.state.pipeline_store  # type: ignore[attr-defined]
    steps = get_steps_for_source(pipeline_store, source_id)
    if not steps:
        # No steps recorded yet
        html = "<p>No pipeline steps recorded for this source yet.</p>"
        return HTMLResponse(html)

    # Get source name (optional)
    ds = get_data_source_by_id(source_id)
    source_name = ds["name"] if ds else None

    config_dict = build_pipeline_config(source_id, source_name, steps)
    config_json = json.dumps(config_dict, indent=2)

    html = (
        "<h4>Pipeline Config (JSON)</h4>"
        "<p>You can copy this and save it as a .json file for later reuse.</p>"
        f"<pre>{config_json}</pre>"
    )
    return HTMLResponse(html)

@router.post("/sources/{source_id}/replay", response_class=HTMLResponse)
async def replay_pipeline_from_raw(request: Request, source_id: int):
    """
    Reset to the raw CSV for this source_id, re-apply all recorded pipeline steps,
    update the in-memory DataFrame, and refresh the preview.
    """
    templates = request.app.state.templates
    df_store = request.app.state.df_store  # type: ignore[attr-defined]
    pipeline_store = request.app.state.pipeline_store  # type: ignore[attr-defined]

    steps = get_steps_for_source(pipeline_store, source_id)
    if not steps:
        logger.info("Replay requested but no steps recorded for source_id=%s", source_id)
        return templates.TemplateResponse(
            "partials/source_preview.html",
            {
                "request": request,
                "filename": f"Source (ID: {source_id})",
                "preview_html": "<p>No pipeline steps recorded yet. Nothing to replay.</p>",
                "source_id": source_id,
            },
        )

    # Load raw CSV from disk
    csv_path = DATA_SOURCES_DIR / f"source_{source_id}.csv"
    if not csv_path.exists():
        logger.error("Replay requested but raw CSV missing for source_id=%s", source_id)
        return templates.TemplateResponse(
            "partials/source_preview.html",
            {
                "request": request,
                "filename": f"Source (ID: {source_id})",
                "preview_html": "<p>Raw source file not found. Cannot replay.</p>",
                "source_id": source_id,
            },
        )

    try:
        raw_df = pd.read_csv(csv_path)
    except Exception as e:
        logger.exception("Failed to read raw CSV for replay, source_id=%s", source_id)
        return templates.TemplateResponse(
            "partials/source_preview.html",
            {
                "request": request,
                "filename": f"Source (ID: {source_id})",
                "preview_html": f"<p>Failed to read raw CSV: {e}</p>",
                "source_id": source_id,
            },
        )

    # Apply pipeline steps
    transformed_df = apply_pipeline_to_df(raw_df, steps)
    df_store[source_id] = transformed_df

    preview_df = transformed_df.head(10)
    table_html = preview_df.to_html(classes="preview-table", index=False)

    message_html = (
        "<p>Replayed pipeline from raw CSV. "
        f"Final shape: ({transformed_df.shape[0]}, {transformed_df.shape[1]}).</p>"
    )

    return templates.TemplateResponse(
        "partials/source_preview.html",
        {
            "request": request,
            "filename": f"Replayed Source (ID: {source_id})",
            "preview_html": message_html + table_html,
            "source_id": source_id,
        },
    )
