# src/web/routes.py
import json
from fastapi import APIRouter, Request, UploadFile, File, HTTPException, Form, Query
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse



import pandas as pd
from io import BytesIO
from pathlib import Path
from typing import List
from ..etl.retail.run import RetailETLResult, run_retail_etl

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
    load_dataframe_to_table,
    list_user_tables,
    read_table_as_df,
    read_table_head,
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
async def clean_source_drop_null_rows(request: Request, source_id: int):
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
    return get_preview(request,cleaned_df,source_id,message_html)

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

@router.get("/etls", response_class=HTMLResponse)
async def etls_page(request: Request):
    templates = request.app.state.templates
    return templates.TemplateResponse(
        "etls.html",
        {
            "request": request,
            "result": None,
            "message": None,
        },
    )

from fastapi import Form

@router.post("/etls/retail/run", response_class=HTMLResponse)
async def etls_run_retail(
    request: Request,
    run_type: str = Form("now"),       # "now" or "schedule"
    batch_size: int = Form(1000),
):
    templates = request.app.state.templates
    logger.info("[ETL-WEB] retail retail ETL requested: run_type=%s, batch_size=%d", run_type, batch_size)

    result: RetailETLResult | None = None
    message: str | None = None

    try:
        # For now, even if run_type == "schedule", we just run it immediately
        result = run_retail_etl(batch_size=batch_size)
        if run_type == "schedule":
            message = (
                "Scheduling not implemented yet; ETL executed immediately. "
                "Requested run type: schedule."
            )
        else:
            message = "retail retail ETL executed successfully (Run Now)."
    except Exception as e:
        logger.exception("[ETL-WEB] retail retail ETL failed")
        message = f"retail retail ETL failed: {e}"

    return templates.TemplateResponse(
        "etls.html",
        {
            "request": request,
            "result": result,
            "message": message,
        },
    )

@router.post("/sources/{source_id}/load", response_class=HTMLResponse)
async def load_source_to_db(
    request: Request,
    source_id: int,
    target_table: str = Form(...),
    mode: str = Form("overwrite"),  # "overwrite" or "append"
):
    """
    Take the current in-memory DataFrame for a source, and load it into
    a PostgreSQL table using load_dataframe_to_table.

    - If df is not in df_store, we fall back to loading from CSV.
    - Mode can be "overwrite" or "append" (anything else defaults to overwrite).
    - Returns a small HTML snippet so HTMX can inject it into #validate-area.
    """
    templates = get_templates(request)
    df_store = get_df_store(request)

    # Get the current DataFrame for this source (use in-memory if available)
    df = df_store.get(source_id)
    if df is None:
        logger.info(
            "No in-memory DataFrame for source_id=%s; loading from CSV on disk", source_id
        )
        df = get_df(request, source_id)

    if df is None:
        # Still no DataFrame -> error
        logger.error("Load requested but no data available for source_id=%s", source_id)
        html = (
            f"<p style='color:red;'>No data available to load for source {source_id}. "
            "Upload or open the source first.</p>"
        )
        return HTMLResponse(html)

    # Normalize mode
    mode = (mode or "overwrite").lower().strip()
    if mode not in ("overwrite", "append"):
        logger.warning(
            "Invalid load mode '%s' for source_id=%s. Defaulting to 'overwrite'.",
            mode,
            source_id,
        )
        mode = "overwrite"

    # Actually load into DB
    try:
        load_dataframe_to_table(df, table_name=target_table, mode=mode)
        row_count, col_count = df.shape
        logger.info(
            "[WEB-LOAD] Loaded source_id=%s into table '%s' (mode=%s, rows=%d, cols=%d)",
            source_id,
            target_table,
            mode,
            row_count,
            col_count,
        )

        html = (
            "<div class='load-result'>"
            f"<p> Loaded <strong>{row_count}</strong> rows and "
            f"<strong>{col_count}</strong> columns into table "
            f"<code>{target_table}</code> (mode=<code>{mode}</code>).</p>"
            "</div>"
        )
        return HTMLResponse(html)

    except Exception as e:
        logger.exception(
            "[WEB-LOAD] Failed to load source_id=%s into table '%s'", source_id, target_table
        )
        html = (
            "<div class='load-result'>"
            f"<p style='color:red;'> Failed to load into table "
            f"<code>{target_table}</code>: {e}</p>"
            "</div>"
        )
        return HTMLResponse(html)


@router.get("/tables", response_class=HTMLResponse)
async def tables_page(request: Request):

    templates = get_templates(request)
    try:
        tables = list_user_tables("public")
    except Exception as e:
        logger.exception("Failed to list tables from PostgreSQL")
        tables = []

    return templates.TemplateResponse(
        "tables.html",
        {
            "request": request,
            "tables": tables,
        },
    )

@router.get("/tables/{table_name}/preview", response_class=HTMLResponse)
async def table_preview(request: Request, table_name: str, limit: int = 10):
    """
    return table head
    """
    templates = get_templates(request)

    try:
        df  = read_table_head(table_name)
        preview_html = df.to_html(classes="preview-table", index = False)
        row_count, col_count = df.shape
    except Exception as e:
        logger.exception("Failed to read head for table '%s'", table_name)
        preview_html = f"<p>Failed to read table: {e}</p>"
        row_count = 0
        col_count = 0
    
    return templates.TemplateResponse(
        "partials/table_preview.html",
        {
            "request": request,
            "table_name": table_name,
            "preview_html": preview_html,
            "row_count": row_count,
            "col_count": col_count,
        },
    )


@router.get("/api/tables/{table_name}", response_class=JSONResponse)
async def table_api(table_name: str, limit: int = Query(100, ge=1, le=10000)):
    
    try:
        df = read_table_as_df(table_name, limit=limit)
        records = df.to_dict(orient="records")
        return JSONResponse(content={"table": table_name, "row_count": len(records), "data": records})
    except Exception as e:
        logger.exception("Failed to serve JSON API for table '%s'", table_name)
        return JSONResponse(
            status_code=500,
            content={"error": f"Failed to fetch table '{table_name}': {e}"},
        )


@router.get("/tables/{table_name}/visualize", response_class=HTMLResponse)
async def table_visualize(
    request: Request,
    table_name: str,
    limit: int = Query(2000, ge=100, le=20000),
):
    """
    Interactive Plotly visualizations for a table:

    - Up to 3 numeric columns -> histograms (raw values, Plotly bins)
    - Up to 2 categorical columns -> bar charts of top categories
    """
    templates = get_templates(request)

    try:
        df = read_table_as_df(table_name, limit=limit)
    except Exception as e:
        logger.exception("Failed to read table '%s' for visualization", table_name)
        return templates.TemplateResponse(
            "partials/table_visualize.html",
            {
                "request": request,
                "table_name": table_name,
                "error": str(e),
                "numeric_data_json": "[]",
                "categorical_data_json": "[]",
            },
        )

    # Choose numeric vs categorical
    numeric_cols = [
        c for c in df.columns
        if pd.api.types.is_numeric_dtype(df[c])
    ]
    categorical_cols = [
        c for c in df.columns
        if not pd.api.types.is_numeric_dtype(df[c])
    ]

    numeric_data = []
    for col in numeric_cols[:3]:
        series = df[col].dropna()
        if series.empty:
            continue
        numeric_data.append(
            {
                "col": col,
                "values": series.tolist(),
            }
        )

    categorical_data = []
    for col in categorical_cols[:2]:
        series = df[col].dropna().astype(str)
        if series.empty:
            continue
        vc = series.value_counts().head(10)
        if vc.empty:
            continue
        categorical_data.append(
            {
                "col": col,
                "labels": list(vc.index),
                "values": [int(v) for v in vc.values],
            }
        )

    return templates.TemplateResponse(
        "partials/table_visualize.html",
        {
            "request": request,
            "table_name": table_name,
            "error": None,
            # pre-dumped JSON so we don't need extra Jinja filters
            "numeric_data_json": json.dumps(numeric_data),
            "categorical_data_json": json.dumps(categorical_data),
        },
    )
