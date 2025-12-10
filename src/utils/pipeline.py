# src/utils/pipeline.py
from typing import Dict, List, Any, Optional
import pandas as pd


def _get_pipeline(store: Dict[int, List[dict]], source_id: int) -> List[dict]:
    """
    Get the list of steps for a given source_id, creating it if missing.
    """
    if source_id not in store:
        store[source_id] = []
    return store[source_id]


def add_step_drop_rows_with_nulls(
    store: Dict[int, List[dict]],
    source_id: int,
    subset: Optional[List[str]] = None,
) -> None:
    """
    Record a 'drop_rows_with_nulls' step for this source.
    """
    pipeline = _get_pipeline(store, source_id)
    step: dict[str, Any] = {"op": "drop_rows_with_nulls"}
    if subset:
        step["subset"] = subset
    pipeline.append(step)


def add_step_drop_columns(
    store: Dict[int, List[dict]],
    source_id: int,
    columns: List[str],
) -> None:
    """
    Record a 'drop_columns' step for this source.
    """
    if not columns:
        return

    pipeline = _get_pipeline(store, source_id)
    step: dict[str, Any] = {
        "op": "drop_columns",
        "columns": columns,
    }
    pipeline.append(step)


def get_steps_for_source(
    store: Dict[int, List[dict]],
    source_id: int,
) -> List[dict]:
    """
    Return the list of steps for a source (empty list if none).
    """
    return store.get(source_id, [])


def build_pipeline_config(
    source_id: int,
    source_name: Optional[str],
    steps: List[dict],
) -> dict:
    """
    Build a simple pipeline config dict that can be serialized to JSON/YAML.
    Currently single-source; later we can extend to multi-source pipelines.
    """
    pipeline_name = source_name or f"source_{source_id}_pipeline"

    return {
        "pipeline_name": pipeline_name,
        "source": {
            "source_id": source_id,
            "name": source_name,
        },
        "steps": steps,
        "load": {
            "target_db": "default",      # placeholder for future
            "target_table": None,        # user will fill later
            "mode": "overwrite",         # or 'append'
        },
    }

def apply_pipeline_to_df(df: pd.DataFrame, steps: List[dict]) -> pd.DataFrame:
    """
    Apply a list of pipeline steps to a DataFrame and return the transformed DataFrame.
    This is what we'll use for 'replay pipeline from raw'.
    """
    result = df.copy()
    
    for step in steps:
        op = step.get("op")

        if op == "drop_columns":
            cols = step.get("columns", [])
            result = result[[c for c in result.columns if c not in cols]]

        elif op == "drop_rows_with_nulls":
            subset = step.get("subset")
            if subset:
                result = result.dropna(how="any", subset=subset)
            else:
                result = result.dropna(how="any")

        # future: fill_nulls, rename_columns, filter_rows, etc.

    return result
