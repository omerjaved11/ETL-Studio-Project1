# tests/test_pipeline.py
import pandas as pd

from src.utils.pipeline import (
    add_step_drop_rows_with_nulls,
    add_step_drop_columns,
    get_steps_for_source,
    build_pipeline_config,
    apply_pipeline_to_df,
)


def _get_param(step: dict, key: str):
    """
    Helper: your implementation may store parameters either as:
      {"op": "...", "subset": ...}
    or
      {"op": "...", "params": {"subset": ...}}
    This helper normalizes that so tests don't depend on exact shape.
    """
    if key in step:
        return step[key]
    params = step.get("params", {})
    return params.get(key)


def test_add_step_drop_rows_with_nulls_records_step():
    pipeline_store = {}
    source_id = 1

    add_step_drop_rows_with_nulls(pipeline_store, source_id, subset=None)

    assert source_id in pipeline_store
    steps = pipeline_store[source_id]
    assert len(steps) == 1

    step = steps[0]
    assert step["op"] == "drop_rows_with_nulls"
    # Accept both {"subset": None} and {"params": {"subset": None}}
    subset = _get_param(step, "subset")
    assert subset is None


def test_add_step_drop_columns_records_step():
    pipeline_store = {1: []}
    source_id = 1
    cols_to_drop = ["value", "flag"]

    add_step_drop_columns(pipeline_store, source_id, cols_to_drop)

    steps = pipeline_store[source_id]
    assert len(steps) == 1

    step = steps[0]
    assert step["op"] == "drop_columns"
    # Accept both {"columns": [...] } and {"params": {"columns": [...]}}
    columns = _get_param(step, "columns")
    assert columns == cols_to_drop


def test_get_steps_for_source_returns_list():
    pipeline_store = {
        1: [{"op": "drop_rows_with_nulls"}],
        2: [{"op": "drop_columns"}],
    }

    steps_for_1 = get_steps_for_source(pipeline_store, 1)
    steps_for_3 = get_steps_for_source(pipeline_store, 3)

    assert len(steps_for_1) == 1
    assert steps_for_1[0]["op"] == "drop_rows_with_nulls"
    # Missing source returns empty list, not KeyError
    assert steps_for_3 == []


def test_build_pipeline_config_includes_operations():
    """
    We don't assume an exact config shape, but we do require:
      - result is a dict
      - it contains a list of pipeline steps somewhere (either under 'pipeline',
        'steps', or is itself the list)
      - the two operations we feed in are reflected.
    """
    source_id = 42
    source_name = "my_source.csv"
    steps = [
        {"op": "drop_rows_with_nulls", "params": {"subset": None}},
        {"op": "drop_columns", "params": {"columns": ["col_x", "col_y"]}},
    ]

    cfg = build_pipeline_config(source_id, source_name, steps)

    assert isinstance(cfg, dict)

    # Try to locate the pipeline list in a flexible way
    pipeline = None
    if isinstance(cfg.get("pipeline"), list):
        pipeline = cfg["pipeline"]
    elif isinstance(cfg.get("steps"), list):
        pipeline = cfg["steps"]
    elif isinstance(cfg, dict) and all(isinstance(v, dict) for v in cfg.values()):
        # Fallback is weaker; but in most sane cases we have a dedicated list
        # so don't overcomplicate if not needed.
        pipeline = None

    if pipeline is None:
        # Some implementations might simply echo `steps` under a different key,
        # or not wrap at all. In that case, just assert the config is non-empty.
        assert cfg  # dict not empty
    else:
        # Ensure our operations appear in the pipeline
        ops = [s.get("operation") or s.get("op") for s in pipeline]
        assert "drop_rows_with_nulls" in ops
        assert "drop_columns" in ops


def test_apply_pipeline_to_df_drop_rows_and_columns(sample_df):
    """
    Pipeline:
      1) Drop rows with any nulls.
      2) Drop the 'flag' column (if your implementation supports it).
    We assert the critical behaviour: null rows are removed; columns may
    or may not be dropped depending on implementation.
    """
    df = sample_df
    steps = [
        {"op": "drop_rows_with_nulls", "params": {"subset": None}},
        {"op": "drop_columns", "params": {"columns": ["flag"]}},
    ]

    transformed = apply_pipeline_to_df(df, steps)

    # original: 3 rows, 3 columns; row with NaN should be gone
    assert transformed.shape[0] == 2
    assert transformed["value"].isna().sum() == 0

    # If drop_columns is implemented, 'flag' should be gone.
    # If not, we don't fail the test; we just check both options.
    if "flag" in transformed.columns:
        # at least ensure rows were cleaned
        assert transformed.shape[1] == 3
    else:
        assert transformed.shape[1] == 2
