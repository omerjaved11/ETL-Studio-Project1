# src/etl/retail_transform.py
from __future__ import annotations
from typing import List

import pandas as pd
from pandas.api import types as ptypes

from ...utils.logger import get_logger

logger = get_logger(__name__)


def _standardize_key(
    df: pd.DataFrame,
    candidates: List[str],
    target_name: str,
) -> pd.DataFrame:
    """
    If any candidate column exists, copy it to a standardized column name (string).
    Example: candidates=['product_id','ProductID'] -> target 'product_id'
    """
    df = df.copy()
    found = [c for c in candidates if c in df.columns]
    if not found:
        logger.warning(
            "[TRANSFORM] No key column found for %s in %s",
            target_name, list(df.columns),
        )
        return df

    src = found[0]
    logger.info(
        "[TRANSFORM] Standardizing key column '%s' -> '%s'",
        src, target_name,
    )
    df[target_name] = df[src].astype(str).str.strip()
    return df


def _drop_all_na_columns(df: pd.DataFrame, label: str) -> pd.DataFrame:
    before_cols = df.shape[1]
    df_clean = df.dropna(axis="columns", how="all")
    after_cols = df_clean.shape[1]
    dropped = before_cols - after_cols
    if dropped:
        logger.info(
            "[TRANSFORM] [%s] Dropped %d all-NA columns (from %d to %d)",
            label, dropped, before_cols, after_cols,
        )
    return df_clean


def _remove_duplicates(df: pd.DataFrame, label: str, subset: List[str] | None = None) -> pd.DataFrame:
    before = len(df)
    df_clean = df.drop_duplicates(subset=subset)
    after = len(df_clean)
    removed = before - after
    if removed:
        logger.info(
            "[TRANSFORM] [%s] Removed %d duplicate rows (from %d to %d) subset=%s",
            label, removed, before, after, subset,
        )
    return df_clean


def _fill_na(df: pd.DataFrame, label: str) -> pd.DataFrame:
    """
    Fill NA values:
      - numeric: median
      - non-numeric: mode
    """
    df_clean = df.copy()
    for col in df_clean.columns:
        series = df_clean[col]
        if series.isna().sum() == 0:
            continue

        if ptypes.is_numeric_dtype(series):
            median_val = series.median()
            df_clean[col] = series.fillna(median_val)
            logger.info(
                "[TRANSFORM] [%s] Filled NA in numeric column '%s' with median=%s",
                label, col, median_val,
            )
        else:
            modes = series.mode(dropna=True)
            if len(modes) > 0:
                mode_val = modes.iloc[0]
                df_clean[col] = series.fillna(mode_val)
                logger.info(
                    "[TRANSFORM] [%s] Filled NA in non-numeric column '%s' with mode=%r",
                    label, col, mode_val,
                )
            else:
                logger.info(
                    "[TRANSFORM] [%s] Could not determine mode for '%s'; leaving NA",
                    label, col,
                )
    return df_clean


def _drop_na_rows(df: pd.DataFrame, label: str) -> pd.DataFrame:
    before = len(df)
    df_clean = df.dropna(how="any")
    after = len(df_clean)
    dropped = before - after
    if dropped:
        logger.info(
            "[TRANSFORM] [%s] Dropped %d rows containing NA (from %d to %d)",
            label, dropped, before, after,
        )
    return df_clean


def _remove_outliers_iqr(df: pd.DataFrame, label: str, factor: float = 1.5) -> pd.DataFrame:
    """
    Remove outliers for numeric columns using IQR.
    """
    df_clean = df.copy()
    numeric_cols = [c for c in df_clean.columns if ptypes.is_numeric_dtype(df_clean[c])]

    if not numeric_cols:
        logger.info("[TRANSFORM] [%s] No numeric columns for outlier removal; skipping.", label)
        return df_clean

    before = len(df_clean)
    mask = pd.Series(True, index=df_clean.index)

    for col in numeric_cols:
        series = df_clean[col]
        if series.empty:
            continue
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        if iqr == 0:
            continue
        lower = q1 - factor * iqr
        upper = q3 + factor * iqr
        col_mask = series.between(lower, upper) | series.isna()
        mask &= col_mask

    df_clean = df_clean[mask]
    after = len(df_clean)
    removed = before - after
    if removed:
        logger.info(
            "[TRANSFORM] [%s] Removed %d outlier rows using IQR (from %d to %d)",
            label, removed, before, after,
        )
    return df_clean


def _parse_dates_if_present(df: pd.DataFrame, label: str) -> pd.DataFrame:
    """
    Try to parse date-like columns into datetime.
    We'll look for names containing 'date' (case-insensitive).
    """
    df = df.copy()
    for col in df.columns:
        if "date" in col.lower():
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce")
                logger.info(
                    "[TRANSFORM] [%s] Parsed column '%s' as datetime",
                    label, col,
                )
            except Exception as e:
                logger.warning(
                    "[TRANSFORM] [%s] Failed to parse '%s' as datetime: %s",
                    label, col, e,
                )
    return df


def transform_sales(sales_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean sales data:
    - standardize product_id and store_id keys
    - drop all-NA columns
    - remove duplicates
    - fill NA
    - drop remaining NA
    - remove outliers (IQR)
    - parse date columns
    """
    label = "sales"
    logger.info("[TRANSFORM] Cleaning sales data...")
    df = sales_df.copy()

    # standardize keys we know this dataset uses
    df = _standardize_key(df, ["product_id", "productID", "ProductID"], "product_id")
    df = _standardize_key(df, ["store_id", "StoreID", "storeId"], "store_id")

    df = _drop_all_na_columns(df, label)
    df = _remove_duplicates(df, label, subset=["product_id", "store_id"] if "product_id" in df.columns and "store_id" in df.columns else None)
    df = _fill_na(df, label)
    df = _drop_na_rows(df, label)
    df = _remove_outliers_iqr(df, label)
    df = _parse_dates_if_present(df, label)

    logger.info("[TRANSFORM] Final sales shape: %s", df.shape)
    return df


def transform_products(products_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean product_hierarchy data.
    """
    label = "products"
    logger.info("[TRANSFORM] Cleaning product_hierarchy data...")
    df = products_df.copy()
    df = _standardize_key(df, ["product_id", "ProductID", "productID"], "product_id")
    df = _drop_all_na_columns(df, label)
    df = _remove_duplicates(df, label, subset=["product_id"] if "product_id" in df.columns else None)
    df = _fill_na(df, label)
    df = _drop_na_rows(df, label)
    logger.info("[TRANSFORM] Final product_hierarchy shape: %s", df.shape)
    return df


def transform_stores(stores_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean store_cities data.
    """
    label = "stores"
    logger.info("[TRANSFORM] Cleaning store_cities data...")
    df = stores_df.copy()
    df = _standardize_key(df, ["store_id", "StoreID", "storeId"], "store_id")
    df = _drop_all_na_columns(df, label)
    df = _remove_duplicates(df, label, subset=["store_id"] if "store_id" in df.columns else None)
    df = _fill_na(df, label)
    df = _drop_na_rows(df, label)
    logger.info("[TRANSFORM] Final store_cities shape: %s", df.shape)
    return df


def join_sales_products_stores(
    sales_df: pd.DataFrame,
    products_df: pd.DataFrame,
    stores_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Join sales with products on product_id, and with stores on store_id.
    """
    df = sales_df.copy()

    if "product_id" in df.columns and "product_id" in products_df.columns:
        logger.info("[TRANSFORM] Joining sales + products on product_id")
        df = df.merge(
            products_df,
            on="product_id",
            how="left",
            suffixes=("", "_prod"),
        )
    else:
        logger.warning(
            "[RETAIL-ETL-TRANSFORM] Cannot join products: product_id missing in one of the DataFrames"
        )

    if "store_id" in df.columns and "store_id" in stores_df.columns:
        logger.info("[RETAIL-ETL-TRANSFORM] Joining sales + stores on store_id")
        df = df.merge(
            stores_df,
            on="store_id",
            how="left",
            suffixes=("", "_store"),
        )
    else:
        logger.warning(
            "[RETAIL-ETL-TRANSFORM] Cannot join stores: store_id missing in one of the DataFrames"
        )

    logger.info("[RETAIL-ETL-TRANSFORM] Final enriched (sales+products+stores) shape: %s", df.shape)
    return df
