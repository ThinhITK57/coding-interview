import pandas as pd
from datetime import datetime
from collections import defaultdict
from io import BytesIO
from pathlib import Path



# ---------------------------------------------------------------------------
# Post-process helpers
# ---------------------------------------------------------------------------

def dedup_by_employee_id(df: pd.DataFrame) -> pd.DataFrame:
    agg = {col: "first" for col in df.columns}
    agg["status"] = "last"
    return df.groupby("employee_id", as_index=False).agg(agg)


def dedup_by_employee_code(df: pd.DataFrame) -> pd.DataFrame:
    agg = {col: "first" for col in df.columns}
    agg["current_status"] = "last"
    return df.groupby("employee_code", as_index=False).agg(agg)


def transform_product_revenue_plan(df: pd.DataFrame) -> pd.DataFrame:
    """
    NOTE: expects drop_rows=3 so df still contains the 3 raw header rows at
    the top (rows 0,1,2). We flatten them here before processing.
    """
    import re as _re

    # ── Step 1: flatten the 3-row multi-level header ─────────────────────────
    h0 = df.iloc[0].tolist()  # top-level: "KH T1/25", "KH T2/25" ...
    h1 = df.iloc[1].tolist()  # mid-level: "MUST", "NICE", ...
    h2 = df.iloc[2].tolist()  # bottom-level (often empty/unnamed)

    new_cols = []
    for i, (a, b, c) in enumerate(zip(h0, h1, h2)):
        parts = [str(x).strip() for x in [a, b, c]
                 if pd.notna(x) and str(x).strip() not in ("", "nan", "None")]
        new_cols.append("_".join(parts) if parts else f"col_{i}")

    df = df.iloc[3:].reset_index(drop=True)
    df.columns = new_cols

    # ── Step 2: everything below is same as notebook logic ───────────────────
    def is_roman(val):
        if pd.isna(val) or str(val).strip() == "":
            return False
        return bool(_re.fullmatch(r"[IVXLCDM]+", str(val).strip().upper()))

    cols = list(df.columns)
    df = df.rename(columns={cols[0]: "tt", cols[1]: "ma_spdv", cols[2]: "ten_spdv"})

    df["is_group"] = (
        df["ma_spdv"].isna()
        | df["ma_spdv"].astype(str).str.strip().eq("")
        | df["tt"].apply(is_roman)
    )
    df["product_group"] = df["ten_spdv"].where(df["is_group"]).ffill()
    df_detail = df[~df["is_group"]].copy().rename(columns={
        "tt": "row_no", "ma_spdv": "product_code", "ten_spdv": "product_name",
    })

    value_cols = [c for c in df_detail.columns if "KH T" in c and ("MUST" in c.upper() or "NICE" in c.upper())]

    if not value_cols:
        raise ValueError(f"No MUST/NICE KH T columns found after flattening. Columns: {list(df_detail.columns)}")

    df_long = df_detail.melt(
        id_vars=["row_no", "product_code", "product_name", "product_group"],
        value_vars=value_cols, var_name="raw_column", value_name="target_value",
    )

    def parse_col(col):
        m = _re.search(r"T(\d+)/(\d+)", col)
        if not m:
            return None, None, None, None
        month = int(m.group(1))
        yy = m.group(2)
        year = int("20" + yy) if len(yy) == 2 else int(yy)
        target_type = "must" if "MUST" in col.upper() else "nice"
        return f"{year}-{month:02d}", year, month, target_type

    df_long[["period", "year", "month", "target_type"]] = df_long["raw_column"].apply(
        lambda c: pd.Series(parse_col(c))
    )

    df_long["target_value"] = (
        pd.to_numeric(
            df_long["target_value"].astype(str).str.replace(",", "", regex=False),
            errors="coerce"
        ).fillna(0).mul(1_000_000).astype("int64")
    )

    df_long = df_long.drop(columns=["raw_column"])
    df_long["currency_code"] = "VND"
    df_long["snapshot_at"] = pd.Timestamp.now(tz="Asia/Ho_Chi_Minh").strftime("%Y-%m-%d %H:%M:%S")
    df_long["snapshot_version"] = 1

    df_wide = (
        df_long.pivot_table(
            index=["row_no", "product_group", "product_code", "product_name",
                   "year", "month", "period", "currency_code", "snapshot_at", "snapshot_version"],
            columns="target_type", values="target_value", aggfunc="sum", fill_value=0,
        ).reset_index()
    )
    df_wide.columns.name = None
    for col in ["must", "nice"]:
        if col not in df_wide.columns:
            df_wide[col] = 0
    df_wide["must"] = df_wide["must"].astype("int64")
    df_wide["nice"] = df_wide["nice"].astype("int64")

    return df_wide.sort_values(["product_group", "product_code", "year", "month"])

def enrich_actual_cost(df: pd.DataFrame) -> pd.DataFrame:
    """
    Derived columns for actual_cost computed inline in the notebook.
    Runs after column normalisation so canonical names are already in place.
    """
    df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce")
    df["report_year"] = df["report_date"].dt.year
    df["report_month"] = pd.to_numeric(df["report_month"], errors="coerce")
    df["base_currency_amount"] = pd.to_numeric(df["base_currency_amount"], errors="coerce") * 1_000_000
    df["currency_code"] = "VND"
    df["territory_name"] = df["old_category"].str.contains("HCM", case=False, na=False).map(
        {True: "Mien Nam", False: "Mien Bac"}
    )
    df["product_category"] = ""
    df["unit_level_1"] = ""
    df["report_date"] = df["report_date"].dt.strftime("%Y-%m-%d")
    return df
