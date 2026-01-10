from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


# ---------- Helpers ----------
def _read_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _to_month_str(x) -> str:
    if pd.isna(x):
        return ""
    if isinstance(x, str):
        return x[:7]
    try:
        return pd.to_datetime(x).strftime("%Y-%m")
    except Exception:
        return str(x)[:7]


def _month_key(month_str: str) -> int:
    # "2025-12" -> 202512
    s = (month_str or "").replace("-", "")
    return int(s) if s.isdigit() else 0


def _date_key(ts) -> int:
    # Timestamp -> YYYYMMDD int
    try:
        d = pd.to_datetime(ts)
        return int(d.strftime("%Y%m%d"))
    except Exception:
        return 0


def _infer_month(kpi: pd.DataFrame) -> str | None:
    if kpi.empty:
        return None
    if "month" not in kpi.columns:
        return None
    months = sorted({_to_month_str(m) for m in kpi["month"].dropna().unique()})
    return months[-1] if months else None


def _filter_to_month_by_date(
    df: pd.DataFrame,
    date_col: str | None,
    month: str,
) -> pd.DataFrame:
    if df.empty or not date_col or date_col not in df.columns:
        return df.copy()
    m = pd.to_datetime(df[date_col], errors="coerce").dt.strftime("%Y-%m")
    return df.loc[m == month].copy()


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


# ---------- Builders ----------
def build_dim_entity(fact: pd.DataFrame, kpi: pd.DataFrame) -> pd.DataFrame:
    entities = set()
    for df in [fact, kpi]:
        if not df.empty and "entity" in df.columns:
            entities.update(df["entity"].dropna().astype(str).unique().tolist())
    ent_sorted = sorted([e for e in entities if e.strip() != ""])
    dim = pd.DataFrame({"entity": ent_sorted})
    dim["entity_key"] = range(1, len(dim) + 1)

    # Optional extra fields if present (e.g., currency exists in fact/kpi)
    for extra in ["currency", "base_currency", "country"]:
        col = extra if (not fact.empty and extra in fact.columns) else None
        if col:
            # naive mapping: first non-null per entity
            m = fact.dropna(subset=["entity", col]).groupby("entity")[col].first().to_dict()
            dim[extra] = dim["entity"].map(m)

    return dim[["entity_key", "entity"] + [c for c in dim.columns if c not in ["entity_key", "entity"]]]


def build_dim_account(dim_accounts: pd.DataFrame) -> pd.DataFrame:
    if dim_accounts.empty:
        return pd.DataFrame(columns=["account_key", "account_code", "account_name", "account_type"])

    code_col = _pick_col(dim_accounts, ["account_code", "code", "gl_account", "account"])
    name_col = _pick_col(dim_accounts, ["account_name", "name", "account"])
    type_col = _pick_col(dim_accounts, ["account_type", "type", "category"])

    out = dim_accounts.copy()
    if code_col != "account_code" and code_col is not None:
        out = out.rename(columns={code_col: "account_code"})
    if name_col and name_col != "account_name":
        out = out.rename(columns={name_col: "account_name"})
    if type_col and type_col != "account_type":
        out = out.rename(columns={type_col: "account_type"})

    # keep stable set + any extra descriptive cols
    base_cols = [c for c in ["account_code", "account_name", "account_type"] if c in out.columns]
    extra_cols = [c for c in out.columns if c not in base_cols]
    out = out[base_cols + extra_cols].copy()

    out["account_code"] = out["account_code"].astype(str)
    out = out.drop_duplicates(subset=["account_code"]).sort_values("account_code").reset_index(drop=True)
    out["account_key"] = range(1, len(out) + 1)

    # move key to front
    cols = ["account_key"] + [c for c in out.columns if c != "account_key"]
    return out[cols]


def build_dim_date(fact_m: pd.DataFrame, date_col: str) -> pd.DataFrame:
    dates = pd.to_datetime(fact_m[date_col], errors="coerce").dropna().dt.normalize().unique()
    if len(dates) == 0:
        return pd.DataFrame(columns=["date_key", "date", "year", "month", "month_name", "quarter", "week", "day"])

    d = pd.Series(sorted(dates), name="date")
    dim = pd.DataFrame({"date": d})
    dim["date_key"] = dim["date"].dt.strftime("%Y%m%d").astype(int)
    dim["year"] = dim["date"].dt.year
    dim["month"] = dim["date"].dt.month
    dim["month_name"] = dim["date"].dt.strftime("%b")
    dim["quarter"] = dim["date"].dt.quarter
    dim["week"] = dim["date"].dt.isocalendar().week.astype(int)
    dim["day"] = dim["date"].dt.day
    dim["month_key"] = dim["date"].dt.strftime("%Y%m").astype(int)
    dim["month_label"] = dim["date"].dt.strftime("%Y-%m")

    return dim[
        [
            "date_key",
            "date",
            "year",
            "quarter",
            "month_key",
            "month_label",
            "month",
            "month_name",
            "week",
            "day",
        ]
    ]


def build_dim_month(dim_date: pd.DataFrame) -> pd.DataFrame:
    if dim_date.empty:
        return pd.DataFrame(
            columns=["month_key", "month_label", "year", "quarter", "month", "month_name", "month_start_date_key"]
        )

    m = dim_date.groupby(
        ["month_key", "month_label", "year", "quarter", "month", "month_name"],
        as_index=False,
    ).agg(month_start_date_key=("date_key", "min"))

    return m.sort_values("month_key").reset_index(drop=True)


def build_fact_gl(
    fact_m: pd.DataFrame,
    dim_entity: pd.DataFrame,
    dim_account: pd.DataFrame,
    date_col: str | None,
) -> pd.DataFrame:
    if fact_m.empty:
        return pd.DataFrame(columns=["date_key", "month_key", "entity_key", "account_key", "amount"])

    entity_col = _pick_col(fact_m, ["entity", "company", "business_unit"])
    acct_col = _pick_col(fact_m, ["account_code", "gl_account", "account"])
    amt_col = _pick_col(fact_m, ["amount_base", "amount", "amount_tzs", "amount_usd"])
    debit_col = _pick_col(fact_m, ["debit"])
    credit_col = _pick_col(fact_m, ["credit"])

    out = fact_m.copy()

    # Normalize key cols
    if entity_col and entity_col != "entity":
        out = out.rename(columns={entity_col: "entity"})
    if acct_col and acct_col != "account_code":
        out = out.rename(columns={acct_col: "account_code"})

    # Amount handling
    if amt_col and amt_col in out.columns:
        out["amount"] = pd.to_numeric(out[amt_col], errors="coerce")
    else:
        # if only debit/credit exist
        if debit_col in out.columns and credit_col in out.columns:
            out["amount"] = pd.to_numeric(out[debit_col], errors="coerce").fillna(0) - pd.to_numeric(
                out[credit_col], errors="coerce"
            ).fillna(0)
        else:
            out["amount"] = pd.NA

    # Date keys
    if date_col and date_col in out.columns:
        dt = pd.to_datetime(out[date_col], errors="coerce")
        out["date_key"] = dt.map(_date_key)
        out["month_key"] = dt.dt.strftime("%Y%m").where(dt.notna()).astype("Int64")
    else:
        out["date_key"] = pd.NA
        out["month_key"] = pd.NA

    # Map entity/account keys (add strict=True for Ruff B905)
    ent_map = dict(
        zip(
            dim_entity["entity"].astype(str),
            dim_entity["entity_key"],
            strict=True,
        )
    )
    acc_map = dict(
        zip(
            dim_account["account_code"].astype(str),
            dim_account["account_key"],
            strict=True,
        )
    )

    out["entity"] = out.get("entity", pd.Series([None] * len(out))).astype(str)
    out["account_code"] = out.get("account_code", pd.Series([None] * len(out))).astype(str)
    out["entity_key"] = out["entity"].map(ent_map)
    out["account_key"] = out["account_code"].map(acc_map)

    # Keep useful passthrough columns (ids, references) if present
    passthrough: list[str] = []
    for c in [
        "transaction_id",
        "move_id",
        "journal_id",
        "journal_name",
        "reference",
        "description",
        "partner",
        "vendor",
        "customer",
        "source_system",
    ]:
        if c in out.columns:
            passthrough.append(c)

    cols = ["date_key", "month_key", "entity_key", "account_key", "amount"] + passthrough
    return out[cols].copy()


def build_fact_kpi_monthly(kpi: pd.DataFrame, dim_entity: pd.DataFrame, month: str) -> pd.DataFrame:
    if kpi.empty:
        return pd.DataFrame(
            columns=[
                "month_key",
                "entity_key",
                "Asset",
                "COGS",
                "Expense",
                "Revenue",
                "gross_profit",
                "operating_profit",
                "gross_margin_pct",
                "operating_margin_pct",
            ]
        )

    out = kpi.copy()
    if "month" in out.columns:
        out["month"] = out["month"].map(_to_month_str)
        out = out.loc[out["month"] == month].copy()

    # Map entity key (add strict=True for Ruff B905)
    ent_map = dict(
        zip(
            dim_entity["entity"].astype(str),
            dim_entity["entity_key"],
            strict=True,
        )
    )
    out["entity"] = out["entity"].astype(str)
    out["entity_key"] = out["entity"].map(ent_map)

    # Month key
    out["month_key"] = _month_key(month)

    # Margin %
    if "Revenue" in out.columns:
        rev = pd.to_numeric(out["Revenue"], errors="coerce")
        if "gross_profit" in out.columns:
            gp = pd.to_numeric(out["gross_profit"], errors="coerce")
            out["gross_margin_pct"] = (gp / rev) * 100
        if "operating_profit" in out.columns:
            op = pd.to_numeric(out["operating_profit"], errors="coerce")
            out["operating_margin_pct"] = (op / rev) * 100

    keep = ["month_key", "entity_key"] + [
        c
        for c in [
            "Asset",
            "COGS",
            "Expense",
            "Revenue",
            "gross_profit",
            "operating_profit",
            "gross_margin_pct",
            "operating_margin_pct",
        ]
        if c in out.columns
    ]
    return out[keep].copy()


# ---------- Main ----------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--curated-dir", default="data/curated")
    ap.add_argument("--month", default=None, help="YYYY-MM e.g. 2025-12 (optional; can infer from kpi_monthly)")
    ap.add_argument("--out-dir", default=None, help="Default: data/bi_star/<month>")
    args = ap.parse_args()

    curated = Path(args.curated_dir)
    fact = _read_parquet(curated / "fact_transactions.parquet")
    dim_accounts_src = _read_parquet(curated / "dim_accounts.parquet")
    kpi = _read_parquet(curated / "kpi_monthly.parquet")

    # Normalize KPI month
    if not kpi.empty and "month" in kpi.columns:
        kpi = kpi.copy()
        kpi["month"] = kpi["month"].map(_to_month_str)

    month = args.month or _infer_month(kpi)
    if not month:
        raise SystemExit("Could not infer month. Provide --month YYYY-MM (e.g., 2025-12).")

    out_dir = Path(args.out_dir) if args.out_dir else Path("data") / "bi_star" / month
    _ensure_dir(out_dir)

    # Identify fact date column for filtering + date dims
    date_col = _pick_col(fact, ["tx_date", "date", "transaction_date", "posting_date", "invoice_date"])
    fact_m = _filter_to_month_by_date(fact, date_col, month) if date_col else fact.copy()

    # Build dims
    dim_entity = build_dim_entity(fact_m, kpi)
    dim_account = build_dim_account(dim_accounts_src)

    if date_col and (not fact_m.empty) and date_col in fact_m.columns:
        dim_date = build_dim_date(fact_m, date_col)
        dim_month = build_dim_month(dim_date)
    else:
        dim_date = pd.DataFrame(
            columns=[
                "date_key",
                "date",
                "year",
                "quarter",
                "month_key",
                "month_label",
                "month",
                "month_name",
                "week",
                "day",
            ]
        )
        dim_month = pd.DataFrame(
            columns=["month_key", "month_label", "year", "quarter", "month", "month_name", "month_start_date_key"]
        )

    # Build facts
    fact_gl = build_fact_gl(fact_m, dim_entity, dim_account, date_col)
    fact_kpi = build_fact_kpi_monthly(kpi, dim_entity, month)

    # Write CSVs
    dim_date.to_csv(out_dir / "dim_date.csv", index=False)
    dim_month.to_csv(out_dir / "dim_month.csv", index=False)
    dim_entity.to_csv(out_dir / "dim_entity.csv", index=False)
    dim_account.to_csv(out_dir / "dim_account.csv", index=False)
    fact_gl.to_csv(out_dir / "fact_gl.csv", index=False)
    fact_kpi.to_csv(out_dir / "fact_kpi_monthly.csv", index=False)

    # Modeling notes (Power BI relationships)
    notes: list[str] = []
    notes.append(f"month={month}")
    notes.append("")
    notes.append("Suggested Power BI Relationships:")
    notes.append("  fact_gl[date_key] -> dim_date[date_key] (Many-to-1, single)")
    notes.append("  fact_gl[entity_key] -> dim_entity[entity_key] (Many-to-1, single)")
    notes.append("  fact_gl[account_key] -> dim_account[account_key] (Many-to-1, single)")
    notes.append("  fact_gl[month_key] -> dim_month[month_key] (Many-to-1, single) (optional)")
    notes.append("  fact_kpi_monthly[entity_key] -> dim_entity[entity_key] (Many-to-1, single)")
    notes.append("  fact_kpi_monthly[month_key] -> dim_month[month_key] (Many-to-1, single)")
    notes.append("")
    notes.append("Files:")
    files = [
        "dim_date.csv",
        "dim_month.csv",
        "dim_entity.csv",
        "dim_account.csv",
        "fact_gl.csv",
        "fact_kpi_monthly.csv",
    ]
    for f in files:
        notes.append(f"  - {f}")

    (out_dir / "POWERBI_MODEL_NOTES.txt").write_text("\n".join(notes), encoding="utf-8")

    print(str(out_dir.resolve()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
