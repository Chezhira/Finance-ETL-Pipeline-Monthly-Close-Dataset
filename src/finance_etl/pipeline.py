from __future__ import annotations

from pathlib import Path
import pandas as pd

from .config import Settings
from .io_utils import read_csv, write_parquet, write_csv
from .quality import (
    validate_or_collect,
    sales_schema,
    expenses_schema,
    payroll_schema,
    inventory_schema,
    fx_schema,
)
from .transform import build_dim_accounts, fx_to_base, to_fact_transactions, kpi_monthly

def _month_window(month: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    start = pd.to_datetime(f"{month}-01")
    end = start + pd.offsets.MonthBegin(1)
    return start, end

def _tag_severity(dq: pd.DataFrame) -> pd.DataFrame:
    """
    Simple severity rules:
    - dtype/required/dup/payroll identity are ERROR
    - everything else WARN (extend later)
    """
    out = dq.copy()
    out["severity"] = "WARN"

    # Common "critical" patterns
    critical_checks = [
        "dtype",
        "required",
        "Duplicates found",
        "Payroll identity",
    ]
    patt = "|".join(critical_checks)
    # check column sometimes stores strings like "dtype('str')" or full error text
    out.loc[out["check"].astype(str).str.contains(patt, case=False, na=False), "severity"] = "ERROR"
    return out

def run_month(
    settings: Settings,
    month: str,
    raw_dir: Path,
    curated_dir: Path,
    reference_dir: Path,
    fail_on: str = "ERROR",   # ERROR | WARN | NEVER
) -> dict[str, Path]:
    raw_dir = Path(raw_dir)
    curated_dir = Path(curated_dir)
    reference_dir = Path(reference_dir)
    curated_dir.mkdir(parents=True, exist_ok=True)

    # Load reference (force codes to strings)
    coa = read_csv(
        reference_dir / "chart_of_accounts.csv",
        dtype={"account_code": str, "account_name": str, "account_type": str},
    )
    dim_accounts = build_dim_accounts(coa)

    # Load raw (force IDs/codes to strings)
    sales = read_csv(
        raw_dir / "sales.csv",
        dtype={"entity": str, "invoice_id": str, "account_code": str, "currency": str},
        parse_dates=["date"],
    )
    expenses = read_csv(
        raw_dir / "expenses.csv",
        dtype={"entity": str, "bill_id": str, "account_code": str, "currency": str},
        parse_dates=["date"],
    )
    payroll = read_csv(
        raw_dir / "payroll.csv",
        dtype={"month": str, "entity": str, "employee_id": str, "currency": str},
    )
    inventory = read_csv(
        raw_dir / "inventory_movements.csv",
        dtype={"entity": str, "sku": str, "movement_type": str, "currency": str},
        parse_dates=["date"],
    )
    fx_rates = read_csv(
        raw_dir / "fx_rates.csv",
        dtype={"from_currency": str, "to_currency": str},
        parse_dates=["date"],
    )

    # Validate raw + collect DQ issues
    issues: list[pd.DataFrame] = []

    v_sales = validate_or_collect(sales, sales_schema(settings.allowed_currencies), "sales", issues)
    v_exp = validate_or_collect(expenses, expenses_schema(settings.allowed_currencies), "expenses", issues)
    v_pay = validate_or_collect(payroll, payroll_schema(settings.allowed_currencies), "payroll", issues)
    v_inv = validate_or_collect(inventory, inventory_schema(settings.allowed_currencies), "inventory_movements", issues)
    v_fx = validate_or_collect(fx_rates, fx_schema(settings.allowed_currencies, settings.base_currency), "fx_rates", issues)

    dq_exceptions_path = curated_dir / "dq_exceptions.csv"
    dq_summary_path = curated_dir / "dq_summary.csv"

    if issues:
        dq_exceptions = pd.concat(issues, ignore_index=True)
        dq_exceptions = _tag_severity(dq_exceptions)
        write_csv(dq_exceptions, dq_exceptions_path)

        summary = (
            dq_exceptions.groupby(["dataset", "severity"])
            .size()
            .reset_index(name="issue_count")
            .sort_values(["severity", "issue_count"], ascending=[True, False])
        )
        write_csv(summary, dq_summary_path)

        fail_on = fail_on.upper().strip()
        if fail_on not in {"ERROR", "WARN", "NEVER"}:
            raise ValueError("fail_on must be one of: ERROR, WARN, NEVER")

        if fail_on == "WARN":
            raise ValueError(f"DQ failed (WARN+ERROR). See {dq_exceptions_path} and {dq_summary_path}")
        if fail_on == "ERROR" and (dq_exceptions["severity"] == "ERROR").any():
            raise ValueError(f"DQ failed (ERROR). See {dq_exceptions_path} and {dq_summary_path}")

        # else: NEVER OR only WARNs and fail_on=ERROR -> continue

    else:
        # If all passed, write empty reports (audit trail)
        write_csv(
            pd.DataFrame(columns=["dataset", "index", "column", "check", "failure_case", "schema_context", "severity"]),
            dq_exceptions_path,
        )
        write_csv(pd.DataFrame([{"status": "PASS", "month": month}]), dq_summary_path)

    # Filter to month window
    start, end = _month_window(month)

    v_sales = v_sales[(v_sales["date"] >= start) & (v_sales["date"] < end)].copy()
    v_exp = v_exp[(v_exp["date"] >= start) & (v_exp["date"] < end)].copy()
    v_inv = v_inv[(v_inv["date"] >= start) & (v_inv["date"] < end)].copy()
    v_pay = v_pay[v_pay["month"] == month].copy()

    fx = fx_to_base(v_fx, settings.base_currency)

    fact = to_fact_transactions(v_sales, v_exp, v_pay, v_inv, fx, settings.base_currency)
    kpi = kpi_monthly(fact, dim_accounts)

    out_fact = curated_dir / "fact_transactions.parquet"
    out_dim = curated_dir / "dim_accounts.parquet"
    out_kpi = curated_dir / "kpi_monthly.parquet"

    write_parquet(fact, out_fact)
    write_parquet(dim_accounts, out_dim)
    write_parquet(kpi, out_kpi)

    return {
        "dq_exceptions": dq_exceptions_path,
        "dq_summary": dq_summary_path,
        "fact": out_fact,
        "dim_accounts": out_dim,
        "kpi": out_kpi,
    }
