# Finance ETL Pipeline (Monthly Close Dataset)

[![CI](https://github.com/Chezhira/finance-etl-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Chezhira/finance-etl-pipeline/actions/workflows/ci.yml)

A portfolio-grade **data engineering mini-project** that turns messy monthly finance extracts into **validated, curated Parquet datasets** for dashboards and FP&A.

## Business problem

Monthly close data often comes from multiple sources (sales, expenses, payroll, FX, inventory) and usually arrives with:
- inconsistent formats (dates, currencies)
- silent type problems (IDs treated as numbers)
- duplicates / missing keys
- FX conversion inconsistencies

This creates **rework**, **wrong KPIs**, and stress during audit/tax season.

**This pipeline solves it** by enforcing data quality rules and producing a **single trusted dataset** each month.

## Outputs

After a run, the pipeline produces:

- `data/curated/fact_transactions.parquet` — unified transaction-level dataset (base currency)
- `data/curated/dim_accounts.parquet` — chart of accounts dimension
- `data/curated/kpi_monthly.parquet` — monthly KPIs for dashboards
- `data/curated/dq_exceptions.csv` — row-level DQ failures (audit trail)
- `data/curated/dq_summary.csv` — PASS/FAIL summary (controls)

## How to run (Anaconda / Miniconda)

Activate your env:

```bash
conda activate finance_etl
Generate sample raw data:

python scripts/generate_synthetic_data.py --month 2025-12 --out-dir data/raw


Run the pipeline:

finance-etl run --month 2025-12 --fail-on ERROR


Run tests:

pytest -q

Data quality controls

--fail-on controls when the pipeline stops:

ERROR (default): fail only if critical issues exist

WARN: fail if any issues exist

NEVER: always produce outputs, but write DQ reports

Example:

finance-etl run --month 2025-12 --fail-on WARN

Notes

This repo uses synthetic finance-like data for safety and portability.
To adapt to real exports, replace data/raw/*.csv with your extracts and keep the schemas/rules.

## Example KPI output

Sample rows from data/curated/kpi_monthly.parquet:

| entity | month   | Asset   | COGS      | Expense    | Revenue   | gross_profit | operating_profit |
|--------|---------|--------:|----------:|-----------:|----------:|------------:|-----------------:|
| TLM    | 2025-12 |  4771.96 | -15648.55 | -38682.57  |  48129.36 |    32480.81 |         -6201.76 |
| UPE    | 2025-12 | 12717.67 | -17281.12 | -31250.48  |  30050.52 |    12769.40 |        -18481.08 |

