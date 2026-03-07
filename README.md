# Finance ETL Pipeline — Monthly Close Dataset

<p align="left">
  <img src="https://img.shields.io/badge/Python-3.10%20|%203.11%20|%203.12-blue?logo=python&style=flat-square" alt="Python">
  <img src="https://img.shields.io/badge/License-MIT-green.svg?style=flat-square" alt="License">
  <img src="https://img.shields.io/badge/Lint-Ruff-4B8BBE?logo=python&style=flat-square" alt="Ruff">
  <img src="https://img.shields.io/badge/Format-Black-000000?style=flat-square" alt="Black">
  <img src="https://img.shields.io/badge/CI-GitHub%20Actions-2088FF?logo=githubactions&style=flat-square" alt="CI">
</p>

---

Every month, a finance team pulls extracts from the ERP — sales invoices, expense bills, payroll runs, inventory movements, FX rates — and spends days cleaning, reconciling, and rebuilding the same reports from scratch. This pipeline automates that entire process: raw CSVs in, validated Parquet curated layer out, star-schema BI-ready outputs, and an interactive HTML dashboard — all from a single CLI command.

Built to demonstrate production-grade data engineering applied to real finance problems.

---

## What it does

```
Raw CSVs (sales, expenses, payroll, inventory, FX)
    │
    ▼
Schema validation + Data quality checks
    │
    ▼
Curated Parquet layer  (fact_transactions · dim_accounts · kpi_monthly)
    │
    ▼
Star schema CSV export  (fact_gl · fact_kpi · dim_date · dim_entity · dim_account)
    │
    ▼
HTML Dashboard  (KPI cards · P&L waterfall · entity contribution · top transactions)
```

**Five datasets validated per run:** sales, expenses, payroll, COGS/inventory, FX rates — each with schema enforcement, null checks, currency whitelisting, and COA referential integrity. DQ exceptions written to audit CSV with ERROR/WARN severity grading.

---

## Quickstart

```bash
git clone https://github.com/Chezhira/Finance-ETL-Pipeline-Monthly-Close-Dataset.git
cd Finance-ETL-Pipeline-Monthly-Close-Dataset
pip install -e .

# 1. Generate synthetic data
python scripts/generate_synthetic_data.py --month 2025-12 --out-dir data/raw

# 2. Run the pipeline
finance-etl run --month 2025-12

# 3. Export star schema for Power BI / Tableau
python scripts/export_powerbi_star_schema.py --month 2025-12

# 4. Build the dashboard
python scripts/build_dashboard.py --month 2025-12
```

Dashboard renders to `reports/2025-12/dashboard.html` — open in any browser.

---

## CLI reference

```bash
finance-etl run --month 2025-12               # Standard run, fail on errors
finance-etl run --month 2025-12 --fail-on WARN  # Stricter — fail on warnings too
finance-etl run --month 2025-12 --fail-on NEVER # Run through all DQ issues
finance-etl version
```

---

## Outputs

| File | Description |
|------|-------------|
| `data/curated/fact_transactions.parquet` | All GL transactions, FX-converted to base currency |
| `data/curated/dim_accounts.parquet` | Chart of accounts dimension |
| `data/curated/kpi_monthly.parquet` | Revenue, COGS, Expense, Gross Profit, Operating Profit per entity |
| `data/curated/dq_exceptions.csv` | Row-level DQ exceptions with severity |
| `data/curated/dq_summary.csv` | Dataset-level DQ summary (PASS/FAIL per source) |
| `data/bi_star/YYYY-MM/fact_gl.csv` | Star schema GL fact table |
| `data/bi_star/YYYY-MM/fact_kpi_monthly.csv` | KPI fact with margin % columns |
| `data/bi_star/YYYY-MM/dim_*.csv` | Date, entity, account dimensions |
| `reports/YYYY-MM/dashboard.html` | Interactive HTML dashboard |

---

## Data Quality controls

- **Schema validation** — required columns, data types, non-null constraints per dataset
- **Currency whitelist** — rejects transactions in currencies outside allowed set
- **COA referential integrity** — flags account codes not present in chart of accounts
- **Severity grading** — key field violations → ERROR; non-critical → WARN
- **Audit trail** — full exception log written every run regardless of pass/fail
- **DQ summary** — one status row per dataset (sales · expenses · payroll · cogs_inventory · fx_rates)

---

## Project structure

```
├── src/finance_etl/
│   ├── pipeline.py       # Core ETL orchestration
│   ├── transform.py      # Fact table, KPI, FX conversion
│   ├── quality.py        # Schema definitions, DQ checks, severity
│   ├── io_utils.py       # CSV/Parquet read/write helpers
│   ├── config.py         # Settings (base currency, allowed currencies, paths)
│   └── cli.py            # Typer CLI entrypoint
├── scripts/
│   ├── generate_synthetic_data.py   # Synthetic raw data generator
│   ├── export_powerbi_star_schema.py
│   └── build_dashboard.py           # HTML dashboard builder
├── data/
│   ├── raw/              # Input CSVs (generated or real extracts)
│   ├── reference/        # chart_of_accounts.csv
│   ├── curated/          # Parquet outputs
│   └── bi_star/          # Star schema CSVs
├── reports/              # HTML dashboards
└── tests/
```

---

## Running tests

```bash
pip install -r requirements-dev.txt
pytest
```

CI runs on every push via GitHub Actions: lint (Ruff), format check (Black), tests (pytest), dependency audit (pip-audit).

---

## Disclaimer

All data in this repository is entirely synthetic and generated for demonstration purposes. No proprietary, confidential, or real business information is included. The tools were built independently to demonstrate finance data engineering patterns.

---

## Author

**Zahidah Murira** · Group Finance Lead · CMA · CGBA · CFA Level I  
[ziddmurira@gmail.com](mailto:ziddmurira@gmail.com) · [LinkedIn](https://linkedin.com/in/zahidahmurira) · [finance-automation-toolkit](https://github.com/Chezhira/finance-automation-toolkit)
