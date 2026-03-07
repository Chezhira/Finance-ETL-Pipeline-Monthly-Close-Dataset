from pathlib import Path

from typer import Option, Typer

app = Typer(help="Finance ETL CLI", no_args_is_help=True, add_completion=False)


@app.command("run")
def run_cmd(
    month: str = Option(..., "--month", "-m", help="Target month, e.g., 2025-12"),
    fail_on: str = Option("ERROR", "--fail-on", help="DQ strictness: ERROR|WARN|NEVER"),
    raw_dir: str = Option("data/raw", "--raw-dir", help="Raw input directory"),
    curated_dir: str = Option("data/curated", "--curated-dir", help="Curated output directory"),
    reference_dir: str = Option("data/reference", "--reference-dir", help="Reference data directory"),
):
    """
    Run the monthly-close ETL pipeline.

    Example:
      finance-etl run --month 2025-12
      finance-etl run --month 2025-12 --fail-on WARN
    """
    from finance_etl.config import Settings
    from finance_etl.pipeline import run_month

    print(f"Running ETL for month={month}, fail_on={fail_on}")

    settings = Settings()
    outputs = run_month(
        settings=settings,
        month=month,
        raw_dir=Path(raw_dir),
        curated_dir=Path(curated_dir),
        reference_dir=Path(reference_dir),
        fail_on=fail_on,
    )

    print(f"  fact_transactions  -> {outputs['fact']}")
    print(f"  dim_accounts       -> {outputs['dim_accounts']}")
    print(f"  kpi_monthly        -> {outputs['kpi']}")
    print(f"  dq_exceptions      -> {outputs['dq_exceptions']}")
    print(f"  dq_summary         -> {outputs['dq_summary']}")
    print(f"\nETL complete. Run: python scripts/export_powerbi_star_schema.py --month {month}")


@app.command("version")
def version_cmd():
    """Show CLI version."""
    try:
        from importlib.metadata import version
        print("finance-etl", version("finance-etl"))
    except Exception:
        print("finance-etl 0.1.0")


if __name__ == "__main__":
    app()
