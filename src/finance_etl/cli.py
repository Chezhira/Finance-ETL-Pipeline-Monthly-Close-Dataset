from __future__ import annotations

from pathlib import Path
import pandas as pd
import typer
from rich import print as rprint

from .config import settings
from .pipeline import run_month

app = typer.Typer(help="Monthly Finance ETL + DQ checks + curated Parquet datasets")

@app.command("hello")
def hello(name: str = "Zahidah"):
    rprint(f"[bold green]Hello {name}![/bold green] CLI wiring works ?")

def _print_dq_summary(path: Path) -> None:
    if not path.exists():
        return
    try:
        df = pd.read_csv(path)
    except Exception:
        return

    # PASS file has different columns
    if "status" in df.columns:
        rprint(f"[bold green]DQ status:[/bold green] {df.loc[0,'status']}")
        return

    if df.empty:
        rprint("[bold green]DQ summary:[/bold green] no issues")
        return

    rprint("[bold yellow]DQ summary (issues):[/bold yellow]")
    for _, row in df.iterrows():
        rprint(f" - {row['dataset']} | {row['severity']} | {int(row['issue_count'])}")

@app.command("run")
def run(
    month: str = typer.Option(..., help="Month to process in YYYY-MM format, e.g. 2025-12"),
    raw_dir: Path = typer.Option(Path("data/raw"), help="Path to raw CSV folder"),
    curated_dir: Path = typer.Option(Path("data/curated"), help="Path to curated output folder"),
    reference_dir: Path = typer.Option(Path("data/reference"), help="Path to reference datasets"),
    fail_on: str = typer.Option("ERROR", help="DQ fail threshold: ERROR | WARN | NEVER"),
):
    outputs = run_month(settings, month, raw_dir, curated_dir, reference_dir, fail_on=fail_on)

    _print_dq_summary(outputs["dq_summary"])

    rprint("[bold green]ETL complete[/bold green]")
    for k, v in outputs.items():
        rprint(f" - {k}: {v}")

if __name__ == "__main__":
    app()
