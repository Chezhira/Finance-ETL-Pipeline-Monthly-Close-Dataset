from __future__ import annotations

from pathlib import Path
import shutil

import pandas as pd

from finance_etl.config import settings
from finance_etl.pipeline import run_month
from finance_etl.sample_data import generate_synthetic_raw

def test_pipeline_smoke(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    curated_dir = tmp_path / "curated"
    reference_dir = tmp_path / "reference"

    reference_dir.mkdir(parents=True, exist_ok=True)

    # copy reference COA from repo into temp reference_dir
    repo_root = Path(__file__).resolve().parents[1]
    shutil.copy(repo_root / "data" / "reference" / "chart_of_accounts.csv", reference_dir / "chart_of_accounts.csv")

    generate_synthetic_raw(raw_dir, month="2025-12", seed=42)

    outputs = run_month(
        settings=settings,
        month="2025-12",
        raw_dir=raw_dir,
        curated_dir=curated_dir,
        reference_dir=reference_dir,
        fail_on="ERROR",
    )

    # outputs exist
    assert Path(outputs["fact"]).exists()
    assert Path(outputs["dim_accounts"]).exists()
    assert Path(outputs["kpi"]).exists()
    assert Path(outputs["dq_summary"]).exists()

    # DQ PASS
    dq = pd.read_csv(outputs["dq_summary"])
    assert dq.loc[0, "status"] == "PASS"

    # KPI sanity
    kpi = pd.read_parquet(outputs["kpi"])
    assert "operating_profit" in kpi.columns
    assert len(kpi) > 0
