from __future__ import annotations

import argparse
from pathlib import Path

from finance_etl.sample_data import generate_synthetic_raw

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", default="2025-12")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out-dir", default="data/raw")
    args = parser.parse_args()

    generate_synthetic_raw(Path(args.out_dir), month=args.month, seed=args.seed)
    print(f"? Synthetic raw data generated in {args.out_dir}")

if __name__ == "__main__":
    main()
