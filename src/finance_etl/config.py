from __future__ import annotations

from pathlib import Path
from pydantic import BaseModel

class Settings(BaseModel):
    base_currency: str = "USD"
    allowed_currencies: tuple[str, ...] = ("USD", "TZS", "EUR")
    raw_dir: Path = Path("data/raw")
    curated_dir: Path = Path("data/curated")
    reference_dir: Path = Path("data/reference")

settings = Settings()
