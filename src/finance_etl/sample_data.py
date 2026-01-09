from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd

def generate_synthetic_raw(out_dir: Path, month: str = "2025-12", seed: int = 42) -> None:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    np.random.seed(seed)

    entities = ["TLM", "UPE"]
    currencies = ["USD", "TZS", "EUR"]

    start = pd.to_datetime(f"{month}-01")
    end = (start + pd.offsets.MonthBegin(1)) - pd.Timedelta(days=1)
    dates = pd.date_range(start, end, freq="D")

    # FX rates (to USD base)
    fx_rows: list[list[object]] = []
    for d in dates:
        fx_rows.append([d.date(), "USD", "USD", 1.0])
        fx_rows.append([d.date(), "EUR", "USD", float(np.random.uniform(1.05, 1.15))])
        fx_rows.append([d.date(), "TZS", "USD", float(np.random.uniform(0.00038, 0.00045))])
    fx = pd.DataFrame(fx_rows, columns=["date", "from_currency", "to_currency", "rate"])
    fx.to_csv(out_dir / "fx_rates.csv", index=False)

    # Sales
    sales_rows: list[list[object]] = []
    for entity in entities:
        n = int(np.random.randint(20, 40))
        for i in range(n):
            d = np.random.choice(dates)
            ccy = np.random.choice(currencies, p=[0.5, 0.4, 0.1])
            amt = float(np.random.uniform(200, 5000))
            account_code = np.random.choice(["40000001", "40000002"], p=[0.7, 0.3])
            sales_rows.append([d, entity, f"INV-{entity}-{i:04d}", str(account_code), ccy, amt, "Synthetic sale"])
    sales = pd.DataFrame(
        sales_rows,
        columns=["date", "entity", "invoice_id", "account_code", "currency", "amount", "description"],
    )
    sales.to_csv(out_dir / "sales.csv", index=False)

    # Expenses
    exp_rows: list[list[object]] = []
    expense_accounts = ["62000001", "63000001", "64000001"]
    for entity in entities:
        n = int(np.random.randint(25, 55))
        for i in range(n):
            d = np.random.choice(dates)
            ccy = np.random.choice(currencies, p=[0.5, 0.4, 0.1])
            amt = float(np.random.uniform(50, 2500))
            account_code = np.random.choice(expense_accounts)
            exp_rows.append([d, entity, f"BILL-{entity}-{i:04d}", str(account_code), ccy, amt, "Synthetic expense"])
    expenses = pd.DataFrame(
        exp_rows,
        columns=["date", "entity", "bill_id", "account_code", "currency", "amount", "description"],
    )
    expenses.to_csv(out_dir / "expenses.csv", index=False)

    # Payroll (monthly)
    pr_rows: list[list[object]] = []
    for entity in entities:
        for i in range(10):
            ccy = np.random.choice(["USD", "TZS"], p=[0.4, 0.6])
            gross = float(np.random.uniform(300, 1500))
            deductions = float(np.random.uniform(0, 150))
            net = float(round(gross - deductions, 2))
            pr_rows.append([month, entity, f"EMP-{entity}-{i:03d}", ccy, gross, deductions, net])
    payroll = pd.DataFrame(
        pr_rows,
        columns=["month", "entity", "employee_id", "currency", "gross", "deductions", "net"],
    )
    payroll.to_csv(out_dir / "payroll.csv", index=False)

    # Inventory movements
    inv_rows: list[list[object]] = []
    skus = ["HONEY-DRUM", "WAX-BLOCK", "GIN-750ML"]
    for entity in entities:
        n = int(np.random.randint(20, 40))
        for _ in range(n):
            d = np.random.choice(dates)
            sku = np.random.choice(skus)
            move = np.random.choice(["receipt", "issue", "adjustment"], p=[0.45, 0.45, 0.10])
            qty = float(np.random.uniform(1, 50))
            unit_cost = float(np.random.uniform(2, 80))
            ccy = np.random.choice(currencies, p=[0.5, 0.4, 0.1])
            inv_rows.append([d, entity, sku, move, qty, unit_cost, ccy])
    inv = pd.DataFrame(
        inv_rows,
        columns=["date", "entity", "sku", "movement_type", "qty", "unit_cost", "currency"],
    )
    inv.to_csv(out_dir / "inventory_movements.csv", index=False)
