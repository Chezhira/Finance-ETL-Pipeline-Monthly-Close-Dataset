from __future__ import annotations

import pandas as pd


def build_dim_accounts(chart_of_accounts: pd.DataFrame) -> pd.DataFrame:
    dim = chart_of_accounts.copy()
    dim["account_code"] = dim["account_code"].astype(str)
    return dim


def fx_to_base(fx_rates: pd.DataFrame, base_currency: str) -> pd.DataFrame:
    fx = fx_rates.copy()
    fx["date"] = pd.to_datetime(fx["date"]).dt.date
    fx = fx[fx["to_currency"] == base_currency].copy()
    return fx


def add_fx_amount_base(df: pd.DataFrame, fx: pd.DataFrame, base_currency: str) -> pd.DataFrame:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"])
    out["date_key"] = out["date"].dt.date
    out["rate"] = 1.0

    mask = out["currency"] != base_currency
    if mask.any():
        fx_lookup = fx[["date", "from_currency", "rate"]].copy()
        fx_lookup["date"] = pd.to_datetime(fx_lookup["date"]).dt.date

        out = out.merge(
            fx_lookup,
            how="left",
            left_on=["date_key", "currency"],
            right_on=["date", "from_currency"],
            suffixes=("", "_fx"),
        )
        out.loc[mask, "rate"] = out.loc[mask, "rate_fx"]
        out.drop(columns=[c for c in ["date_fx", "from_currency", "rate_fx"] if c in out.columns], inplace=True)

    if out["rate"].isna().any():
        missing = out[out["rate"].isna()][["date_key", "currency"]].drop_duplicates()
        raise ValueError(f"Missing FX rates for:\n{missing}")

    out["amount_base"] = (out["amount"] * out["rate"]).round(2)
    out.drop(columns=["date_key"], inplace=True)
    return out


def to_fact_transactions(
    sales: pd.DataFrame,
    expenses: pd.DataFrame,
    payroll: pd.DataFrame,
    inventory: pd.DataFrame,
    fx: pd.DataFrame,
    base_currency: str,
) -> pd.DataFrame:
    s = sales.copy()
    s["source"] = "sales"
    s["document_id"] = s["invoice_id"]
    s = s[["date", "entity", "source", "document_id", "account_code", "currency", "amount", "description"]]

    e = expenses.copy()
    e["source"] = "expenses"
    e["document_id"] = e["bill_id"]
    e = e[["date", "entity", "source", "document_id", "account_code", "currency", "amount", "description"]]
    e["amount"] = -e["amount"]

    p = payroll.copy()
    p["source"] = "payroll"
    p["date"] = pd.to_datetime(p["month"] + "-01") + pd.offsets.MonthEnd(0)
    p["document_id"] = p["employee_id"] + "_" + p["month"]
    p["account_code"] = "61000001"
    p["amount"] = -p["net"]
    p["description"] = "Payroll net"
    p = p[["date", "entity", "source", "document_id", "account_code", "currency", "amount", "description"]]

    inv = inventory.copy()
    inv["source"] = "inventory"
    inv["document_id"] = inv["sku"] + "_" + inv["date"].astype(str)
    inv["account_code"] = inv["movement_type"].map(
        {"issue": "50000001", "receipt": "10000001", "adjustment": "10000001"}
    )
    inv["amount"] = (inv["qty"] * inv["unit_cost"]).round(2)
    inv.loc[inv["movement_type"] == "issue", "amount"] = -inv.loc[inv["movement_type"] == "issue", "amount"]
    inv["description"] = inv["movement_type"] + " " + inv["sku"]
    inv = inv[["date", "entity", "source", "document_id", "account_code", "currency", "amount", "description"]]

    fact = pd.concat([s, e, p, inv], ignore_index=True)
    fact["account_code"] = fact["account_code"].astype(str)
    fact["currency"] = fact["currency"].astype(str)

    fact = add_fx_amount_base(fact, fx, base_currency)

    fact = fact.sort_values(["date", "entity", "source", "document_id"]).reset_index(drop=True)
    fact["txn_id"] = fact["entity"].astype(str) + "|" + fact["source"] + "|" + fact["document_id"].astype(str)

    cols = [
        "txn_id",
        "date",
        "entity",
        "source",
        "document_id",
        "account_code",
        "currency",
        "amount",
        "rate",
        "amount_base",
        "description",
    ]
    return fact[cols]


def kpi_monthly(fact: pd.DataFrame, dim_accounts: pd.DataFrame) -> pd.DataFrame:
    df = fact.merge(dim_accounts[["account_code", "account_type"]], on="account_code", how="left")
    df["month"] = pd.to_datetime(df["date"]).dt.to_period("M").astype(str)

    pivot = df.groupby(["entity", "month", "account_type"], dropna=False)["amount_base"].sum().reset_index()
    wide = pivot.pivot_table(
        index=["entity", "month"], columns="account_type", values="amount_base", fill_value=0
    ).reset_index()

    for col in ["Revenue", "COGS", "Expense"]:
        if col not in wide.columns:
            wide[col] = 0.0

    wide["gross_profit"] = (wide["Revenue"] + wide["COGS"]).round(2)
    wide["operating_profit"] = (wide["gross_profit"] + wide["Expense"]).round(2)
    return wide.sort_values(["entity", "month"]).reset_index(drop=True)
