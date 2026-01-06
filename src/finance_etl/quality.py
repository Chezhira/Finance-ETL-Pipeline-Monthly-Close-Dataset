from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
from pandera import Column, Check

def _dup_check(keys: list[str], label: str) -> Check:
    return Check(
        lambda df: df.groupby(keys).size().max() == 1,
        element_wise=False,
        error=f"Duplicates found for keys {keys} in {label}",
    )

def sales_schema(allowed_currencies: tuple[str, ...]) -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        {
            "date": Column(pa.DateTime, coerce=True, nullable=False),
            "entity": Column(str, nullable=False),
            "invoice_id": Column(str, nullable=False),
            "account_code": Column(str, nullable=False),
            "currency": Column(str, checks=Check.isin(list(allowed_currencies)), nullable=False),
            "amount": Column(float, checks=Check.gt(0), coerce=True, nullable=False),
            "description": Column(str, nullable=True),
        },
        checks=[_dup_check(["entity", "invoice_id"], "sales")],
        strict=True,
    )

def expenses_schema(allowed_currencies: tuple[str, ...]) -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        {
            "date": Column(pa.DateTime, coerce=True, nullable=False),
            "entity": Column(str, nullable=False),
            "bill_id": Column(str, nullable=False),
            "account_code": Column(str, nullable=False),
            "currency": Column(str, checks=Check.isin(list(allowed_currencies)), nullable=False),
            "amount": Column(float, checks=Check.gt(0), coerce=True, nullable=False),
            "description": Column(str, nullable=True),
        },
        checks=[_dup_check(["entity", "bill_id"], "expenses")],
        strict=True,
    )

def payroll_schema(allowed_currencies: tuple[str, ...]) -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        {
            "month": Column(str, nullable=False),
            "entity": Column(str, nullable=False),
            "employee_id": Column(str, nullable=False),
            "currency": Column(str, checks=Check.isin(list(allowed_currencies)), nullable=False),
            "gross": Column(float, checks=Check.ge(0), coerce=True, nullable=False),
            "deductions": Column(float, checks=Check.ge(0), coerce=True, nullable=False),
            "net": Column(float, checks=Check.ge(0), coerce=True, nullable=False),
        },
        checks=[
            Check(
                lambda df: (df["gross"] - df["deductions"] - df["net"]).abs().max() < 0.01,
                element_wise=False,
                error="Payroll identity gross - deductions = net violated",
            ),
        ],
        strict=True,
    )

def inventory_schema(allowed_currencies: tuple[str, ...]) -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        {
            "date": Column(pa.DateTime, coerce=True, nullable=False),
            "entity": Column(str, nullable=False),
            "sku": Column(str, nullable=False),
            "movement_type": Column(str, checks=Check.isin(["receipt", "issue", "adjustment"]), nullable=False),
            "qty": Column(float, checks=Check.ne(0), coerce=True, nullable=False),
            "unit_cost": Column(float, checks=Check.ge(0), coerce=True, nullable=False),
            "currency": Column(str, checks=Check.isin(list(allowed_currencies)), nullable=False),
        },
        strict=True,
    )

def fx_schema(allowed_currencies: tuple[str, ...], base_currency: str) -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        {
            "date": Column(pa.DateTime, coerce=True, nullable=False),
            "from_currency": Column(str, checks=Check.isin(list(allowed_currencies)), nullable=False),
            "to_currency": Column(str, checks=Check.isin([base_currency]), nullable=False),
            "rate": Column(float, checks=Check.gt(0), coerce=True, nullable=False),
        },
        checks=[_dup_check(["date", "from_currency", "to_currency"], "fx_rates")],
        strict=True,
    )

def validate_or_collect(
    df: pd.DataFrame,
    schema: pa.DataFrameSchema,
    dataset_name: str,
    issues: list[pd.DataFrame],
) -> pd.DataFrame | None:
    try:
        return schema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as e:
        fc = e.failure_cases.copy()
        if "index" not in fc.columns and "row" in fc.columns:
            fc["index"] = fc["row"]
        fc["dataset"] = dataset_name
        keep = [c for c in ["dataset", "index", "column", "check", "failure_case", "schema_context"] if c in fc.columns]
        rest = [c for c in fc.columns if c not in keep]
        fc = fc[keep + rest]
        issues.append(fc)
        return None
