"""Finance Monthly Close Dashboard — Acacia Group"""
from __future__ import annotations
import argparse
from datetime import datetime
from pathlib import Path
import pandas as pd

# ── Entity anonymisation map ──────────────────────────────────────────────────
ENTITY_MAP = {"TLM": "Entity A", "UPE": "Entity B"}
def ename(code: str) -> str:
    return ENTITY_MAP.get(str(code), str(code))

# ── Helpers ───────────────────────────────────────────────────────────────────
def _read_parquet(p: Path) -> pd.DataFrame:
    return pd.read_parquet(p) if p.exists() else pd.DataFrame()

def _read_csv(p: Path) -> pd.DataFrame:
    return pd.read_csv(p) if p.exists() else pd.DataFrame()

def fmt(n, decimals=0):
    if pd.isna(n): return "—"
    prefix = "-" if n < 0 else ""
    return f"{prefix}{abs(n):,.{decimals}f}"

def pct_color(v):
    if v >= 50: return "#4ade80"
    if v >= 20: return "#facc15"
    return "#f87171"

def val_color(v):
    return "#4ade80" if v >= 0 else "#f87171"

def minibar(v, max_v, color):
    pct = min(abs(v) / max_v * 100, 100) if max_v else 0
    return f'<div style="width:{pct:.1f}%;height:3px;background:{color};border-radius:2px;margin-top:3px"></div>'

# ── Main build ────────────────────────────────────────────────────────────────
def build(month: str, curated_dir: Path, out_dir: Path) -> Path:
    kpi  = _read_parquet(curated_dir / "kpi_monthly.parquet")
    fact = _read_parquet(curated_dir / "fact_transactions.parquet")
    dim  = _read_parquet(curated_dir / "dim_accounts.parquet")
    dq   = _read_csv(curated_dir / "dq_exceptions.csv")
    dq_s = _read_csv(curated_dir / "dq_summary.csv")

    kpi_m = kpi[kpi["month"] == month].copy() if (not kpi.empty and "month" in kpi.columns) else kpi.copy()

    if not fact.empty and not dim.empty:
        merged = fact.merge(dim[["account_code","account_name","account_type"]], on="account_code", how="left")
    else:
        merged = pd.DataFrame()

    # ── Group KPIs ────────────────────────────────────────────────────────────
    total_rev  = kpi_m["Revenue"].sum() if not kpi_m.empty else 0
    total_gp   = kpi_m["gross_profit"].sum() if not kpi_m.empty else 0
    total_op   = kpi_m["operating_profit"].sum() if not kpi_m.empty else 0
    total_cogs = kpi_m["COGS"].sum() if not kpi_m.empty else 0
    total_exp  = kpi_m["Expense"].sum() if not kpi_m.empty else 0
    gm_pct     = (total_gp / total_rev * 100) if total_rev else 0
    op_pct     = (total_op / total_rev * 100) if total_rev else 0
    txn_count  = len(fact) if not fact.empty else 0
    dq_issues  = len(dq) if not dq.empty else 0

    # ── Sparkline data ────────────────────────────────────────────────────────
    spark_data = "[]"
    if not merged.empty and "date" in merged.columns:
        daily = merged[merged["account_type"] == "Revenue"].copy()
        daily["date"] = pd.to_datetime(daily["date"])
        dr = daily.groupby("date")["amount_base"].sum().reset_index().sort_values("date")
        spark_data = str(list(zip(dr["date"].dt.strftime("%d").tolist(), dr["amount_base"].round(0).tolist())))

    # ── Waterfall data ────────────────────────────────────────────────────────
    wf_labels = ["Revenue", "COGS", "Gross Profit", "OpEx", "Op. Profit"]
    wf_values = [total_rev, total_cogs, total_gp, total_exp, total_op]
    # For canvas waterfall: [label, value, isTotal, base]
    wf_data = []
    running = 0
    for i, (lbl, val) in enumerate(zip(wf_labels, wf_values)):
        is_total = lbl in ("Gross Profit", "Op. Profit")
        base = 0 if is_total else running
        wf_data.append({"label": lbl, "value": val, "isTotal": is_total, "base": base})
        if not is_total:
            running += val
        else:
            running = val
    import json
    wf_json = json.dumps(wf_data)

    # ── Entity contribution ───────────────────────────────────────────────────
    entity_contrib = ""
    entity_donut_data = "[]"
    if not kpi_m.empty:
        rows = []
        colors = ["#a78bfa", "#60a5fa", "#4ade80", "#facc15", "#f87171"]
        for i, (_, r) in enumerate(kpi_m.iterrows()):
            pct = (r["Revenue"] / total_rev * 100) if total_rev else 0
            c = colors[i % len(colors)]
            rows.append({"name": ename(r["entity"]), "rev": r["Revenue"], "pct": pct, "color": c})
        entity_donut_data = json.dumps([{"label": x["name"], "value": x["rev"], "color": x["color"]} for x in rows])
        for x in rows:
            entity_contrib += f"""
            <div style="margin-bottom:14px">
              <div style="display:flex;justify-content:space-between;margin-bottom:4px">
                <span style="color:#94a3b8;font-size:11px">{x['name']}</span>
                <span style="color:#e2e8f0;font-size:11px;font-weight:600">{x['pct']:.1f}%&nbsp;&nbsp;<span style="color:#334155">{fmt(x['rev'])}</span></span>
              </div>
              {minibar(x['rev'], total_rev, x['color'])}
            </div>"""

    # ── Revenue / Expense breakdown ───────────────────────────────────────────
    rev_rows = ""
    exp_rows = ""
    if not merged.empty:
        for typ, container, color in [("Revenue", "rev_rows", "#4ade80"), ("Expense", "exp_rows", "#f87171")]:
            df = merged[merged["account_type"] == typ].groupby("account_name")["amount_base"].sum().reset_index()
            df["abs"] = df["amount_base"].abs()
            df = df.sort_values("abs", ascending=False).head(6)
            maxv = df["abs"].max() if not df.empty else 1
            html_rows = ""
            for _, r in df.iterrows():
                html_rows += f"""
                <div style="margin-bottom:14px">
                  <div style="display:flex;justify-content:space-between;margin-bottom:4px">
                    <span style="color:#94a3b8;font-size:11px">{r['account_name']}</span>
                    <span style="color:#e2e8f0;font-size:11px;font-weight:600">{fmt(r['abs'])}</span>
                  </div>
                  {minibar(r['abs'], maxv, color)}
                </div>"""
            if typ == "Revenue": rev_rows = html_rows
            else: exp_rows = html_rows

    # ── Entity P&L rows ───────────────────────────────────────────────────────
    entity_rows = ""
    if not kpi_m.empty:
        for _, r in kpi_m.iterrows():
            gm = (r["gross_profit"] / r["Revenue"] * 100) if r["Revenue"] else 0
            op = (r["operating_profit"] / r["Revenue"] * 100) if r["Revenue"] else 0
            entity_rows += f"""
            <tr>
              <td style="padding:11px 16px;color:#a78bfa;font-weight:700;font-family:'DM Mono',monospace">{ename(r['entity'])}</td>
              <td style="padding:11px 16px;text-align:right;color:#e2e8f0">{fmt(r['Revenue'])}</td>
              <td style="padding:11px 16px;text-align:right;color:#f87171">{fmt(r['COGS'])}</td>
              <td style="padding:11px 16px;text-align:right;color:{val_color(r['gross_profit'])}">{fmt(r['gross_profit'])}</td>
              <td style="padding:11px 16px;text-align:right;color:{pct_color(gm)}">{gm:.1f}%</td>
              <td style="padding:11px 16px;text-align:right;color:#f87171">{fmt(r['Expense'])}</td>
              <td style="padding:11px 16px;text-align:right;color:{val_color(r['operating_profit'])}">{fmt(r['operating_profit'])}</td>
              <td style="padding:11px 16px;text-align:right;color:{pct_color(op)}">{op:.1f}%</td>
            </tr>"""

    # ── MoM row (placeholder — real data when multi-month curated) ────────────
    mom_note = "Prior month data not yet available — MoM comparison activates when pipeline runs across multiple months."

    # ── Top 10 transactions ───────────────────────────────────────────────────
    top10_rows = ""
    if not merged.empty:
        top10 = merged.copy()
        top10["abs"] = top10["amount_base"].abs()
        top10 = top10.nlargest(10, "abs")[["date","entity","account_name","account_type","description","amount_base"]]
        src_colors = {"Revenue":"#4ade80","Expense":"#f87171","COGS":"#facc15","Asset":"#60a5fa","Liability":"#a78bfa"}
        for _, r in top10.iterrows():
            c = src_colors.get(str(r.get("account_type","")), "#94a3b8")
            date_str = pd.to_datetime(r["date"]).strftime("%d %b") if not pd.isna(r["date"]) else "—"
            desc = str(r.get("description",""))[:35]
            top10_rows += f"""
            <tr>
              <td style="padding:10px 16px;color:#475569;font-family:'DM Mono',monospace;font-size:10px">{date_str}</td>
              <td style="padding:10px 16px;color:#a78bfa;font-family:'DM Mono',monospace;font-size:11px">{ename(r['entity'])}</td>
              <td style="padding:10px 16px;color:#94a3b8;font-size:11px">{r['account_name']}</td>
              <td style="padding:10px 16px">
                <span style="padding:2px 7px;border-radius:3px;font-size:9px;font-weight:700;letter-spacing:0.5px;
                  background:{c}18;border:1px solid {c}44;color:{c}">{r['account_type']}</span>
              </td>
              <td style="padding:10px 16px;color:#64748b;font-size:10px">{desc}</td>
              <td style="padding:10px 16px;text-align:right;color:{val_color(r['amount_base'])};font-family:'DM Mono',monospace;font-size:11px;font-weight:600">{fmt(r['amount_base'])}</td>
            </tr>"""

    # ── DQ rows ───────────────────────────────────────────────────────────────
    dq_rows = ""
    if not dq_s.empty:
        for _, r in dq_s.iterrows():
            sc = "#4ade80" if str(r.get("status","")) == "PASS" else "#f87171"
            dq_rows += f"""
            <tr>
              <td style="padding:10px 16px;color:#94a3b8;font-family:'DM Mono',monospace;font-size:11px">{r.get('dataset','—')}</td>
              <td style="padding:10px 16px;text-align:center;color:#f87171;font-family:'DM Mono',monospace">{r.get('error_count',0)}</td>
              <td style="padding:10px 16px;text-align:center;color:#facc15;font-family:'DM Mono',monospace">{r.get('warn_count',0)}</td>
              <td style="padding:10px 16px;text-align:center">
                <span style="padding:2px 9px;border-radius:3px;font-size:9px;font-weight:700;letter-spacing:1px;
                  background:{sc}18;border:1px solid {sc}44;color:{sc}">{r.get('status','—')}</span>
              </td>
            </tr>"""

    # ────────────────────────────────────────────────────────────────────────
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1.0"/>
  <title>Finance Close — {month}</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Playfair+Display:wght@700;900&family=DM+Mono:wght@300;400;500&family=DM+Sans:wght@300;400;500;600&display=swap" rel="stylesheet">
  <style>
    *,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
    :root{{
      --bg:#060a0f;--surface:#0c1118;--surface2:#0f1520;--border:#1a2435;--border2:#1e2d40;
      --text:#e2e8f0;--muted:#334155;--sub:#475569;
      --accent:#a78bfa;--green:#4ade80;--red:#f87171;--yellow:#facc15;--blue:#60a5fa;
    }}
    body{{background:var(--bg);color:var(--text);font-family:'DM Sans',sans-serif;padding:36px 28px;min-height:100vh}}
    body::before{{content:'';position:fixed;inset:0;
      background-image:url("data:image/svg+xml,%3Csvg viewBox='0 0 512 512' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.85' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)' opacity='0.03'/%3E%3C/svg%3E");
      pointer-events:none;z-index:0}}
    .wrap{{max-width:1200px;margin:0 auto;position:relative;z-index:1}}

    /* Header */
    .hdr{{display:flex;justify-content:space-between;align-items:flex-end;flex-wrap:wrap;gap:16px;margin-bottom:32px}}
    .eyebrow{{font-size:9px;letter-spacing:3px;color:var(--muted);text-transform:uppercase;margin-bottom:6px;font-family:'DM Mono',monospace}}
    h1{{font-family:'Playfair Display',serif;font-size:38px;font-weight:900;line-height:1}}
    h1 span{{color:var(--accent)}}
    .hdr-meta{{font-family:'DM Mono',monospace;font-size:9px;color:var(--muted);text-align:right;line-height:2}}
    .hdr-meta b{{color:var(--sub)}}

    /* KPI grid */
    .kpi-grid{{display:grid;grid-template-columns:repeat(6,1fr);gap:10px;margin-bottom:20px}}
    @media(max-width:900px){{.kpi-grid{{grid-template-columns:repeat(3,1fr)}}}}
    @media(max-width:560px){{.kpi-grid{{grid-template-columns:repeat(2,1fr)}}}}
    .kc{{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:16px;
      animation:fadeUp .45s ease both}}
    .kc:nth-child(1){{animation-delay:.05s}}.kc:nth-child(2){{animation-delay:.10s}}
    .kc:nth-child(3){{animation-delay:.15s}}.kc:nth-child(4){{animation-delay:.20s}}
    .kc:nth-child(5){{animation-delay:.25s}}.kc:nth-child(6){{animation-delay:.30s}}
    .kc-label{{font-size:9px;letter-spacing:1.5px;text-transform:uppercase;color:var(--muted);font-family:'DM Mono',monospace;margin-bottom:8px}}
    .kc-value{{font-family:'Playfair Display',serif;font-size:22px;font-weight:700;line-height:1}}
    .kc-sub{{font-size:9px;color:var(--muted);margin-top:5px;font-family:'DM Mono',monospace}}

    /* Panels */
    .panel{{background:var(--surface);border:1px solid var(--border);border-radius:8px;overflow:hidden;animation:fadeUp .45s ease both;animation-delay:.3s}}
    .ph{{padding:13px 18px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center}}
    .pt{{font-size:10px;font-weight:600;letter-spacing:1.5px;text-transform:uppercase;color:var(--accent);font-family:'DM Mono',monospace}}
    .psub{{font-size:9px;color:var(--muted);font-family:'DM Mono',monospace}}
    .pb{{padding:18px}}

    /* Grids */
    .g2{{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px}}
    .g3{{display:grid;grid-template-columns:1.8fr 1fr 1fr;gap:14px;margin-bottom:14px}}
    .g4{{display:grid;grid-template-columns:2fr 1fr 1fr 1fr;gap:14px;margin-bottom:14px}}
    @media(max-width:800px){{.g2,.g3,.g4{{grid-template-columns:1fr}}}}

    /* Tables */
    table{{width:100%;border-collapse:collapse;font-size:12px}}
    thead tr{{border-bottom:1px solid var(--border)}}
    th{{padding:10px 16px;text-align:right;font-size:9px;font-weight:700;letter-spacing:1px;text-transform:uppercase;color:var(--muted);font-family:'DM Mono',monospace}}
    th:first-child{{text-align:left}}
    tbody tr{{border-bottom:1px solid #0b0f17;transition:background .15s}}
    tbody tr:hover{{background:#0f1622}}
    tbody tr:last-child{{border-bottom:none}}

    /* MoM banner */
    .mom-banner{{background:var(--surface2);border:1px solid var(--border2);border-radius:8px;
      padding:14px 20px;margin-bottom:14px;display:flex;align-items:center;gap:12px;
      font-size:11px;color:var(--sub);font-family:'DM Mono',monospace}}
    .mom-banner::before{{content:'◈';color:var(--accent);font-size:14px;flex-shrink:0}}

    /* Canvas */
    canvas{{display:block;width:100%}}

    @keyframes fadeUp{{from{{opacity:0;transform:translateY(8px)}}to{{opacity:1;transform:translateY(0)}}}}

    .footer{{font-family:'DM Mono',monospace;font-size:9px;color:var(--muted);letter-spacing:.5px;
      margin-top:32px;padding-top:16px;border-top:1px solid var(--border);
      display:flex;justify-content:space-between;flex-wrap:wrap;gap:8px}}
  </style>
</head>
<body>
<div class="wrap">

<!-- ── Header ─────────────────────────────────────────────────────────────── -->
<div class="hdr">
  <div>
    <div class="eyebrow">Acacia Group · Monthly Close Report</div>
    <h1>Finance <span>{month}</span></h1>
  </div>
  <div class="hdr-meta">
    <b>Generated</b> {datetime.now().strftime('%Y-%m-%d %H:%M')}<br/>
    <b>Transactions</b> {txn_count:,}<br/>
    <b>DQ Status</b> {'ALL PASS' if dq_issues == 0 else f'{dq_issues} ISSUES'}<br/>
    <b>Base Currency</b> USD
  </div>
</div>

<!-- ── KPI Cards ──────────────────────────────────────────────────────────── -->
<div class="kpi-grid">
  <div class="kc" style="border-top:2px solid var(--green)">
    <div class="kc-label">Total Revenue</div>
    <div class="kc-value" style="color:var(--green)">{fmt(total_rev)}</div>
    <div class="kc-sub">group consolidated</div>
  </div>
  <div class="kc" style="border-top:2px solid var(--accent)">
    <div class="kc-label">Gross Profit</div>
    <div class="kc-value" style="color:var(--accent)">{fmt(total_gp)}</div>
    <div class="kc-sub">after COGS</div>
  </div>
  <div class="kc" style="border-top:2px solid {pct_color(gm_pct)}">
    <div class="kc-label">Gross Margin</div>
    <div class="kc-value" style="color:{pct_color(gm_pct)}">{gm_pct:.1f}%</div>
    <div class="kc-sub">group blended</div>
  </div>
  <div class="kc" style="border-top:2px solid {val_color(total_op)}">
    <div class="kc-label">Operating Profit</div>
    <div class="kc-value" style="color:{val_color(total_op)}">{fmt(total_op)}</div>
    <div class="kc-sub">{op_pct:.1f}% margin</div>
  </div>
  <div class="kc" style="border-top:2px solid var(--red)">
    <div class="kc-label">Total OpEx</div>
    <div class="kc-value" style="color:var(--red)">{fmt(abs(total_exp))}</div>
    <div class="kc-sub">excl. COGS</div>
  </div>
  <div class="kc" style="border-top:2px solid {'var(--green)' if dq_issues==0 else 'var(--red)'}">
    <div class="kc-label">DQ Status</div>
    <div class="kc-value" style="color:{'var(--green)' if dq_issues==0 else 'var(--red)'}">{'PASS' if dq_issues==0 else 'FAIL'}</div>
    <div class="kc-sub">{dq_issues} issue{'s' if dq_issues!=1 else ''} flagged</div>
  </div>
</div>

<!-- ── MoM Banner ─────────────────────────────────────────────────────────── -->
<div class="mom-banner">
  {mom_note}
</div>

<!-- ── Row 1: Sparkline + Waterfall + Entity Contribution ─────────────────── -->
<div class="g3">
  <div class="panel">
    <div class="ph"><span class="pt">Daily Revenue</span><span class="psub">{month} · USD</span></div>
    <div class="pb" style="padding-bottom:14px">
      <canvas id="spark" height="88"></canvas>
      <div style="margin-top:8px;font-family:'DM Mono',monospace;font-size:9px;color:var(--muted)">
        All entities combined · day-by-day
      </div>
    </div>
  </div>
  <div class="panel">
    <div class="ph"><span class="pt">P&amp;L Waterfall</span><span class="psub">USD</span></div>
    <div class="pb" style="padding-bottom:14px">
      <canvas id="waterfall" height="160"></canvas>
    </div>
  </div>
  <div class="panel">
    <div class="ph"><span class="pt">Revenue by Entity</span><span class="psub">% of group</span></div>
    <div class="pb">
      <canvas id="donut" height="100" style="margin-bottom:12px"></canvas>
      {entity_contrib}
    </div>
  </div>
</div>

<!-- ── Row 2: Revenue mix + Expense breakdown ─────────────────────────────── -->
<div class="g2">
  <div class="panel">
    <div class="ph"><span class="pt">Revenue Mix</span><span class="psub">by account · USD</span></div>
    <div class="pb">{rev_rows or '<span style="color:var(--muted);font-size:11px">No data</span>'}</div>
  </div>
  <div class="panel">
    <div class="ph"><span class="pt">OpEx Breakdown</span><span class="psub">by account · USD</span></div>
    <div class="pb">{exp_rows or '<span style="color:var(--muted);font-size:11px">No data</span>'}</div>
  </div>
</div>

<!-- ── Entity P&L Table ───────────────────────────────────────────────────── -->
<div class="panel" style="margin-bottom:14px">
  <div class="ph">
    <span class="pt">Entity P&amp;L Summary</span>
    <span class="psub">All figures USD · {month}</span>
  </div>
  <div style="overflow-x:auto">
    <table>
      <thead><tr>
        <th>Entity</th><th>Revenue</th><th>COGS</th><th>Gross Profit</th>
        <th>GM %</th><th>OpEx</th><th>Op. Profit</th><th>OP %</th>
      </tr></thead>
      <tbody>
        {entity_rows}
        <tr style="border-top:2px solid var(--border);background:#0c1118">
          <td style="padding:11px 16px;color:var(--accent);font-weight:700;font-family:'DM Mono',monospace">GROUP TOTAL</td>
          <td style="padding:11px 16px;text-align:right;font-weight:700;color:var(--green)">{fmt(total_rev)}</td>
          <td style="padding:11px 16px;text-align:right;font-weight:700;color:var(--red)">{fmt(total_cogs)}</td>
          <td style="padding:11px 16px;text-align:right;font-weight:700;color:{val_color(total_gp)}">{fmt(total_gp)}</td>
          <td style="padding:11px 16px;text-align:right;font-weight:700;color:{pct_color(gm_pct)}">{gm_pct:.1f}%</td>
          <td style="padding:11px 16px;text-align:right;font-weight:700;color:var(--red)">{fmt(total_exp)}</td>
          <td style="padding:11px 16px;text-align:right;font-weight:700;color:{val_color(total_op)}">{fmt(total_op)}</td>
          <td style="padding:11px 16px;text-align:right;font-weight:700;color:{pct_color(op_pct)}">{op_pct:.1f}%</td>
        </tr>
      </tbody>
    </table>
  </div>
</div>

<!-- ── Top 10 Transactions ────────────────────────────────────────────────── -->
<div class="panel" style="margin-bottom:14px">
  <div class="ph">
    <span class="pt">Top 10 Transactions by Value</span>
    <span class="psub">{month}</span>
  </div>
  <div style="overflow-x:auto">
    <table>
      <thead><tr>
        <th>Date</th><th>Entity</th><th>Account</th><th>Type</th><th>Description</th><th>Amount (USD)</th>
      </tr></thead>
      <tbody>{top10_rows}</tbody>
    </table>
  </div>
</div>

<!-- ── DQ + Metadata ─────────────────────────────────────────────────────── -->
<div class="g2">
  <div class="panel">
    <div class="ph">
      <span class="pt">Data Quality Report</span>
      <span style="padding:2px 9px;border-radius:3px;font-size:9px;font-weight:700;letter-spacing:1px;
        background:{'#0a201580' if dq_issues==0 else '#200a0a80'};
        border:1px solid {'#4ade8066' if dq_issues==0 else '#f8717166'};
        color:{'#4ade80' if dq_issues==0 else '#f87171'}">
        {'ALL PASS' if dq_issues==0 else 'ISSUES'}
      </span>
    </div>
    <table>
      <thead><tr><th>Dataset</th><th style="text-align:center">Errors</th><th style="text-align:center">Warnings</th><th style="text-align:center">Status</th></tr></thead>
      <tbody>
        {dq_rows or '''<tr><td colspan="4" style="padding:20px 16px;text-align:center;color:#334155;
          font-size:11px;font-family:DM Mono,monospace">✓ All datasets passed quality checks</td></tr>'''}
      </tbody>
    </table>
  </div>
  <div class="panel">
    <div class="ph"><span class="pt">Pipeline Metadata</span></div>
    <div class="pb">
      <div style="display:grid;gap:0">
        {"".join(f'''<div style="display:flex;justify-content:space-between;padding:10px 0;
          border-bottom:1px solid var(--border)">
          <span style="font-size:11px;color:var(--muted)">{k}</span>
          <span style="font-size:11px;font-family:'DM Mono',monospace;color:var(--sub)">{v}</span>
        </div>''' for k,v in [
          ("Report month", month),
          ("Base currency", "USD"),
          ("Transactions", f"{txn_count:,}"),
          ("Entities", ", ".join(ename(e) for e in kpi_m["entity"].tolist()) if not kpi_m.empty else "—"),
          ("DQ exceptions", str(dq_issues)),
          ("Generated", datetime.now().strftime("%Y-%m-%d %H:%M")),
        ])}
      </div>
    </div>
  </div>
</div>

<!-- Footer -->
<div class="footer">
  <span>Finance ETL Pipeline · Acacia Group · {month}</span>
  <span>All data synthetic · github.com/Chezhira/Finance-ETL-Pipeline-Monthly-Close-Dataset</span>
</div>

</div><!-- /wrap -->

<script>
// ── Sparkline ────────────────────────────────────────────────────────────────
(function() {{
  const raw = {spark_data};
  const canvas = document.getElementById('spark');
  if (!canvas || !raw.length) return;
  const dpr = window.devicePixelRatio || 1;
  const W = canvas.parentElement.offsetWidth - 36, H = 88;
  canvas.width = W*dpr; canvas.height = H*dpr;
  canvas.style.width = W+'px'; canvas.style.height = H+'px';
  const ctx = canvas.getContext('2d'); ctx.scale(dpr, dpr);
  const vals = raw.map(r=>r[1]), labels = raw.map(r=>r[0]);
  const maxV = Math.max(...vals), minV = Math.min(0,...vals), range = maxV-minV||1;
  const pad = {{l:4,r:4,t:8,b:20}}, pW = W-pad.l-pad.r, pH = H-pad.t-pad.b;
  const step = pW/Math.max(raw.length-1,1);
  const pts = raw.map((r,i)=>{{
    return {{x:pad.l+i*step, y:pad.t+pH*(1-(r[1]-minV)/range), v:r[1], lbl:r[0]}};
  }});
  const g = ctx.createLinearGradient(0,pad.t,0,H-pad.b);
  g.addColorStop(0,'rgba(74,222,128,0.20)'); g.addColorStop(1,'rgba(74,222,128,0)');
  ctx.beginPath(); ctx.moveTo(pts[0].x,pts[0].y);
  pts.forEach(p=>ctx.lineTo(p.x,p.y));
  ctx.lineTo(pts[pts.length-1].x,H-pad.b); ctx.lineTo(pts[0].x,H-pad.b);
  ctx.closePath(); ctx.fillStyle=g; ctx.fill();
  ctx.beginPath(); ctx.moveTo(pts[0].x,pts[0].y);
  pts.forEach(p=>ctx.lineTo(p.x,p.y));
  ctx.strokeStyle='#4ade80'; ctx.lineWidth=1.8; ctx.stroke();
  pts.forEach(p=>{{
    ctx.beginPath(); ctx.arc(p.x,p.y,2,0,Math.PI*2);
    ctx.fillStyle='#4ade80'; ctx.fill();
  }});
  ctx.fillStyle='#334155'; ctx.font='8px DM Mono,monospace'; ctx.textAlign='center';
  pts.forEach((p,i)=>{{ if(i%5===0) ctx.fillText(p.lbl,p.x,H-4); }});
}})();

// ── Waterfall ────────────────────────────────────────────────────────────────
(function() {{
  const data = {wf_json};
  const canvas = document.getElementById('waterfall');
  if (!canvas || !data.length) return;
  const dpr = window.devicePixelRatio || 1;
  const W = canvas.parentElement.offsetWidth - 36, H = 160;
  canvas.width = W*dpr; canvas.height = H*dpr;
  canvas.style.width = W+'px'; canvas.style.height = H+'px';
  const ctx = canvas.getContext('2d'); ctx.scale(dpr,dpr);
  const pad = {{l:8,r:8,t:12,b:36}};
  const pW = W-pad.l-pad.r, pH = H-pad.t-pad.b;
  const allVals = data.map(d => d.isTotal ? d.value : d.base+d.value).concat(data.map(d=>d.base));
  const maxV = Math.max(...allVals.map(v=>Math.abs(v)))*1.15||1;
  const zeroY = pad.t + pH*0.5;
  function toY(v) {{ return pad.t + pH*(0.5 - v/(maxV*2)); }}
  const bw = pW/data.length*0.6, gap = pW/data.length;
  const COLORS = {{pos:'#4ade80',neg:'#f87171',total:'#a78bfa',zero:'#334155'}};
  // Zero line
  ctx.strokeStyle='#1a2435'; ctx.lineWidth=1;
  ctx.beginPath(); ctx.moveTo(pad.l,zeroY); ctx.lineTo(W-pad.r,zeroY); ctx.stroke();
  data.forEach((d,i) => {{
    const cx = pad.l + i*gap + gap*0.5;
    const x = cx - bw*0.5;
    const top = d.isTotal ? toY(d.value) : toY(d.base + (d.value>0?d.value:0));
    const bot = d.isTotal ? toY(0) : toY(d.base + (d.value<0?d.value:0));
    const barH = Math.abs(bot-top)||2;
    const color = d.isTotal ? COLORS.total : (d.value>=0 ? COLORS.pos : COLORS.neg);
    // Connector line
    if (i>0 && !d.isTotal) {{
      const prevD = data[i-1];
      const prevTop = prevD.isTotal ? toY(prevD.value) : toY(prevD.base+prevD.value);
      ctx.strokeStyle='#1e2d40'; ctx.lineWidth=1; ctx.setLineDash([2,3]);
      ctx.beginPath(); ctx.moveTo(x, prevTop); ctx.lineTo(x+bw, prevTop); ctx.stroke();
      ctx.setLineDash([]);
    }}
    ctx.fillStyle=color+'33'; ctx.fillRect(x, Math.min(top,bot), bw, barH);
    ctx.strokeStyle=color; ctx.lineWidth=1.5; ctx.strokeRect(x, Math.min(top,bot), bw, barH);
    // Value label
    const labelY = Math.min(top,bot) - 4;
    ctx.fillStyle = color; ctx.font='bold 8px DM Mono,monospace'; ctx.textAlign='center';
    const v = Math.abs(d.value);
    const lbl = v>=1000 ? (v/1000).toFixed(1)+'k' : v.toFixed(0);
    ctx.fillText((d.value<0&&!d.isTotal?'-':'')+lbl, cx, labelY<pad.t+8?Math.min(top,bot)+barH+10:labelY);
    // X label
    ctx.fillStyle='#334155'; ctx.font='8px DM Mono,monospace';
    const words = d.label.split(' ');
    words.forEach((w,wi) => ctx.fillText(w, cx, H-pad.b+12+wi*10));
  }});
}})();

// ── Donut ────────────────────────────────────────────────────────────────────
(function() {{
  const data = {entity_donut_data};
  const canvas = document.getElementById('donut');
  if (!canvas || !data.length) return;
  const dpr = window.devicePixelRatio || 1;
  const W = canvas.parentElement.offsetWidth - 36, H = 100;
  canvas.width = W*dpr; canvas.height = H*dpr;
  canvas.style.width = W+'px'; canvas.style.height = H+'px';
  const ctx = canvas.getContext('2d'); ctx.scale(dpr,dpr);
  const cx = W/2, cy = H/2, r = Math.min(W,H)*0.38, ir = r*0.6;
  const total = data.reduce((s,d)=>s+d.value,0);
  let angle = -Math.PI/2;
  data.forEach(d => {{
    const slice = (d.value/total)*Math.PI*2;
    ctx.beginPath(); ctx.moveTo(cx,cy);
    ctx.arc(cx,cy,r,angle,angle+slice);
    ctx.closePath(); ctx.fillStyle=d.color+'bb'; ctx.fill();
    ctx.strokeStyle=d.color; ctx.lineWidth=1.5; ctx.stroke();
    angle += slice;
  }});
  // Hole
  ctx.beginPath(); ctx.arc(cx,cy,ir,0,Math.PI*2);
  ctx.fillStyle='#0c1118'; ctx.fill();
  // Centre label
  ctx.fillStyle='#e2e8f0'; ctx.font='bold 10px Playfair Display,serif'; ctx.textAlign='center';
  ctx.fillText(data.length+' Entities', cx, cy+4);
}})();
</script>
</body>
</html>"""

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "dashboard.html"
    out_path.write_text(html, encoding="utf-8")
    print(str(out_path))
    return out_path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", default=None)
    ap.add_argument("--curated-dir", default="data/curated")
    ap.add_argument("--out-dir", default=None)
    args = ap.parse_args()
    curated = Path(args.curated_dir)
    month = args.month
    if not month:
        kpi = _read_parquet(curated / "kpi_monthly.parquet")
        if not kpi.empty and "month" in kpi.columns:
            month = sorted(kpi["month"].unique())[-1]
        else:
            raise SystemExit("Provide --month YYYY-MM")
    out_dir = Path(args.out_dir) if args.out_dir else Path("reports") / month
    build(month, curated, out_dir)

if __name__ == "__main__":
    main()
