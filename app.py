# app.py
# Requirements: pandas>=2.2, streamlit>=1.36, openpyxl>=3.1.2
# Optional charts: plotly>=5
import argparse
import os
import re
import sqlite3
import sys
from datetime import datetime
from typing import List, Tuple, Optional

import pandas as pd
import streamlit as st
from pandas.tseries.offsets import MonthEnd, DateOffset# app.py
# Requirements: pandas>=2.2, streamlit>=1.36, openpyxl>=3.1.2
# Optional charts: plotly>=5
import argparse
import os
import re
import sqlite3
import sys
from datetime import datetime
from typing import List, Tuple, Optional

import pandas as pd
import streamlit as st
from pandas.tseries.offsets import MonthEnd, DateOffset

FINANCE_DB = "finance.db"
COMMENTARY_DB = "commentary.db"

# Margin hints & units
MARGIN_HINTS = {"GM%", "EBITDA%", "Opex%"}
MARGIN_UNITS = {"%", "pts", "pt"}

# Priority rules (broadened P2)
PRIORITY_RULES = [
    # P1 — topline / profit / cash
    (re.compile(r'^(revenue|sales|net\s*revenue)$', re.I), "P1"),
    (re.compile(r'^(gross\s*margin(\s*%)?|gm%?|gm$)', re.I), "P1"),
    (re.compile(r'^ebitda(\s*%)?$', re.I), "P1"),
    (re.compile(r'^\s*dso\s*$', re.I), "P1"),
    (re.compile(r'^(capex|capital\s*exp)', re.I), "P1"),

    # P2 — efficiency & spend discipline
    (re.compile(r'^(opex|op\s*ex|operating\s*exp(ense|s)?)(\s*%|\s*ratio|\s*/\s*rev(enue)?)?$', re.I), "P2"),
    (re.compile(r'^(sg&?a|sga|selling\s*&\s*general\s*&\s*admin(istration)?)', re.I), "P2"),
    (re.compile(r'^(dpo|days\s*payable)', re.I), "P2"),
    (re.compile(r'^(dio|days\s*inventory)', re.I), "P2"),
    (re.compile(r'^(ccc|cash\s*conversion)', re.I), "P2"),
    (re.compile(r'^(bookings|billings|arr|mrr)$', re.I), "P2"),
]

def priority_of(metric: str) -> str:
    name = (metric or "").strip()
    for pat, p in PRIORITY_RULES:
        if pat.search(name):
            return p
    return "P3"

# Colors
DARK_GREEN = "#006400"
RED = "#C00000"
GREY = "#9CA3AF"

# ----------------------------
# DB init & schema evolve
# ----------------------------
def init_dbs() -> None:
    with sqlite3.connect(FINANCE_DB) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS financials (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market TEXT,
                month_end TEXT,
                metric TEXT,
                unit TEXT,
                scope TEXT,
                basis TEXT,
                value REAL
            )
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_fin
            ON financials(market, month_end, metric, unit, scope, basis)
            """
        )
    with sqlite3.connect(COMMENTARY_DB) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS commentary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market TEXT,
                month_end TEXT,
                metric TEXT,
                text TEXT,
                author TEXT,
                validation_score REAL,
                passed INTEGER,
                accepted INTEGER DEFAULT 0
            )
            """
        )
        cols = pd.read_sql("PRAGMA table_info(commentary);", conn)
        if "accepted" not in cols["name"].tolist():
            conn.execute("ALTER TABLE commentary ADD COLUMN accepted INTEGER DEFAULT 0")

# ----------------------------
# Helpers
# ----------------------------
def _true_month_end(date_obj: datetime) -> str:
    return (date_obj + MonthEnd(0)).strftime("%Y-%m-%d")

def parse_filename(filename: str) -> Tuple[str, str]:
    """
    Expect '<MARKET>_<MonYYYY>.xlsx', e.g., 'SG_Aug2025.xlsx'.
    Returns (market, month_end).
    """
    base = os.path.splitext(os.path.basename(filename))[0]
    m = re.match(r"^(?P<market>[A-Za-z]+)_(?P<mon>[A-Za-z]{3}\d{4})$", base)
    if not m:
        raise ValueError("Filename must be '<MARKET>_<MonYYYY>.xlsx' (e.g., 'SG_Aug2025.xlsx').")
    market = m.group("market").upper()
    mon = datetime.strptime(m.group("mon").title(), "%b%Y")
    return market, _true_month_end(mon)

def _read_excel_or_fail(io_obj):
    try:
        return pd.read_excel(io_obj, engine="openpyxl")
    except ImportError as e:
        raise RuntimeError("Excel engine missing. Install it: pip install openpyxl") from e

def _strip_nullable(s: pd.Series) -> pd.Series:
    return s.apply(lambda v: v.strip() if isinstance(v, str) else v)

def _fmt_value(val, unit):
    if pd.isna(val) or val is None:
        return "-"
    if isinstance(unit, str) and unit.strip():
        u = unit.strip()
        if u == "%":
            return f"{val:.2f}{u}"
        return f"{val:.2f} {u}"
    return f"{val:.2f}"

def _fmt_delta(val: float, unit) -> str:
    if isinstance(unit, str) and unit.strip() in ("%", "pts", "pt"):
        return f"{val:.2f} pp"
    if isinstance(unit, str) and unit.strip():
        return f"{val:.2f} {unit.strip()}"
    return f"{val:.2f}"

def _is_margin(metric: str, unit: Optional[str]) -> bool:
    if metric in MARGIN_HINTS:
        return True
    if isinstance(unit, str) and unit.strip() in MARGIN_UNITS:
        return True
    return False

# Business-sense coloring (DSO lower is better)
def is_better_when_lower(metric: str) -> bool:
    return bool(re.fullmatch(r'\s*dso\s*', (metric or ''), flags=re.I))

def color_for_delta(metric: str, delta: Optional[float]) -> str:
    if delta is None or pd.isna(delta):
        return GREY
    d = float(delta)
    if is_better_when_lower(metric):
        return DARK_GREEN if d < 0 else RED if d > 0 else GREY
    else:
        return DARK_GREEN if d > 0 else RED if d < 0 else GREY

# ----------------------------
# Excel ingestion
# ----------------------------
def _allowed_map(cols: List[str]) -> dict:
    """Map column -> (scope,basis); ignore prior-month and derived columns."""
    out = {}
    pat_vs = re.compile(r"^vs\s+(n-1|fct|bud)\s+\((ITM|YTD)\)$", re.IGNORECASE)
    for c in cols:
        c0 = str(c).strip()
        scope = basis = None
        if c0 == "m":
            scope, basis = "ITM", "Act"
        elif c0 == "Act (YTD)":
            scope, basis = "YTD", "Act"
        else:
            m = pat_vs.match(c0)
            if m:
                basis_map = {"n-1": "vs n-1", "fct": "vs Fct", "bud": "vs BUD"}
                basis = basis_map[m.group(1).lower()]
                scope = m.group(2).upper()
        out[c0] = (scope, basis)
    return out

def _tidy_from_dataframe(df: pd.DataFrame, market: str, month_end: str) -> pd.DataFrame:
    df = df.copy()
    df.rename(columns={df.columns[0]: "Metric", df.columns[1]: "Unit"}, inplace=True)
    allowed = _allowed_map(df.columns.tolist())
    keep_cols = [c for c, (s, b) in allowed.items() if s is not None]
    base_cols = ["Metric", "Unit"] + keep_cols

    m = df[base_cols].melt(id_vars=["Metric", "Unit"], var_name="column", value_name="value")
    m["scope"] = m["column"].map(lambda c: allowed[c][0])
    m["basis"] = m["column"].map(lambda c: allowed[c][1])
    m["market"] = market
    m["month_end"] = month_end
    m.rename(columns={"Metric": "metric", "Unit": "unit"}, inplace=True)

    m["value"] = pd.to_numeric(m["value"], errors="coerce")
    m = m.dropna(subset=["value"])
    m["metric"] = _strip_nullable(m["metric"])
    m["unit"] = _strip_nullable(m["unit"])
    m["scope"] = m["scope"].apply(lambda v: v.upper() if isinstance(v, str) else v)
    m["basis"] = _strip_nullable(m["basis"])

    key_cols = ["market", "month_end", "metric", "unit", "scope", "basis"]
    m = (
        m.sort_values(key_cols + ["value"], na_position="last")
         .drop_duplicates(subset=key_cols, keep="last")
    )
    return m[key_cols + ["value"]]

def ingest_excel_path(path: str) -> pd.DataFrame:
    market, month_end = parse_filename(path)
    df_raw = _read_excel_or_fail(path)
    return _tidy_from_dataframe(df_raw, market, month_end)

def ingest_excel_upload(file_obj, original_name: str) -> pd.DataFrame:
    market, month_end = parse_filename(original_name)
    df_raw = _read_excel_or_fail(file_obj)
    return _tidy_from_dataframe(df_raw, market, month_end)

# ----------------------------
# Storage
# ----------------------------
def store_financials(df: pd.DataFrame) -> None:
    key_cols = ["market", "month_end", "metric", "unit", "scope", "basis"]
    df2 = (
        df.sort_values(key_cols + ["value"])
          .drop_duplicates(subset=key_cols, keep="last")
    )
    # Replace that market+month completely to avoid UNIQUE issues
    pairs = df2[["market", "month_end"]].drop_duplicates().itertuples(index=False, name=None)
    with sqlite3.connect(FINANCE_DB) as conn:
        cur = conn.cursor()
        for market, month_end in pairs:
            cur.execute("DELETE FROM financials WHERE market = ? AND month_end = ?", (market, month_end))
        conn.commit()
        df2.to_sql("financials", conn, if_exists="append", index=False)

# ----------------------------
# Data APIs
# ----------------------------
def get_markets() -> List[str]:
    with sqlite3.connect(FINANCE_DB) as conn:
        df = pd.read_sql("SELECT DISTINCT market FROM financials ORDER BY market", conn)
    return df["market"].dropna().tolist()

def get_months_for_markets(markets: List[str]) -> List[str]:
    if not markets:
        return []
    ph = ",".join(["?"] * len(markets))
    with sqlite3.connect(FINANCE_DB) as conn:
        df = pd.read_sql(
            f"SELECT DISTINCT month_end FROM financials WHERE market IN ({ph}) ORDER BY month_end",
            conn, params=markets
        )
    return df["month_end"].dropna().tolist()

def get_slice(market: str, month_end: str) -> pd.DataFrame:
    with sqlite3.connect(FINANCE_DB) as conn:
        df = pd.read_sql(
            """
            SELECT market, month_end, metric, unit, scope, basis, value
            FROM financials
            WHERE market = ? AND month_end = ?
            """,
            conn, params=(market, month_end)
        )
    return df

def get_slice_multi(markets: List[str], month_end: str) -> pd.DataFrame:
    if not markets:
        return pd.DataFrame(columns=["market","month_end","metric","unit","scope","basis","value"])
    ph = ",".join(["?"] * len(markets))
    with sqlite3.connect(FINANCE_DB) as conn:
        df = pd.read_sql(
            f"""
            SELECT market, month_end, metric, unit, scope, basis, value
            FROM financials
            WHERE market IN ({ph}) AND month_end = ?
            """,
            conn, params=markets + [month_end]
        )
    return df

def get_history(market: str, metric: str, months: int = 12) -> pd.DataFrame:
    with sqlite3.connect(FINANCE_DB) as conn:
        df = pd.read_sql(
            """
            SELECT month_end, value
            FROM financials
            WHERE market = ? AND metric = ? AND scope = 'ITM' AND basis = 'Act'
            ORDER BY month_end
            """,
            conn, params=(market, metric)
        )
    if df.empty:
        return df
    return df.tail(months)

def get_history_multi(markets: List[str], metric: str) -> pd.DataFrame:
    if not markets:
        return pd.DataFrame(columns=["month_end","market","value"])
    ph = ",".join(["?"] * len(markets))
    with sqlite3.connect(FINANCE_DB) as conn:
        df = pd.read_sql(
            f"""
            SELECT month_end, market, value
            FROM financials
            WHERE market IN ({ph}) AND metric = ? AND scope = 'ITM' AND basis = 'Act'
            ORDER BY month_end
            """,
            conn, params=markets + [metric]
        )
    return df

# ----------------------------
# Aggregation (GLOBAL view)
# ----------------------------
def aggregate_slice(df: pd.DataFrame,
                    markets: List[str],
                    month_end: str,
                    weight_metric: Optional[str] = "Revenue",
                    weight_scope: str = "ITM",
                    weight_basis: str = "Act") -> pd.DataFrame:
    if df.empty:
        return df
    weights = None
    if weight_metric:
        weights = df[(df["metric"] == weight_metric) &
                     (df["scope"] == weight_scope) &
                     (df["basis"] == weight_basis)][["market", "value"]]
        weights = weights.rename(columns={"value": "w"}).set_index("market")["w"]

    out = []
    for (metric, unit, scope, basis), g in df.groupby(["metric","unit","scope","basis"], dropna=False):
        if _is_margin(metric, unit):
            if weights is not None and not weights.empty:
                merged = g.merge(weights.rename("w"), left_on="market", right_index=True, how="left")
                merged["w"] = merged["w"].fillna(0)
                num = (merged["value"] * merged["w"]).sum()
                den = merged["w"].sum()
                agg_val = (num / den) if den else g["value"].mean()
            else:
                agg_val = g["value"].mean()
        else:
            agg_val = g["value"].sum()
        out.append({
            "market": "GLOBAL", "month_end": month_end, "metric": metric, "unit": unit,
            "scope": scope, "basis": basis, "value": float(agg_val)
        })
    return pd.DataFrame(out)

def aggregate_history(markets: List[str],
                      metric: str,
                      weight_metric: Optional[str] = "Revenue",
                      weight_scope: str = "ITM",
                      weight_basis: str = "Act") -> pd.DataFrame:
    raw = get_history_multi(markets, metric)
    if raw.empty:
        return raw
    is_margin = (metric in MARGIN_HINTS) or metric.endswith("%")
    if not is_margin:
        return raw.groupby("month_end", as_index=False)["value"].sum()
    if not weight_metric:
        return raw.groupby("month_end", as_index=False)["value"].mean()

    ph = ",".join(["?"] * len(markets))
    with sqlite3.connect(FINANCE_DB) as conn:
        wdf = pd.read_sql(
            f"""
            SELECT month_end, market, value
            FROM financials
            WHERE market IN ({ph})
              AND metric = ?
              AND scope = ?
              AND basis = ?
            """,
            conn, params=markets + [weight_metric, weight_scope, weight_basis]
        )
    merged = raw.merge(wdf.rename(columns={"value":"w"}), on=["month_end","market"], how="left")
    merged["w"] = merged["w"].fillna(0)
    agg = merged.groupby("month_end").apply(
        lambda x: (x["value"] * x["w"]).sum() / x["w"].sum() if x["w"].sum() else x["value"].mean()
    ).reset_index(name="value")
    return agg

# ----------------------------
# Commentary (unchanged core)
# ----------------------------
def validate_commentary(text: str, metric: str) -> Tuple[float, bool]:
    coverage = 100 if metric.lower() in text.lower() else 50
    specificity = 100 if re.search(r"\d", text) else 50
    correctness = 90
    coherence = 90
    score = (coverage + specificity + correctness + coherence) / 4
    return score, score >= 80

def store_commentary(market: str, month_end: str, metric: str, text: str, author: str,
                     score: float, passed: bool, accepted: int = 0) -> None:
    with sqlite3.connect(COMMENTARY_DB) as conn:
        conn.execute(
            """
            INSERT INTO commentary (market, month_end, metric, text, author, validation_score, passed, accepted)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (market, month_end, metric, text, author, score, int(passed), int(accepted)),
        )

def load_commentary(market: str, month_end: str) -> pd.DataFrame:
    with sqlite3.connect(COMMENTARY_DB) as conn:
        df = pd.read_sql(
            """
            SELECT id, metric, text, author, validation_score AS score, passed, accepted
            FROM commentary
            WHERE market = ? AND month_end = ?
            ORDER BY id DESC
            """, conn, params=(market, month_end)
        )
    return df

def save_commentary_edits(df_edit: pd.DataFrame) -> None:
    with sqlite3.connect(COMMENTARY_DB) as conn:
        for _, r in df_edit.iterrows():
            conn.execute(
                """
                UPDATE commentary
                SET metric = ?, text = ?, author = ?, validation_score = ?, passed = ?, accepted = ?
                WHERE id = ?
                """,
                (r["metric"], r["text"], r["author"], float(r["score"]),
                 int(r["passed"]), int(r["accepted"]), int(r["id"]))
            )

# ----------------------------
# Auto commentary (kept as before)
# ----------------------------
def get_value(market: str, month_end: str, metric: str, scope: str, basis: str) -> Optional[float]:
    with sqlite3.connect(FINANCE_DB) as conn:
        df = pd.read_sql(
            """
            SELECT value FROM financials
            WHERE market = ? AND month_end = ? AND metric = ? AND scope = ? AND basis = ?
            """,
            conn, params=(market, month_end, metric, scope, basis)
        )
    return None if df.empty else float(df["value"].iloc[0])

def generate_cfo_commentary_enhanced(df_slice: pd.DataFrame, market: str, month_end: str,
                                     level_thr: float, margin_thr: float,
                                     kpis: List[str]) -> List[str]:
    comments: List[str] = []
    dt = pd.to_datetime(month_end)
    prev_m = (dt - MonthEnd(1)).strftime("%Y-%m-%d")
    prev_y = (dt - DateOffset(years=1)).strftime("%Y-%m-%d")

    def _thr(metric, unit):
        return (margin_thr if _is_margin(metric, unit) else level_thr)

    for metric in kpis:
        sub = df_slice[df_slice["metric"] == metric]
        if sub.empty:
            continue
        unit = sub["unit"].dropna().iloc[0] if not sub["unit"].dropna().empty else None
        thr = _thr(metric, unit)

        itm = sub[(sub["scope"] == "ITM")]
        if not itm.empty:
            vbud = itm[itm["basis"] == "vs BUD"]["value"]
            if not vbud.empty and abs(float(vbud.iloc[0])) >= thr:
                sign = "above" if vbud.iloc[0] > 0 else "below"
                comments.append(f"{metric} {sign} budget by {_fmt_delta(float(vbud.iloc[0]), unit)} (ITM).")

            vly = itm[itm["basis"] == "vs n-1"]["value"]
            if not vly.empty and abs(float(vly.iloc[0])) >= thr:
                sign = "up" if vly.iloc[0] > 0 else "down"
                comments.append(f"{metric} {sign} YoY by {_fmt_delta(float(vly.iloc[0]), unit)} (ITM).")

        if market != "GLOBAL":
            act_now = get_value(market, month_end, metric, "ITM", "Act")
            act_prev = get_value(market, prev_m, metric, "ITM", "Act")
            if act_now is not None and act_prev is not None:
                mom = act_now - act_prev
                if abs(mom) >= thr:
                    sign = "higher" if mom > 0 else "lower"
                    comments.append(f"{metric} {sign} MoM by {_fmt_delta(mom, unit)} (ITM).")

        ytd_now = sub[(sub["scope"] == "YTD") & (sub["basis"] == "Act")]["value"]
        if not ytd_now.empty and market != "GLOBAL":
            ytd_prev_y = get_value(market, prev_y, metric, "YTD", "Act")
            if ytd_prev_y is not None:
                yoy_ytd = float(ytd_now.iloc[0]) - ytd_prev_y
                if abs(yoy_ytd) >= thr:
                    sign = "up" if yoy_ytd > 0 else "down"
                    comments.append(f"{metric} {sign} versus PY-YTD by {_fmt_delta(yoy_ytd, unit)} (YTD).")
    return comments

# ----------------------------
# KPI priorities selector (robust, with preview)
# ----------------------------
def _colored_delta_html(metric: str, val: Optional[float], unit) -> str:
    if val is None or pd.isna(val):
        return f'<span style="color:{GREY}">–</span>'
    color = color_for_delta(metric, val)  # DSO business-sense
    return f'<span style="color:{color}; font-weight:600">{_fmt_delta(float(val), unit)}</span>'

def _preview_line(df_slice: pd.DataFrame, metric: str) -> str:
    sub = df_slice[df_slice["metric"] == metric]
    if sub.empty:
        return ""
    unit = sub["unit"].dropna().iloc[0] if not sub["unit"].dropna().empty else None
    vbud = sub[(sub["scope"] == "ITM") & (sub["basis"] == "vs BUD")]["value"]
    vly  = sub[(sub["scope"] == "ITM") & (sub["basis"] == "vs n-1")]["value"]
    vb = float(vbud.iloc[0]) if not vbud.empty else None
    vl = float(vly.iloc[0]) if not vly.empty else None
    return f"<b>{metric}</b> — vs BUD: {_colored_delta_html(metric, vb, unit)} | vs n-1: {_colored_delta_html(metric, vl, unit)}"

def _init_priority_table(metrics: List[str], key: str, default_show: bool) -> pd.DataFrame:
    df = pd.DataFrame({"metric": sorted(metrics), "Show": default_show})
    df["Show"] = df["Show"].astype(bool)
    prev = st.session_state.get(key)
    if isinstance(prev, pd.DataFrame) and {"metric", "Show"}.issubset(prev.columns):
        prev = prev[prev["metric"].isin(metrics)]
        prev_map = dict(zip(prev["metric"], prev["Show"].astype(bool)))
        df["Show"] = df["metric"].map(prev_map).fillna(default_show).astype(bool)
    st.session_state[key] = df
    return df

def kpi_priority_selector(df_slice: pd.DataFrame) -> List[str]:
    st.sidebar.header("KPI Priorities")

    available = sorted(df_slice["metric"].unique().tolist())
    p1 = [m for m in available if priority_of(m) == "P1"]
    p2 = [m for m in available if priority_of(m) == "P2"]
    p3 = [m for m in available if priority_of(m) == "P3"]

    # Fallback: if P2 empty, promote likely discipline proxies from P3
    if not p2:
        heur = [m for m in available if re.search(r'(opex|sg&?a|cost|rate|utili|headcount|fte|vendor|external|indirect)', m, re.I)]
        p2 = [m for m in heur if m not in p1]

    st.sidebar.caption(f"P1: {len(p1)} | P2: {len(p2)} | P3: {len(p3)}")

    tbl_p1 = _init_priority_table(p1, "kpi_tbl_P1", True)
    tbl_p2 = _init_priority_table(p2, "kpi_tbl_P2", False)
    tbl_p3 = _init_priority_table(p3, "kpi_tbl_P3", False)

    def table_block(title, key, df_tbl):
        with st.sidebar.expander(title, expanded=(key == "kpi_tbl_P1")):
            c1, c2 = st.columns(2)
            with c1:
                if st.button("Select all", key=key+"_all"):
                    df_tbl["Show"] = True
                    st.session_state[key] = df_tbl
            with c2:
                if st.button("Clear all", key=key+"_none"):
                    df_tbl["Show"] = False
                    st.session_state[key] = df_tbl

            edited = st.data_editor(
                st.session_state[key],
                key=key+"_editor",
                hide_index=True,
                column_config={
                    "metric": st.column_config.TextColumn(disabled=True),
                    "Show": st.column_config.CheckboxColumn(),
                },
                width="stretch",
            )
            if "Show" not in edited.columns:
                edited["Show"] = False
            edited["Show"] = edited["Show"].astype(bool)
            st.session_state[key] = edited

            show_prev = st.checkbox("Show variance preview", value=False, key=key+"_preview")
            if show_prev:
                if edited.empty:
                    st.markdown("*No metrics available.*")
                else:
                    for m in edited["metric"].tolist():
                        st.markdown(f"- {_preview_line(df_slice, m)}", unsafe_allow_html=True)

    table_block("P1 — Topline / Profit / Cash", "kpi_tbl_P1", tbl_p1)
    table_block("P2 — Efficiency & Spend", "kpi_tbl_P2", tbl_p2)
    table_block("P3 — Operational drivers", "kpi_tbl_P3", tbl_p3)

    # Final KPI set + expose per-priority selections
    kpis = []
    sel_p1 = []; sel_p2 = []; sel_p3 = []
    for key, lst, bucket in [("kpi_tbl_P1", p1, "P1"), ("kpi_tbl_P2", p2, "P2"), ("kpi_tbl_P3", p3, "P3")]:
        dfk = st.session_state.get(key)
        chosen = dfk.loc[dfk["Show"] == True, "metric"].tolist() if isinstance(dfk, pd.DataFrame) and "Show" in dfk.columns else []
        ordered = [m for m in lst if m in chosen]
        kpis.extend(ordered)
        if bucket == "P1": sel_p1 = ordered
        if bucket == "P2": sel_p2 = ordered
        if bucket == "P3": sel_p3 = ordered
    st.session_state["selected_P1"] = sel_p1
    st.session_state["selected_P2"] = sel_p2
    st.session_state["selected_P3"] = sel_p3
    return kpis

# ----------------------------
# “What Changed” (one row/metric, Impact, DSO coloring)
# ----------------------------
def threshold_for_metric(metric: str, unit: Optional[str], level_thr: float, margin_thr: float) -> float:
    return margin_thr if _is_margin(metric, unit) else level_thr

def build_changes_df(df_slice: pd.DataFrame,
                     level_thr: float,
                     margin_thr: float) -> pd.DataFrame:
    rows = []
    for metric in sorted(df_slice["metric"].unique()):
        sub = df_slice[df_slice["metric"] == metric]
        unit = sub["unit"].dropna().iloc[0] if not sub["unit"].dropna().empty else None

        act = sub[(sub["scope"] == "ITM") & (sub["basis"] == "Act")]["value"]
        bud = sub[(sub["scope"] == "ITM") & (sub["basis"] == "vs BUD")]["value"]
        ly  = sub[(sub["scope"] == "ITM") & (sub["basis"] == "vs n-1")]["value"]
        ytd_bud = sub[(sub["scope"] == "YTD") & (sub["basis"] == "vs BUD")]["value"]

        act_val = float(act.iloc[0]) if not act.empty else None
        d_bud   = float(bud.iloc[0]) if not bud.empty else None
        d_ly    = float(ly.iloc[0])  if not ly.empty  else None
        d_ytd   = float(ytd_bud.iloc[0]) if not ytd_bud.empty else None

        thr = threshold_for_metric(metric, unit, level_thr, margin_thr)
        basis_val = d_bud if d_bud is not None else (
            max([abs(x) for x in [d_ly, d_ytd] if x is not None], default=0.0)
        )
        impact = (abs(basis_val) / thr) if thr and basis_val is not None else 0.0

        rows.append({
            "priority": priority_of(metric),
            "metric": metric,
            "unit": unit,
            "act": act_val,
            "vs_bud": d_bud,
            "vs_ly": d_ly,
            "ytd_vs_bud": d_ytd,
            "impact": float(impact)
        })
    return pd.DataFrame(rows)

def render_changes_panel(df_slice: pd.DataFrame,
                         kpis: List[str],
                         level_thr: float,
                         margin_thr: float):
    st.subheader("What Changed")
    if df_slice.empty:
        st.info("No data for this selection.")
        return

    base = build_changes_df(df_slice, level_thr, margin_thr)
    if kpis:
        base = base[base["metric"].isin(kpis)]

    # Default view = P1
    view_opt = st.radio("View", options=["P1", "P2", "P3", "All"],
                        index=0, horizontal=True, key="wc_view")
    if view_opt != "All":
        base = base[base["priority"] == view_opt]
    if base.empty:
        st.info("No metrics match this view.")
        return

    def fmt_delta_cell(metric, val, unit):
        if val is None or pd.isna(val):
            return f'<span style="color:{GREY}">–</span>'
        color = color_for_delta(metric, val)
        return f'<span style="color:{color}; font-weight:600">{_fmt_delta(float(val), unit)}</span>'

    def fmt_act_cell(val, unit):
        if val is None or pd.isna(val):
            return f'<span style="color:{GREY}">–</span>'
        return f'<span style="font-weight:600">{_fmt_value(float(val), unit)}</span>'

    show = base.copy()
    show["ITM Act"]  = [fmt_act_cell(r.act, r.unit) for r in show.itertuples(index=False)]
    show["vs BUD"]   = [fmt_delta_cell(r.metric, r.vs_bud, r.unit) for r in show.itertuples(index=False)]
    show["vs n-1"]   = [fmt_delta_cell(r.metric, r.vs_ly, r.unit) for r in show.itertuples(index=False)]
    have_ytd = show["ytd_vs_bud"].notna().any()
    if have_ytd:
        show["YTD vs BUD"] = [fmt_delta_cell(r.metric, r.ytd_vs_bud, r.unit) for r in show.itertuples(index=False)]
    show["Impact"]   = show["impact"].apply(lambda x: f"{x:.1f}×" if x and x != float("inf") else "0.0×")
    show = show.sort_values("impact", ascending=False)

    display_cols = ["metric", "ITM Act", "vs BUD", "vs n-1"] + (["YTD vs BUD"] if have_ytd else []) + ["Impact"]
    st.dataframe(
        show[display_cols].rename(columns={"metric": "Metric"}),
        width="stretch"
    )

# ----------------------------
# Trends + Priority charts
# ----------------------------
def render_kpi_cards(df_slice: pd.DataFrame, kpis: List[str]):
    st.subheader("KPIs (Current Month)")
    if not kpis:
        st.info("No KPIs selected. Use the sidebar to choose P1/P2/P3 items.")
        return
    cols = st.columns(len(kpis))
    for i, metric in enumerate(kpis):
        sub = df_slice[df_slice["metric"] == metric]
        if sub.empty:
            with cols[i]:
                st.metric(metric, value="-", delta=None)
            continue
        unit = sub["unit"].dropna().iloc[0] if not sub["unit"].dropna().empty else None
        act = sub[(sub["scope"] == "ITM") & (sub["basis"] == "Act")]["value"]
        bud = sub[(sub["scope"] == "ITM") & (sub["basis"] == "vs BUD")]["value"]
        ly  = sub[(sub["scope"] == "ITM") & (sub["basis"] == "vs n-1")]["value"]
        act_val = act.iloc[0] if not act.empty else None
        bud_delta = _fmt_delta(float(bud.iloc[0]), unit) if not bud.empty else None
        ly_delta  = _fmt_delta(float(ly.iloc[0]), unit) if not ly.empty else None
        delta_str = " | ".join([x for x in [f"vs BUD: {bud_delta}" if bud_delta else None,
                                            f"vs n-1: {ly_delta}" if ly_delta else None] if x]) or None
        with cols[i]:
            st.metric(metric, value=_fmt_value(act_val, unit), delta=delta_str)

def render_trends(markets: List[str], kpis: List[str], engine: str,
                  weight_metric: Optional[str], weight_scope: str, weight_basis: str,
                  global_view: bool):
    with st.expander("Trends (last 12 months)", expanded=False):
        if engine == "Plotly":
            try:
                import plotly.express as px
            except Exception:
                st.error("Plotly not installed. Run: pip install plotly")
                return
        for metric in kpis:
            if global_view and len(markets) > 1:
                agg = aggregate_history(markets, metric, weight_metric, weight_scope, weight_basis)
                if agg.empty:
                    continue
                agg["month_end"] = pd.to_datetime(agg["month_end"])
                st.caption(f"{metric} — GLOBAL")
                if engine == "Plotly":
                    fig = px.line(agg, x="month_end", y="value")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.line_chart(agg.set_index("month_end")[["value"]])
            else:
                mkt = markets[0]
                hist = get_history(mkt, metric, months=12)
                if hist.empty:
                    continue
                hist["month_end"] = pd.to_datetime(hist["month_end"])
                st.caption(f"{metric} — {mkt}")
                if engine == "Plotly":
                    fig = px.line(hist, x="month_end", y="value")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.line_chart(hist.set_index("month_end")[["value"]])

def render_priority_charts(markets: List[str],
                           engine: str,
                           weight_metric: Optional[str],
                           weight_scope: str,
                           weight_basis: str,
                           global_view: bool):
    try:
        import plotly.express as px
        have_plotly = True
    except Exception:
        have_plotly = False

    def _series(metric: str):
        if global_view and len(markets) > 1:
            data = aggregate_history(markets, metric, weight_metric, weight_scope, weight_basis)
            title_suffix = " — GLOBAL"
        else:
            mkt = markets[0]
            data = get_history(mkt, metric, months=12)
            title_suffix = f" — {mkt}"
        return data, title_suffix

    def _chart(metric: str):
        data, suffix = _series(metric)
        if data is None or data.empty:
            return
        data["month_end"] = pd.to_datetime(data["month_end"])
        st.caption(f"{metric}{suffix}")
        if engine == "Plotly" and have_plotly:
            fig = px.line(data, x="month_end", y="value")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.line_chart(data.set_index("month_end")[["value"]])

    p2 = st.session_state.get("selected_P2", [])
    if p2:
        st.subheader("P2 KPI Charts")
        for m in p2:
            _chart(m)
    p3 = st.session_state.get("selected_P3", [])
    if p3:
        st.subheader("P3 KPI Charts")
        for m in p3:
            _chart(m)

# ----------------------------
# Commentary panel
# ----------------------------
def render_commentary_panel(market_label: str, month_end: str, df_slice: pd.DataFrame,
                            kpis: List[str], level_thr: float, margin_thr: float):
    st.subheader("Commentary")

    auto_comments = generate_cfo_commentary_enhanced(df_slice, market_label, month_end, level_thr, margin_thr, kpis)
    if auto_comments:
        st.markdown("**Auto Commentary**")
        for c in auto_comments:
            st.write("-", c)
        if st.checkbox("Persist auto commentary to repository", value=False):
            if st.button("Save Auto Commentary"):
                for c in auto_comments:
                    metric = next((m for m in kpis if m.lower() in c.lower()), "General")
                    score, passed = validate_commentary(c, metric)
                    store_commentary(market_label, month_end, metric, c, "Auto", score, passed, accepted=1)
                st.success("Auto commentary saved.")

    st.markdown("---")
    st.markdown("**Add Manual Commentary**")
    metric = st.selectbox("Metric", kpis, key="manual_metric")
    text = st.text_area("Commentary", key="manual_text")
    author = st.text_input("Author", key="manual_author", value="")
    if st.button("Submit Commentary"):
        score, passed = validate_commentary(text, metric)
        store_commentary(market_label, month_end, metric, text, author or "User", score, passed, accepted=0)
        st.success(f"Saved. Score: {score:.1f} — Pass: {passed}")

    st.markdown("---")
    st.markdown("**Commentary Manager (edit / accept / reject)**")
    saved = load_commentary(market_label, month_end)
    if not saved.empty:
        edited = st.data_editor(
            saved,
            num_rows="dynamic",
            column_config={
                "id": st.column_config.NumberColumn(disabled=True),
                "passed": st.column_config.CheckboxColumn(),
                "accepted": st.column_config.CheckboxColumn()
            },
            width="stretch",
        )
        if st.button("Apply Changes"):
            save_commentary_edits(edited)
            st.success("Commentary updated.")
    else:
        st.info("No saved commentary yet for this selection.")

# ----------------------------
# Downloads
# ----------------------------
def render_downloads(df_slice: pd.DataFrame, market_label: str, month_end: str):
    st.subheader("Data & Exports")
    if not df_slice.empty:
        st.download_button(
            "Download current selection CSV",
            data=df_slice.to_csv(index=False).encode("utf-8"),
            file_name=f"{market_label}_{month_end}_slice.csv",
            mime="text/csv",
        )
    with sqlite3.connect(FINANCE_DB) as conn:
        all_fin = pd.read_sql("SELECT * FROM financials", conn)
    if not all_fin.empty:
        st.download_button(
            "Download full financials CSV",
            data=all_fin.to_csv(index=False).encode("utf-8"),
            file_name="financials_full.csv",
            mime="text/csv",
        )

# ----------------------------
# Streamlit app
# ----------------------------
def run_streamlit() -> None:
    st.set_page_config(page_title="Finance Dashboard", layout="wide")
    st.title("Finance Dashboard")
    init_dbs()

    st.header("Upload Excel")
    uploaded = st.file_uploader("Upload financial report (e.g., SG_Aug2025.xlsx)", type=["xlsx"])
    if uploaded is not None:
        try:
            df = ingest_excel_upload(uploaded, uploaded.name)
            store_financials(df)
            st.success(f"Data ingested for {df['market'].iloc[0]} — {df['month_end'].iloc[0]}")
        except Exception as e:
            st.error(str(e))

    st.markdown("---")
    st.header("Current View")

    markets_all = get_markets()
    if not markets_all:
        st.info("No data yet. Upload a file to begin.")
        return

    col_a, col_b = st.columns([2, 2])
    with col_a:
        markets = st.multiselect("Markets (multi-select up to 50)",
                                 options=markets_all,
                                 default=[markets_all[0]],
                                 max_selections=50)
    with col_b:
        months = get_months_for_markets(markets) if markets else []
        month_end = st.selectbox("Month", months, index=len(months) - 1 if months else 0)

    if not markets or not month_end:
        st.info("Select at least one market and a month.")
        return

    # Sidebar controls
    st.sidebar.header("Display & Thresholds")
    global_view = st.sidebar.checkbox("Combine selected markets as GLOBAL view", value=(len(markets) > 1))
    weight_scope = st.sidebar.selectbox("Weight scope (for %/rate)", ["ITM", "YTD"], index=0)
    weight_basis = st.sidebar.selectbox("Weight basis (for %/rate)", ["Act"], index=0)
    weight_metric = st.sidebar.text_input("Weight metric name", value="Revenue")
    level_thr = st.sidebar.number_input("Threshold (level metrics)", value=5.0, step=0.5)
    margin_thr = st.sidebar.number_input("Threshold (margin metrics, pp)", value=0.5, step=0.1)
    chart_engine = st.sidebar.selectbox("Chart Engine", ["Streamlit", "Plotly"])

    # Slice (single or global)
    if len(markets) == 1 and not global_view:
        df_slice = get_slice(markets[0], month_end)
        market_label = markets[0]
    else:
        raw = get_slice_multi(markets, month_end)
        df_slice = aggregate_slice(raw, markets, month_end, weight_metric, weight_scope, weight_basis) if global_view else raw
        market_label = "GLOBAL" if global_view else "/".join(markets)

    if df_slice.empty:
        st.warning("No data for this selection.")
        return

    # KPI selection
    kpis = kpi_priority_selector(df_slice)

    # Missing actuals banner
    missing = [m for m in kpis if df_slice[(df_slice["metric"] == m) &
                                           (df_slice["scope"] == "ITM") &
                                           (df_slice["basis"] == "Act")].empty]
    if missing:
        st.warning(f"Missing ITM Actuals for: {', '.join(missing)}")

    # Sections
    render_kpi_cards(df_slice, kpis)
    render_changes_panel(df_slice, kpis, level_thr, margin_thr)
    render_trends(markets, kpis, chart_engine, weight_metric, weight_scope, weight_basis, global_view)
    render_priority_charts(markets, chart_engine, weight_metric, weight_scope, weight_basis, global_view)
    render_commentary_panel(market_label, month_end, df_slice, kpis, level_thr, margin_thr)
    render_downloads(df_slice, market_label, month_end)

# ----------------------------
# CLI
# ----------------------------
def cli_ingest(args: argparse.Namespace) -> None:
    init_dbs()
    df = ingest_excel_path(args.file)
    store_financials(df)
    print("Ingested:", args.file)

def cli_comment(args: argparse.Namespace) -> None:
    init_dbs()
    text = " ".join(args.text)
    score, passed = validate_commentary(text, args.metric)
    store_commentary(args.market, args.month_end, args.metric, text, args.author, score, passed, accepted=0)
    print(f"Saved. Score: {score:.1f} — Pass: {passed}")

def main() -> None:
    if len(sys.argv) == 1:
        run_streamlit()
        return
    parser = argparse.ArgumentParser(description="Finance dashboard CLI")
    sub = parser.add_subparsers(dest="command")
    p_ingest = sub.add_parser("ingest", help="Ingest an Excel file")
    p_ingest.add_argument("file", help="Path to Excel file")
    p_ingest.set_defaults(func=cli_ingest)
    p_comment = sub.add_parser("comment", help="Store a commentary")
    p_comment.add_argument("market"); p_comment.add_argument("month_end")
    p_comment.add_argument("metric"); p_comment.add_argument("author")
    p_comment.add_argument("text", nargs="+")
    p_comment.set_defaults(func=cli_comment)
    args = parser.parse_args()
    if hasattr(args, "func"):
        args.func(args)

if __name__ == "__main__":
    main()


FINANCE_DB = "finance.db"
COMMENTARY_DB = "commentary.db"

# Default KPI suggestions; actual availability comes from data
DEFAULT_KPIS = ["Revenue", "GM%", "EBITDA%", "Opex%", "DSO", "Capex"]
MARGIN_HINTS = {"GM%", "EBITDA%", "Opex%"}           # treat as margin-like
MARGIN_UNITS = {"%", "pts", "pt"}                    # treat as margin-like

# Priority rules (regex → P1/P2; everything else becomes P3)
PRIORITY_RULES = [
    (re.compile(r'^(revenue|sales|net\s*revenue)$', re.I), "P1"),
    (re.compile(r'^(gross\s*margin(\s*%)?|gm%?|gm$)', re.I), "P1"),
    (re.compile(r'^ebitda(\s*%)?$', re.I), "P1"),
    (re.compile(r'^dso$', re.I), "P1"),
    (re.compile(r'^(capex|capital\s*exp)', re.I), "P1"),

    (re.compile(r'^(opex(\s*%)?|operating\s*expense)', re.I), "P2"),
    (re.compile(r'^(dpo|dio|ccc|cash\s*conversion)', re.I), "P2"),
    (re.compile(r'^(bookings|billings|arr|mrr)', re.I), "P2"),
]

def priority_of(metric: str) -> str:
    name = (metric or "").strip()
    for pat, p in PRIORITY_RULES:
        if pat.search(name):
            return p
    return "P3"

# Colors for preview
DARK_GREEN = "#006400"
RED = "#C00000"
GREY = "#9CA3AF"


# ----------------------------
# DB init & schema evolve
# ----------------------------
def init_dbs() -> None:
    with sqlite3.connect(FINANCE_DB) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS financials (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market TEXT,
                month_end TEXT,
                metric TEXT,
                unit TEXT,
                scope TEXT,
                basis TEXT,
                value REAL
            )
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_fin
            ON financials(market, month_end, metric, unit, scope, basis)
            """
        )

    with sqlite3.connect(COMMENTARY_DB) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS commentary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market TEXT,
                month_end TEXT,
                metric TEXT,
                text TEXT,
                author TEXT,
                validation_score REAL,
                passed INTEGER,
                accepted INTEGER DEFAULT 0
            )
            """
        )
        # evolve: ensure accepted exists
        cols = pd.read_sql("PRAGMA table_info(commentary);", conn)
        if "accepted" not in cols["name"].tolist():
            conn.execute("ALTER TABLE commentary ADD COLUMN accepted INTEGER DEFAULT 0")


# ----------------------------
# Helpers
# ----------------------------
def _true_month_end(date_obj: datetime) -> str:
    return (date_obj + MonthEnd(0)).strftime("%Y-%m-%d")

def parse_filename(filename: str) -> Tuple[str, str]:
    """
    Expect '<MARKET>_<MonYYYY>.xlsx', e.g., 'SG_Aug2025.xlsx'.
    Returns (market, month_end as 'YYYY-MM-DD').
    """
    base_no_ext = os.path.splitext(os.path.basename(filename))[0]
    m = re.match(r"^(?P<market>[A-Za-z]+)_(?P<mon>[A-Za-z]{3}\d{4})$", base_no_ext)
    if not m:
        raise ValueError("Filename must be '<MARKET>_<MonYYYY>.xlsx' (e.g., 'SG_Aug2025.xlsx').")
    market = m.group("market").upper()
    mon = datetime.strptime(m.group("mon").title(), "%b%Y")
    return market, _true_month_end(mon)

def _read_excel_or_fail(io_obj):
    try:
        return pd.read_excel(io_obj, engine="openpyxl")
    except ImportError as e:
        raise RuntimeError("Excel engine missing. Install it: pip install openpyxl") from e

def _strip_nullable(s: pd.Series) -> pd.Series:
    return s.apply(lambda v: v.strip() if isinstance(v, str) else v)

def _fmt_value(val, unit):
    if pd.isna(val) or val is None:
        return "-"
    if isinstance(unit, str) and unit.strip():
        u = unit.strip()
        if u == "%":
            return f"{val:.2f}{u}"
        return f"{val:.2f} {u}"
    return f"{val:.2f}"

def _fmt_delta(val: float, unit) -> str:
    if isinstance(unit, str) and unit.strip() in ("%", "pts", "pt"):
        return f"{val:.2f} pp"
    if isinstance(unit, str) and unit.strip():
        return f"{val:.2f} {unit.strip()}"
    return f"{val:.2f}"

def _is_margin(metric: str, unit: Optional[str]) -> bool:
    if metric in MARGIN_HINTS:
        return True
    if isinstance(unit, str) and unit.strip() in MARGIN_UNITS:
        return True
    return False


# ----------------------------
# Excel ingestion (tailored to your sheet schema)
# ----------------------------
def _allowed_map(cols: List[str]) -> dict:
    """Map column -> (scope,basis); ignore prior-month and 'derived' columns."""
    out = {}
    pat_vs = re.compile(r"^vs\s+(n-1|fct|bud)\s+\((ITM|YTD)\)$", re.IGNORECASE)
    for c in cols:
        c0 = str(c).strip()
        scope = basis = None
        if c0 == "m":
            scope, basis = "ITM", "Act"
        elif c0 == "Act (YTD)":
            scope, basis = "YTD", "Act"
        else:
            m = pat_vs.match(c0)
            if m:
                basis_map = {"n-1": "vs n-1", "fct": "vs Fct", "bud": "vs BUD"}
                basis = basis_map[m.group(1).lower()]
                scope = m.group(2).upper()
        out[c0] = (scope, basis)
    return out

def _tidy_from_dataframe(df: pd.DataFrame, market: str, month_end: str) -> pd.DataFrame:
    df = df.copy()
    df.rename(columns={df.columns[0]: "Metric", df.columns[1]: "Unit"}, inplace=True)

    allowed = _allowed_map(df.columns.tolist())
    keep_cols = [c for c, (s, b) in allowed.items() if s is not None]

    base_cols = ["Metric", "Unit"] + keep_cols
    m = df[base_cols].melt(id_vars=["Metric", "Unit"], var_name="column", value_name="value")
    m["scope"] = m["column"].map(lambda c: allowed[c][0])
    m["basis"] = m["column"].map(lambda c: allowed[c][1])
    m["market"] = market
    m["month_end"] = month_end
    m.rename(columns={"Metric": "metric", "Unit": "unit"}, inplace=True)

    m["value"] = pd.to_numeric(m["value"], errors="coerce")
    m = m.dropna(subset=["value"])

    # Clean text fields (no "nan" strings)
    m["metric"] = _strip_nullable(m["metric"])
    m["unit"] = _strip_nullable(m["unit"])
    m["scope"] = m["scope"].apply(lambda v: v.upper() if isinstance(v, str) else v)
    m["basis"] = _strip_nullable(m["basis"])

    key_cols = ["market", "month_end", "metric", "unit", "scope", "basis"]
    m = (
        m.sort_values(key_cols + ["value"], na_position="last")
         .drop_duplicates(subset=key_cols, keep="last")
    )
    return m[key_cols + ["value"]]

def ingest_excel_path(path: str) -> pd.DataFrame:
    market, month_end = parse_filename(path)
    df_raw = _read_excel_or_fail(path)
    return _tidy_from_dataframe(df_raw, market, month_end)

def ingest_excel_upload(file_obj, original_name: str) -> pd.DataFrame:
    market, month_end = parse_filename(original_name)
    df_raw = _read_excel_or_fail(file_obj)
    return _tidy_from_dataframe(df_raw, market, month_end)


# ----------------------------
# Storage
# ----------------------------
def store_financials(df: pd.DataFrame) -> None:
    key_cols = ["market", "month_end", "metric", "unit", "scope", "basis"]
    df2 = (
        df.sort_values(key_cols + ["value"])
          .drop_duplicates(subset=key_cols, keep="last")
    )
    pairs = df2[["market", "month_end"]].drop_duplicates().itertuples(index=False, name=None)
    with sqlite3.connect(FINANCE_DB) as conn:
        cur = conn.cursor()
        for market, month_end in pairs:
            cur.execute("DELETE FROM financials WHERE market = ? AND month_end = ?", (market, month_end))
        conn.commit()
        df2.to_sql("financials", conn, if_exists="append", index=False)


# ----------------------------
# Data APIs
# ----------------------------
def get_markets() -> List[str]:
    with sqlite3.connect(FINANCE_DB) as conn:
        df = pd.read_sql("SELECT DISTINCT market FROM financials ORDER BY market", conn)
    return df["market"].dropna().tolist()

def get_months_for_markets(markets: List[str]) -> List[str]:
    if not markets:
        return []
    ph = ",".join(["?"] * len(markets))
    with sqlite3.connect(FINANCE_DB) as conn:
        df = pd.read_sql(
            f"SELECT DISTINCT month_end FROM financials WHERE market IN ({ph}) ORDER BY month_end",
            conn, params=markets
        )
    return df["month_end"].dropna().tolist()

def get_slice(market: str, month_end: str) -> pd.DataFrame:
    with sqlite3.connect(FINANCE_DB) as conn:
        df = pd.read_sql(
            """
            SELECT market, month_end, metric, unit, scope, basis, value
            FROM financials
            WHERE market = ? AND month_end = ?
            """,
            conn, params=(market, month_end)
        )
    return df

def get_slice_multi(markets: List[str], month_end: str) -> pd.DataFrame:
    if not markets:
        return pd.DataFrame(columns=["market","month_end","metric","unit","scope","basis","value"])
    ph = ",".join(["?"] * len(markets))
    with sqlite3.connect(FINANCE_DB) as conn:
        df = pd.read_sql(
            f"""
            SELECT market, month_end, metric, unit, scope, basis, value
            FROM financials
            WHERE market IN ({ph}) AND month_end = ?
            """,
            conn, params=markets + [month_end]
        )
    return df

def get_history(market: str, metric: str, months: int = 12) -> pd.DataFrame:
    with sqlite3.connect(FINANCE_DB) as conn:
        df = pd.read_sql(
            """
            SELECT month_end, value
            FROM financials
            WHERE market = ? AND metric = ? AND scope = 'ITM' AND basis = 'Act'
            ORDER BY month_end
            """,
            conn, params=(market, metric)
        )
    if df.empty:
        return df
    return df.tail(months)

def get_history_multi(markets: List[str], metric: str) -> pd.DataFrame:
    if not markets:
        return pd.DataFrame(columns=["month_end","market","value"])
    ph = ",".join(["?"] * len(markets))
    with sqlite3.connect(FINANCE_DB) as conn:
        df = pd.read_sql(
            f"""
            SELECT month_end, market, value
            FROM financials
            WHERE market IN ({ph}) AND metric = ? AND scope = 'ITM' AND basis = 'Act'
            ORDER BY month_end
            """,
            conn, params=markets + [metric]
        )
    return df


# ----------------------------
# Aggregation (Global view)
# ----------------------------
def aggregate_slice(df: pd.DataFrame,
                    markets: List[str],
                    month_end: str,
                    weight_metric: Optional[str] = "Revenue",
                    weight_scope: str = "ITM",
                    weight_basis: str = "Act") -> pd.DataFrame:
    if df.empty:
        return df

    # Precompute weights per market
    weights = None
    if weight_metric:
        weights = df[(df["metric"] == weight_metric) &
                     (df["scope"] == weight_scope) &
                     (df["basis"] == weight_basis)][["market", "value"]]
        weights = weights.rename(columns={"value": "w"}).set_index("market")["w"]

    out = []
    for (metric, unit, scope, basis), g in df.groupby(["metric","unit","scope","basis"], dropna=False):
        if _is_margin(metric, unit):
            if weights is not None and not weights.empty:
                merged = g.merge(weights.rename("w"), left_on="market", right_index=True, how="left")
                merged["w"] = merged["w"].fillna(0)
                num = (merged["value"] * merged["w"]).sum()
                den = merged["w"].sum()
                agg_val = (num / den) if den else g["value"].mean()
            else:
                agg_val = g["value"].mean()
        else:
            agg_val = g["value"].sum()

        out.append({
            "market": "GLOBAL",
            "month_end": month_end,
            "metric": metric,
            "unit": unit,
            "scope": scope,
            "basis": basis,
            "value": float(agg_val)
        })

    return pd.DataFrame(out)

def aggregate_history(markets: List[str],
                      metric: str,
                      weight_metric: Optional[str] = "Revenue",
                      weight_scope: str = "ITM",
                      weight_basis: str = "Act") -> pd.DataFrame:
    raw = get_history_multi(markets, metric)
    if raw.empty:
        return raw

    is_margin = (metric in MARGIN_HINTS) or metric.endswith("%")
    if not is_margin:
        return raw.groupby("month_end", as_index=False)["value"].sum()

    if not weight_metric:
        return raw.groupby("month_end", as_index=False)["value"].mean()

    ph = ",".join(["?"] * len(markets))
    with sqlite3.connect(FINANCE_DB) as conn:
        wdf = pd.read_sql(
            f"""
            SELECT month_end, market, value
            FROM financials
            WHERE market IN ({ph})
              AND metric = ?
              AND scope = ?
              AND basis = ?
            """,
            conn, params=markets + [weight_metric, weight_scope, weight_basis]
        )

    merged = raw.merge(wdf.rename(columns={"value":"w"}), on=["month_end","market"], how="left")
    merged["w"] = merged["w"].fillna(0)
    agg = merged.groupby("month_end").apply(
        lambda x: (x["value"] * x["w"]).sum() / x["w"].sum() if x["w"].sum() else x["value"].mean()
    ).reset_index(name="value")
    return agg


# ----------------------------
# Commentary
# ----------------------------
def validate_commentary(text: str, metric: str) -> Tuple[float, bool]:
    coverage = 100 if metric.lower() in text.lower() else 50
    specificity = 100 if re.search(r"\d", text) else 50
    correctness = 90
    coherence = 90
    score = (coverage + specificity + correctness + coherence) / 4
    return score, score >= 80

def store_commentary(market: str, month_end: str, metric: str, text: str, author: str,
                     score: float, passed: bool, accepted: int = 0) -> None:
    with sqlite3.connect(COMMENTARY_DB) as conn:
        conn.execute(
            """
            INSERT INTO commentary (market, month_end, metric, text, author, validation_score, passed, accepted)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (market, month_end, metric, text, author, score, int(passed), int(accepted)),
        )

def load_commentary(market: str, month_end: str) -> pd.DataFrame:
    with sqlite3.connect(COMMENTARY_DB) as conn:
        df = pd.read_sql(
            """
            SELECT id, metric, text, author, validation_score AS score, passed, accepted
            FROM commentary
            WHERE market = ? AND month_end = ?
            ORDER BY id DESC
            """, conn, params=(market, month_end)
        )
    return df

def save_commentary_edits(df_edit: pd.DataFrame) -> None:
    with sqlite3.connect(COMMENTARY_DB) as conn:
        for _, r in df_edit.iterrows():
            conn.execute(
                """
                UPDATE commentary
                SET metric = ?, text = ?, author = ?, validation_score = ?, passed = ?, accepted = ?
                WHERE id = ?
                """,
                (r["metric"], r["text"], r["author"], float(r["score"]),
                 int(r["passed"]), int(r["accepted"]), int(r["id"]))
            )


# ----------------------------
# Auto commentary (kept as before)
# ----------------------------
def get_value(market: str, month_end: str, metric: str, scope: str, basis: str) -> Optional[float]:
    with sqlite3.connect(FINANCE_DB) as conn:
        df = pd.read_sql(
            """
            SELECT value FROM financials
            WHERE market = ? AND month_end = ? AND metric = ? AND scope = ? AND basis = ?
            """,
            conn, params=(market, month_end, metric, scope, basis)
        )
    return None if df.empty else float(df["value"].iloc[0])

def generate_cfo_commentary_enhanced(df_slice: pd.DataFrame, market: str, month_end: str,
                                     level_thr: float, margin_thr: float,
                                     kpis: List[str]) -> List[str]:
    comments: List[str] = []
    dt = pd.to_datetime(month_end)
    prev_m = (dt - MonthEnd(1)).strftime("%Y-%m-%d")
    prev_y = (dt - DateOffset(years=1)).strftime("%Y-%m-%d")

    def _thr(metric, unit):
        return (margin_thr if _is_margin(metric, unit) else level_thr)

    for metric in kpis:
        sub = df_slice[df_slice["metric"] == metric]
        if sub.empty:
            continue
        unit = sub["unit"].dropna().iloc[0] if not sub["unit"].dropna().empty else None
        thr = _thr(metric, unit)

        itm = sub[(sub["scope"] == "ITM")]
        if not itm.empty:
            vbud = itm[itm["basis"] == "vs BUD"]["value"]
            if not vbud.empty and abs(float(vbud.iloc[0])) >= thr:
                sign = "above" if vbud.iloc[0] > 0 else "below"
                comments.append(f"{metric} {sign} budget by {_fmt_delta(float(vbud.iloc[0]), unit)} (ITM).")

            vly = itm[itm["basis"] == "vs n-1"]["value"]
            if not vly.empty and abs(float(vly.iloc[0])) >= thr:
                sign = "up" if vly.iloc[0] > 0 else "down"
                comments.append(f"{metric} {sign} YoY by {_fmt_delta(float(vly.iloc[0]), unit)} (ITM).")

        if market != "GLOBAL":
            act_now = get_value(market, month_end, metric, "ITM", "Act")
            act_prev = get_value(market, prev_m, metric, "ITM", "Act")
            if act_now is not None and act_prev is not None:
                mom = act_now - act_prev
                if abs(mom) >= thr:
                    sign = "higher" if mom > 0 else "lower"
                    comments.append(f"{metric} {sign} MoM by {_fmt_delta(mom, unit)} (ITM).")

        ytd_now = sub[(sub["scope"] == "YTD") & (sub["basis"] == "Act")]["value"]
        if not ytd_now.empty and market != "GLOBAL":
            ytd_prev_y = get_value(market, prev_y, metric, "YTD", "Act")
            if ytd_prev_y is not None:
                yoy_ytd = float(ytd_now.iloc[0]) - ytd_prev_y
                if abs(yoy_ytd) >= thr:
                    sign = "up" if yoy_ytd > 0 else "down"
                    comments.append(f"{metric} {sign} versus PY-YTD by {_fmt_delta(yoy_ytd, unit)} (YTD).")
    return comments


# ----------------------------
# KPI priorities selector (robust, with red/green preview)
# ----------------------------
def _colored_delta_html(val: Optional[float], unit) -> str:
    if val is None or pd.isna(val):
        return f'<span style="color:{GREY}">–</span>'
    color = DARK_GREEN if float(val) > 0 else RED if float(val) < 0 else GREY
    return f'<span style="color:{color}; font-weight:600">{_fmt_delta(float(val), unit)}</span>'

def _preview_line(df_slice: pd.DataFrame, metric: str) -> str:
    sub = df_slice[df_slice["metric"] == metric]
    if sub.empty:
        return ""
    unit = sub["unit"].dropna().iloc[0] if not sub["unit"].dropna().empty else None
    vbud = sub[(sub["scope"] == "ITM") & (sub["basis"] == "vs BUD")]["value"]
    vly  = sub[(sub["scope"] == "ITM") & (sub["basis"] == "vs n-1")]["value"]
    vb = float(vbud.iloc[0]) if not vbud.empty else None
    vl = float(vly.iloc[0]) if not vly.empty else None
    return f"<b>{metric}</b> — vs BUD: {_colored_delta_html(vb, unit)} | vs n-1: {_colored_delta_html(vl, unit)}"

def _init_priority_table(metrics: List[str], key: str, default_show: bool) -> pd.DataFrame:
    # Always start with both columns present
    df = pd.DataFrame({"metric": sorted(metrics), "Show": default_show})
    df["Show"] = df["Show"].astype(bool)

    # Overlay prior selections if present & valid
    prev = st.session_state.get(key)
    if isinstance(prev, pd.DataFrame) and {"metric", "Show"}.issubset(prev.columns):
        prev = prev[prev["metric"].isin(metrics)]
        prev_map = dict(zip(prev["metric"], prev["Show"].astype(bool)))
        df["Show"] = df["metric"].map(prev_map).fillna(default_show).astype(bool)

    st.session_state[key] = df
    return df

def kpi_priority_selector(df_slice: pd.DataFrame) -> List[str]:
    st.sidebar.header("KPI Priorities")

    available = sorted(df_slice["metric"].unique().tolist())
    p1 = [m for m in available if priority_of(m) == "P1"]
    p2 = [m for m in available if priority_of(m) == "P2"]
    p3 = [m for m in available if priority_of(m) == "P3"]

    # Initialize tables (defaults: P1 shown; P2/P3 hidden)
    tbl_p1 = _init_priority_table(p1, "kpi_tbl_P1", True)
    tbl_p2 = _init_priority_table(p2, "kpi_tbl_P2", False)
    tbl_p3 = _init_priority_table(p3, "kpi_tbl_P3", False)

    def table_block(title, key, df_tbl):
        with st.sidebar.expander(title, expanded=(key == "kpi_tbl_P1")):
            c1, c2 = st.columns(2)
            with c1:
                if st.button("Select all", key=key+"_all"):
                    df_tbl["Show"] = True
                    st.session_state[key] = df_tbl
            with c2:
                if st.button("Clear all", key=key+"_none"):
                    df_tbl["Show"] = False
                    st.session_state[key] = df_tbl

            edited = st.data_editor(
                st.session_state[key],
                key=key+"_editor",
                hide_index=True,
                column_config={
                    "metric": st.column_config.TextColumn(disabled=True),
                    "Show": st.column_config.CheckboxColumn(),
                },
                width="stretch",
            )
            # Safety: ensure Show exists & boolean
            if "Show" not in edited.columns:
                edited["Show"] = False
            edited["Show"] = edited["Show"].astype(bool)
            st.session_state[key] = edited

            show_prev = st.checkbox("Show variance preview", value=False, key=key+"_preview")
            if show_prev:
                if edited.empty:
                    st.markdown("*No metrics available.*")
                else:
                    for m in edited["metric"].tolist():
                        st.markdown(f"- {_preview_line(df_slice, m)}", unsafe_allow_html=True)

    table_block("P1 — Topline / Profit / Cash", "kpi_tbl_P1", tbl_p1)
    table_block("P2 — Efficiency & Spend", "kpi_tbl_P2", tbl_p2)
    table_block("P3 — Operational drivers", "kpi_tbl_P3", tbl_p3)

    # Final KPI set = union of all Show==True in P1,P2,P3 (preserve priority order)
    kpis = []
    for key, lst in [("kpi_tbl_P1", p1), ("kpi_tbl_P2", p2), ("kpi_tbl_P3", p3)]:
        dfk = st.session_state.get(key)
        chosen = dfk.loc[dfk["Show"] == True, "metric"].tolist() if isinstance(dfk, pd.DataFrame) and "Show" in dfk.columns else []
        ordered = [m for m in lst if m in chosen]
        kpis.extend(ordered)
    return kpis


# ----------------------------
# UI: KPI Cards, Changes, Trends, Commentary, Downloads
# ----------------------------
def render_kpi_cards(df_slice: pd.DataFrame, kpis: List[str]):
    st.subheader("KPIs (Current Month)")
    if not kpis:
        st.info("No KPIs selected. Use the sidebar to choose P1/P2/P3 items.")
        return
    cols = st.columns(len(kpis))
    for i, metric in enumerate(kpis):
        sub = df_slice[df_slice["metric"] == metric]
        if sub.empty:
            with cols[i]:
                st.metric(metric, value="-", delta=None)
            continue
        unit = sub["unit"].dropna().iloc[0] if not sub["unit"].dropna().empty else None
        act = sub[(sub["scope"] == "ITM") & (sub["basis"] == "Act")]["value"]
        bud = sub[(sub["scope"] == "ITM") & (sub["basis"] == "vs BUD")]["value"]
        ly  = sub[(sub["scope"] == "ITM") & (sub["basis"] == "vs n-1")]["value"]

        act_val = act.iloc[0] if not act.empty else None
        bud_delta = _fmt_delta(float(bud.iloc[0]), unit) if not bud.empty else None
        ly_delta  = _fmt_delta(float(ly.iloc[0]), unit) if not ly.empty else None

        delta_str = " | ".join([x for x in [f"vs BUD: {bud_delta}" if bud_delta else None,
                                            f"vs n-1: {ly_delta}" if ly_delta else None]
                                 if x]) or None
        with cols[i]:
            st.metric(metric, value=_fmt_value(act_val, unit), delta=delta_str)

def render_changes_panel(df_slice: pd.DataFrame, kpis: List[str], level_thr: float, margin_thr: float):
    st.subheader("What Changed")
    rows = []
    for metric in kpis:
        sub = df_slice[(df_slice["metric"] == metric) & (df_slice["scope"] == "ITM")]
        if sub.empty:
            continue
        unit = sub["unit"].dropna().iloc[0] if not sub["unit"].dropna().empty else None
        thr = margin_thr if _is_margin(metric, unit) else level_thr
        for basis, label in [("vs BUD", "Budget"), ("vs n-1", "Last Year")]:
            vals = sub[sub["basis"] == basis]["value"]
            if vals.empty:
                continue
            val = float(vals.iloc[0])
            if abs(val) >= thr:
                direction = ("Above" if val > 0 else "Below") if basis == "vs BUD" else ("Up" if val > 0 else "Down")
                rows.append({
                    "metric": metric,
                    "variance": _fmt_delta(val, unit),
                    "against": label,
                    "direction": direction,
                    "abs": abs(val)
                })
    if rows:
        changed = pd.DataFrame(rows).sort_values(by="abs", ascending=False).drop(columns=["abs"])
        st.dataframe(changed, width="stretch")
    else:
        st.info("No significant changes for the selected KPIs and thresholds.")

def render_trends(markets: List[str], kpis: List[str], engine: str,
                  weight_metric: Optional[str], weight_scope: str, weight_basis: str,
                  global_view: bool):
    with st.expander("Trends (last 12 months)", expanded=False):
        if engine == "Plotly":
            try:
                import plotly.express as px
            except Exception:
                st.error("Plotly not installed. Run: pip install plotly")
                return

        for metric in kpis:
            if global_view and len(markets) > 1:
                agg = aggregate_history(markets, metric, weight_metric, weight_scope, weight_basis)
                if agg.empty:
                    continue
                agg["month_end"] = pd.to_datetime(agg["month_end"])
                st.caption(f"{metric} — GLOBAL")
                if engine == "Plotly":
                    fig = px.line(agg, x="month_end", y="value")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.line_chart(agg.set_index("month_end")[["value"]])
            else:
                mkt = markets[0]
                hist = get_history(mkt, metric, months=12)
                if hist.empty:
                    continue
                hist["month_end"] = pd.to_datetime(hist["month_end"])
                st.caption(f"{metric} — {mkt}")
                if engine == "Plotly":
                    fig = px.line(hist, x="month_end", y="value")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.line_chart(hist.set_index("month_end")[["value"]])

def render_commentary_panel(market_label: str, month_end: str, df_slice: pd.DataFrame,
                            kpis: List[str], level_thr: float, margin_thr: float):
    st.subheader("Commentary")

    # Auto commentary (as before)
    auto_comments = generate_cfo_commentary_enhanced(df_slice, market_label, month_end, level_thr, margin_thr, kpis)
    if auto_comments:
        st.markdown("**Auto Commentary**")
        for c in auto_comments:
            st.write("-", c)
        if st.checkbox("Persist auto commentary to repository", value=False):
            if st.button("Save Auto Commentary"):
                for c in auto_comments:
                    metric = next((m for m in kpis if m.lower() in c.lower()), "General")
                    score, passed = validate_commentary(c, metric)
                    store_commentary(market_label, month_end, metric, c, "Auto", score, passed, accepted=1)
                st.success("Auto commentary saved.")

    st.markdown("---")
    st.markdown("**Add Manual Commentary**")
    metric = st.selectbox("Metric", kpis, key="manual_metric")
    text = st.text_area("Commentary", key="manual_text")
    author = st.text_input("Author", key="manual_author", value="")
    if st.button("Submit Commentary"):
        score, passed = validate_commentary(text, metric)
        store_commentary(market_label, month_end, metric, text, author or "User", score, passed, accepted=0)
        st.success(f"Saved. Score: {score:.1f} — Pass: {passed}")

    st.markdown("---")
    st.markdown("**Commentary Manager (edit / accept / reject)**")
    saved = load_commentary(market_label, month_end)
    if not saved.empty:
        edited = st.data_editor(
            saved,
            num_rows="dynamic",
            column_config={
                "id": st.column_config.NumberColumn(disabled=True),
                "passed": st.column_config.CheckboxColumn(),
                "accepted": st.column_config.CheckboxColumn()
            },
            width="stretch",
        )
        if st.button("Apply Changes"):
            save_commentary_edits(edited)
            st.success("Commentary updated.")
    else:
        st.info("No saved commentary yet for this selection.")

def render_downloads(df_slice: pd.DataFrame, market_label: str, month_end: str):
    st.subheader("Data & Exports")
    if not df_slice.empty:
        st.download_button(
            "Download current selection CSV",
            data=df_slice.to_csv(index=False).encode("utf-8"),
            file_name=f"{market_label}_{month_end}_slice.csv",
            mime="text/csv",
        )
    with sqlite3.connect(FINANCE_DB) as conn:
        all_fin = pd.read_sql("SELECT * FROM financials", conn)
    if not all_fin.empty:
        st.download_button(
            "Download full financials CSV",
            data=all_fin.to_csv(index=False).encode("utf-8"),
            file_name="financials_full.csv",
            mime="text/csv",
        )


# ----------------------------
# Streamlit app
# ----------------------------
def run_streamlit() -> None:
    st.set_page_config(page_title="Finance Dashboard", layout="wide")
    st.title("Finance Dashboard")
    init_dbs()

    # Upload
    st.header("Upload Excel")
    uploaded = st.file_uploader("Upload financial report (e.g., SG_Aug2025.xlsx)", type=["xlsx"])
    if uploaded is not None:
        try:
            df = ingest_excel_upload(uploaded, uploaded.name)
            store_financials(df)
            st.success(f"Data ingested for {df['market'].iloc[0]} — {df['month_end'].iloc[0]}")
        except Exception as e:
            st.error(str(e))

    st.markdown("---")

    # Filters
    st.header("Current View")
    markets_all = get_markets()
    if not markets_all:
        st.info("No data yet. Upload a file to begin.")
        return

    col_a, col_b = st.columns([2, 2])
    with col_a:
        markets = st.multiselect("Markets (multi-select up to 50)", options=markets_all,
                                 default=[markets_all[0]], max_selections=50)
    with col_b:
        months = get_months_for_markets(markets) if markets else []
        month_end = st.selectbox("Month", months, index=len(months) - 1 if months else 0)

    if not markets or not month_end:
        st.info("Select at least one market and a month.")
        return

    # Sidebar global/weight options
    st.sidebar.header("Display & Thresholds")
    global_view = st.sidebar.checkbox("Combine selected markets as GLOBAL view", value=(len(markets) > 1))
    weight_scope = st.sidebar.selectbox("Weight scope (for %/rate)", ["ITM", "YTD"], index=0)
    weight_basis = st.sidebar.selectbox("Weight basis (for %/rate)", ["Act"], index=0)
    weight_metric = st.sidebar.text_input("Weight metric name", value="Revenue")
    level_thr = st.sidebar.number_input("Threshold (level metrics)", value=5.0, step=0.5)
    margin_thr = st.sidebar.number_input("Threshold (margin metrics, pp)", value=0.5, step=0.1)
    chart_engine = st.sidebar.selectbox("Chart Engine", ["Streamlit", "Plotly"])

    # Build slice (single or global)
    if len(markets) == 1 and not global_view:
        df_slice = get_slice(markets[0], month_end)
        market_label = markets[0]
    else:
        raw = get_slice_multi(markets, month_end)
        df_slice = aggregate_slice(raw, markets, month_end, weight_metric, weight_scope, weight_basis) if global_view else raw
        market_label = "GLOBAL" if global_view else "/".join(markets)

    if df_slice.empty:
        st.warning("No data for this selection.")
        return

    # KPI priorities selector → final KPI list
    kpis = kpi_priority_selector(df_slice)

    # Missing actuals banner
    missing = [m for m in kpis if df_slice[(df_slice["metric"] == m) &
                                           (df_slice["scope"] == "ITM") &
                                           (df_slice["basis"] == "Act")].empty]
    if missing:
        st.warning(f"Missing ITM Actuals for: {', '.join(missing)}")

    # Sections
    render_kpi_cards(df_slice, kpis)
    render_changes_panel(df_slice, kpis, level_thr, margin_thr)
    render_trends(markets, kpis, chart_engine, weight_metric, weight_scope, weight_basis, global_view)
    render_commentary_panel(market_label, month_end, df_slice, kpis, level_thr, margin_thr)
    render_downloads(df_slice, market_label, month_end)


# ----------------------------
# CLI
# ----------------------------
def cli_ingest(args: argparse.Namespace) -> None:
    init_dbs()
    df = ingest_excel_path(args.file)
    store_financials(df)
    print("Ingested:", args.file)

def cli_comment(args: argparse.Namespace) -> None:
    init_dbs()
    text = " ".join(args.text)
    score, passed = validate_commentary(text, args.metric)
    store_commentary(args.market, args.month_end, args.metric, text, args.author, score, passed, accepted=0)
    print(f"Saved. Score: {score:.1f} — Pass: {passed}")

def main() -> None:
    if len(sys.argv) == 1:
        run_streamlit()
        return
    parser = argparse.ArgumentParser(description="Finance dashboard CLI")
    sub = parser.add_subparsers(dest="command")
    p_ingest = sub.add_parser("ingest", help="Ingest an Excel file")
    p_ingest.add_argument("file", help="Path to Excel file")
    p_ingest.set_defaults(func=cli_ingest)
    p_comment = sub.add_parser("comment", help="Store a commentary")
    p_comment.add_argument("market"); p_comment.add_argument("month_end")
    p_comment.add_argument("metric"); p_comment.add_argument("author")
    p_comment.add_argument("text", nargs="+")
    p_comment.set_defaults(func=cli_comment)
    args = parser.parse_args()
    if hasattr(args, "func"):
        args.func(args)

if __name__ == "__main__":
    main()
