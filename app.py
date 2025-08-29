# app.py
# Requirements: pandas>=2.2, streamlit>=1.36, openpyxl>=3.1.2
# Optional for charts: plotly>=5
import argparse
import os
import re
import sqlite3
import sys
from calendar import monthrange
from datetime import datetime
from typing import List, Tuple, Optional

import pandas as pd
import streamlit as st
from pandas.tseries.offsets import MonthEnd, DateOffset

FINANCE_DB = "finance.db"
COMMENTARY_DB = "commentary.db"

# Default “suggested” KPIs; users can add/remove from sidebar based on what exists
DEFAULT_KPIS = ["Revenue", "GM%", "EBITDA%", "Opex%", "DSO", "Capex"]
MARGIN_HINTS = {"GM%", "EBITDA%", "Opex%"}          # if name contains these, treat as margin-like
MARGIN_UNITS = {"%", "pts"}                          # if unit matches these, treat as margin-like

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
        # If column accepted didn’t exist, try to add it (no-op if already there)
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
        raise ValueError(
            "Filename must be '<MARKET>_<MonYYYY>.xlsx' (e.g., 'SG_Aug2025.xlsx')."
        )
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
    if isinstance(unit, str) and unit.strip() == "%":
        return f"{val:.2f} pp"
    if isinstance(unit, str) and unit.strip() in ("pts", "pt"):
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
# Excel ingestion tailored to your sheet
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
    """Reshape raw Excel dataframe to tidy format with required columns (robust for Japan_Sep2023.xlsx)."""
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

def get_months_for_market(market: str) -> List[str]:
    with sqlite3.connect(FINANCE_DB) as conn:
        df = pd.read_sql(
            "SELECT DISTINCT month_end FROM financials WHERE market = ? ORDER BY month_end",
            conn, params=(market,)
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
# Auto commentary (enhanced)
# ----------------------------
def generate_cfo_commentary_enhanced(df_slice: pd.DataFrame, market: str, month_end: str,
                                     level_thr: float, margin_thr: float,
                                     kpis: List[str]) -> List[str]:
    comments: List[str] = []
    dt = pd.to_datetime(month_end)
    prev_m = (dt - MonthEnd(1)).strftime("%Y-%m-%d")
    prev_y = (dt - DateOffset(years=1)).strftime("%Y-%m-%d")

    for metric in kpis:
        sub = df_slice[(df_slice["metric"] == metric)]
        if sub.empty:
            continue
        unit = sub["unit"].dropna().iloc[0] if not sub["unit"].dropna().empty else None
        thr = margin_thr if _is_margin(metric, unit) else level_thr

        # ITM comparisons available in slice
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

        # MoM from DB (Act ITM current vs previous month)
        act_now = get_value(market, month_end, metric, "ITM", "Act")
        act_prev = get_value(market, prev_m, metric, "ITM", "Act")
        if act_now is not None and act_prev is not None:
            mom = act_now - act_prev
            if abs(mom) >= thr:
                sign = "higher" if mom > 0 else "lower"
                comments.append(f"{metric} {sign} MoM by {_fmt_delta(mom, unit)} (ITM).")

        # YTD vs PY-YTD from DB/slice
        ytd_now = get_value(market, month_end, metric, "YTD", "Act")
        ytd_prev_y = get_value(market, prev_y, metric, "YTD", "Act")
        if ytd_now is not None and ytd_prev_y is not None:
            yoy_ytd = ytd_now - ytd_prev_y
            if abs(yoy_ytd) >= thr:
                sign = "up" if yoy_ytd > 0 else "down"
                comments.append(f"{metric} {sign} versus PY-YTD by {_fmt_delta(yoy_ytd, unit)} (YTD).")

        # 3-month trend (acceleration)
        hist = get_history(market, metric, months=3)
        if not hist.empty and len(hist) >= 3:
            first, last = float(hist["value"].iloc[0]), float(hist["value"].iloc[-1])
            trend = last - first
            if abs(trend) >= thr:
                direction = "accelerating" if trend > 0 else "decelerating"
                comments.append(f"{metric} shows {direction} over the last 3 months ({_fmt_delta(trend, unit)}).")

    return comments

# ----------------------------
# UI: KPI Cards, Changes, Trends, Commentary, Downloads
# ----------------------------
def render_kpi_cards(df_slice: pd.DataFrame, kpis: List[str]):
    st.subheader("KPIs (Current Month)")
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
        st.dataframe(changed, use_container_width=True)
    else:
        st.info("No significant changes for the selected KPIs and thresholds.")

def render_trends(market: str, kpis: List[str], engine: str):
    with st.expander("Trends (last 12 months)", expanded=False):
        if engine == "Plotly":
            try:
                import plotly.express as px
            except Exception as e:
                st.error("Plotly not installed. Run: pip install plotly")
                return
        rows = []
        for metric in kpis:
            hist = get_history(market, metric, months=12)
            if hist.empty:
                continue
            hist = hist.copy()
            hist["month_end"] = pd.to_datetime(hist["month_end"])
            hist["metric"] = metric
            rows.append(hist)
        if not rows:
            st.write("No history available yet.")
            return
        data = pd.concat(rows, ignore_index=True)
        for metric in kpis:
            sub = data[data["metric"] == metric]
            if sub.empty:
                continue
            st.caption(metric)
            if engine == "Plotly":
                fig = px.line(sub, x="month_end", y="value")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.line_chart(sub.set_index("month_end")[["value"]])

def render_commentary_panel(market: str, month_end: str, df_slice: pd.DataFrame,
                            kpis: List[str], level_thr: float, margin_thr: float):
    st.subheader("Commentary")

    auto_comments = generate_cfo_commentary_enhanced(df_slice, market, month_end, level_thr, margin_thr, kpis)
    if auto_comments:
        st.markdown("**Auto Commentary (generated)**")
        for c in auto_comments:
            st.write("-", c)
        if st.checkbox("Persist auto commentary to repository", value=False):
            if st.button("Save Auto Commentary"):
                for c in auto_comments:
                    metric = next((m for m in kpis if m.lower() in c.lower()), "General")
                    score, passed = validate_commentary(c, metric)
                    store_commentary(market, month_end, metric, c, "Auto", score, passed, accepted=1)
                st.success("Auto commentary saved.")

    st.markdown("---")
    st.markdown("**Add Manual Commentary**")
    metric = st.selectbox("Metric", kpis, key="manual_metric")
    text = st.text_area("Commentary", key="manual_text")
    author = st.text_input("Author", key="manual_author", value="")
    if st.button("Submit Commentary"):
        score, passed = validate_commentary(text, metric)
        store_commentary(market, month_end, metric, text, author or "User", score, passed, accepted=0)
        st.success(f"Saved. Score: {score:.1f} — Pass: {passed}")

    st.markdown("---")
    st.markdown("**Commentary Manager (edit / accept / reject)**")
    saved = load_commentary(market, month_end)
    if not saved.empty:
        edited = st.data_editor(
            saved,
            num_rows="dynamic",
            use_container_width=True,
            column_config={
                "id": st.column_config.NumberColumn(disabled=True),
                "passed": st.column_config.CheckboxColumn(),
                "accepted": st.column_config.CheckboxColumn()
            }
        )
        if st.button("Apply Changes"):
            save_commentary_edits(edited)
            st.success("Commentary updated.")
    else:
        st.info("No saved commentary yet for this month.")

def render_downloads(market: str, month_end: str):
    st.subheader("Data & Exports")
    this_slice = get_slice(market, month_end)
    if not this_slice.empty:
        st.download_button(
            "Download current month CSV",
            data=this_slice.to_csv(index=False).encode("utf-8"),
            file_name=f"{market}_{month_end}_slice.csv",
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
    st.header("Current Month View")
    markets = get_markets()
    if not markets:
        st.info("No data yet. Upload a file to begin.")
        return

    col_a, col_b = st.columns(2)
    with col_a:
        market = st.selectbox("Market", markets)
    months = get_months_for_market(market)
    with col_b:
        month_end = st.selectbox("Month", months, index=len(months) - 1 if months else 0)

    df_slice = get_slice(market, month_end)
    if df_slice.empty:
        st.warning("No data for this selection.")
        return

    # Sidebar options
    st.sidebar.header("Display & Thresholds")
    available_metrics = sorted(df_slice["metric"].unique().tolist())
    default_kpis = [m for m in DEFAULT_KPIS if m in available_metrics] or available_metrics[:6]
    kpis = st.sidebar.multiselect("KPIs to show", options=available_metrics, default=default_kpis)

    level_thr = st.sidebar.number_input("Threshold (level metrics)", value=5.0, step=0.5)
    margin_thr = st.sidebar.number_input("Threshold (margin metrics, pp)", value=0.5, step=0.1)
    chart_engine = st.sidebar.selectbox("Chart Engine", ["Streamlit", "Plotly"])

    # Missing actuals banner
    missing = [m for m in kpis if df_slice[(df_slice["metric"] == m) & (df_slice["scope"] == "ITM") & (df_slice["basis"] == "Act")].empty]
    if missing:
        st.warning(f"Missing ITM Actuals for: {', '.join(missing)}")

    # Sections
    render_kpi_cards(df_slice, kpis)
    render_changes_panel(df_slice, kpis, level_thr, margin_thr)
    render_trends(market, kpis, chart_engine)
    render_commentary_panel(market, month_end, df_slice, kpis, level_thr, margin_thr)
    render_downloads(market, month_end)

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
