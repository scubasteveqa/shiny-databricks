"""
Shiny for Python – Databricks NYC Taxi Explorer
=================================================
Connects to Databricks via databricks-sql-connector and queries
the samples.nyctaxi.trips table.

Prerequisites:
    pip install shiny databricks-sql-connector pandas plotly

Environment variables (set before running):
    DATABRICKS_HOST          – e.g. adb-123456789.12.azuredatabricks.net
    DATABRICKS_HTTP_PATH     – e.g. /sql/1.0/warehouses/abc123
    DATABRICKS_ACCESS_TOKEN  – your personal access token
"""

from __future__ import annotations

import os
from datetime import date

import pandas as pd
import plotly.express as px
from shiny import App, reactive, render, ui

# ── Databricks connection helper ─────────────────────────────────────────────

def get_databricks_connection():
    """Return a fresh DBSQL connection using env vars."""
    from databricks import sql as dbsql

    return dbsql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_ACCESS_TOKEN"],
    )


def query_trips(
    start_date: str,
    end_date: str,
    limit: int = 5_000,
) -> pd.DataFrame:
    """Fetch taxi trips between two dates."""
    sql = f"""
        SELECT
            tpep_pickup_datetime   AS pickup_time,
            tpep_dropoff_datetime  AS dropoff_time,
            trip_distance,
            fare_amount,
            tip_amount,
            total_amount,
            pickup_zip,
            dropoff_zip
        FROM samples.nyctaxi.trips
        WHERE tpep_pickup_datetime >= '{start_date}'
          AND tpep_pickup_datetime <  '{end_date}'
        LIMIT {limit}
    """
    conn = get_databricks_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
        return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()


# ── UI ───────────────────────────────────────────────────────────────────────

CUSTOM_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,400;0,9..40,600;0,9..40,700;1,9..40,400&family=JetBrains+Mono:wght@400;600&display=swap');

:root {
    --bg:        #0f1117;
    --surface:   #181b24;
    --border:    #2a2e3a;
    --text:      #e4e6ed;
    --muted:     #8b8fa3;
    --accent:    #f5c542;
    --accent2:   #42c9f5;
    --danger:    #f5425a;
    --success:   #42f5a7;
    --radius:    10px;
}

* { box-sizing: border-box; }

body {
    font-family: 'DM Sans', sans-serif;
    background: var(--bg);
    color: var(--text);
    margin: 0;
    padding: 0;
}

h2, h3, h4 { margin: 0; font-weight: 700; }

/* Sidebar */
.sidebar-panel {
    background: var(--surface) !important;
    border-right: 1px solid var(--border) !important;
    padding: 1.5rem !important;
}

.sidebar-panel label {
    color: var(--muted);
    font-size: 0.78rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    font-weight: 600;
    margin-bottom: 4px;
}

/* Inputs */
.form-control, .shiny-date-input input {
    background: var(--bg) !important;
    border: 1px solid var(--border) !important;
    color: var(--text) !important;
    border-radius: var(--radius) !important;
    padding: 8px 12px !important;
    font-family: 'DM Sans', sans-serif !important;
}

.form-control:focus {
    border-color: var(--accent) !important;
    box-shadow: 0 0 0 2px rgba(245,197,66,0.15) !important;
}

/* Action button */
.btn-default {
    background: var(--accent) !important;
    color: var(--bg) !important;
    border: none !important;
    border-radius: var(--radius) !important;
    font-weight: 700 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.06em !important;
    padding: 10px 20px !important;
    cursor: pointer;
    transition: transform 0.15s, box-shadow 0.15s;
    width: 100%;
    font-family: 'DM Sans', sans-serif !important;
}

.btn-default:hover {
    transform: translateY(-1px);
    box-shadow: 0 4px 20px rgba(245,197,66,0.25) !important;
}

/* Stat cards */
.stat-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 1.2rem 1.4rem;
    flex: 1;
    min-width: 150px;
    transition: border-color 0.2s;
}
.stat-card:hover { border-color: var(--accent); }
.stat-label {
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: var(--muted);
    margin-bottom: 4px;
    font-weight: 600;
}
.stat-value {
    font-family: 'JetBrains Mono', monospace;
    font-size: 1.6rem;
    font-weight: 600;
    color: var(--accent);
}
.stat-value.blue  { color: var(--accent2); }
.stat-value.green { color: var(--success); }
.stat-value.red   { color: var(--danger); }

/* Chart containers */
.chart-box {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 1rem;
    margin-top: 1rem;
}
.chart-box h4 {
    font-size: 0.85rem;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: var(--muted);
    margin-bottom: 0.6rem;
}

/* Header */
.app-header {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    margin-bottom: 1rem;
}
.app-header .icon {
    width: 36px; height: 36px;
    background: var(--accent);
    border-radius: 8px;
    display: flex; align-items: center; justify-content: center;
    font-size: 1.1rem;
}
.app-header h2 {
    font-size: 1.25rem;
    letter-spacing: -0.02em;
}

.status-badge {
    display: inline-block;
    font-size: 0.7rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    padding: 3px 10px;
    border-radius: 100px;
    background: rgba(66,245,167,0.12);
    color: var(--success);
    margin-top: 1rem;
}

/* Table */
.dataframe { width: 100%; border-collapse: collapse; }
.dataframe th {
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--muted);
    font-weight: 600;
    text-align: left;
    padding: 8px 10px;
    border-bottom: 1px solid var(--border);
}
.dataframe td {
    padding: 7px 10px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.82rem;
    border-bottom: 1px solid var(--border);
    color: var(--text);
}
.dataframe tbody tr:hover { background: rgba(245,197,66,0.04); }

/* Plotly dark overrides */
.js-plotly-plot .plotly .modebar { display: none !important; }
</style>
"""

app_ui = ui.page_sidebar(
    ui.sidebar(
        ui.HTML("""
        <div style="margin-bottom:1.2rem;">
            <div class="app-header">
                <div class="icon">🚕</div>
                <h2>NYC Taxi Explorer</h2>
            </div>
            <p style="color:var(--muted);font-size:0.82rem;margin:0;">
                Query <code style="color:var(--accent);background:rgba(245,197,66,0.08);
                padding:2px 6px;border-radius:4px;font-size:0.78rem;">
                samples.nyctaxi.trips</code> via Databricks SQL Connector.
            </p>
        </div>
        """),
        ui.input_date("start_date", "Start Date", value="2016-01-01"),
        ui.input_date("end_date", "End Date", value="2016-01-08"),
        ui.input_numeric("limit", "Row Limit", value=5000, min=100, max=50_000, step=500),
        ui.input_action_button("go", "Fetch Trips"),
        ui.HTML('<div class="status-badge" id="conn-badge">⚡ Databricks connected</div>'),
        width=300,
    ),
    # ── Main panel ──
    ui.HTML(CUSTOM_CSS),
    ui.output_ui("stat_cards"),
    ui.div(
        ui.div(
            ui.HTML('<h4>Fare Distribution</h4>'),
            ui.output_ui("plot_fare"),
            class_="chart-box",
            style="flex:1;min-width:0;",
        ),
        ui.div(
            ui.HTML('<h4>Distance vs Fare</h4>'),
            ui.output_ui("plot_scatter"),
            class_="chart-box",
            style="flex:1;min-width:0;",
        ),
        style="display:flex;gap:1rem;flex-wrap:wrap;",
    ),
    ui.div(
        ui.div(
            ui.HTML('<h4>Trips by Hour of Day</h4>'),
            ui.output_ui("plot_hourly"),
            class_="chart-box",
            style="flex:1;min-width:0;",
        ),
        ui.div(
            ui.HTML('<h4>Tip % Distribution</h4>'),
            ui.output_ui("plot_tip"),
            class_="chart-box",
            style="flex:1;min-width:0;",
        ),
        style="display:flex;gap:1rem;flex-wrap:wrap;margin-bottom:1rem;",
    ),
    ui.div(
        ui.HTML('<h4>Sample Rows</h4>'),
        ui.output_ui("table_preview"),
        class_="chart-box",
    ),
    title="NYC Taxi Explorer",
    fillable=True,
)


# ── Server ───────────────────────────────────────────────────────────────────

PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="DM Sans", color="#8b8fa3", size=12),
    margin=dict(l=40, r=20, t=10, b=40),
    xaxis=dict(gridcolor="#2a2e3a", zerolinecolor="#2a2e3a"),
    yaxis=dict(gridcolor="#2a2e3a", zerolinecolor="#2a2e3a"),
    height=280,
)


def server(input, output, session):

    df_store: reactive.Value[pd.DataFrame | None] = reactive.Value(None)

    @reactive.Effect
    @reactive.event(input.go)
    def fetch():
        start = str(input.start_date())
        end = str(input.end_date())
        limit = int(input.limit())
        df = query_trips(start, end, limit)
        df_store.set(df)

    # ── Stat cards ──

    @output
    @render.ui
    def stat_cards():
        df = df_store()
        if df is None or df.empty:
            return ui.HTML(
                '<p style="color:var(--muted);padding:2rem 0;">'
                "Click <b>Fetch Trips</b> to load data from Databricks.</p>"
            )
        n = len(df)
        avg_fare = df["fare_amount"].mean()
        avg_dist = df["trip_distance"].mean()
        avg_tip = df["tip_amount"].mean()
        return ui.HTML(f"""
        <div style="display:flex;gap:0.8rem;flex-wrap:wrap;margin-bottom:0.4rem;">
            <div class="stat-card">
                <div class="stat-label">Total Trips</div>
                <div class="stat-value">{n:,}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Avg Fare</div>
                <div class="stat-value blue">${avg_fare:,.2f}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Avg Distance</div>
                <div class="stat-value green">{avg_dist:,.2f} mi</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Avg Tip</div>
                <div class="stat-value red">${avg_tip:,.2f}</div>
            </div>
        </div>
        """)

    # ── Charts ──

    @output
    @render.ui
    def plot_fare():
        df = df_store()
        if df is None or df.empty:
            return ui.HTML("")
        fig = px.histogram(
            df, x="fare_amount", nbins=50,
            color_discrete_sequence=["#f5c542"],
        )
        fig.update_layout(**PLOTLY_LAYOUT)
        fig.update_xaxes(title_text="Fare ($)")
        fig.update_yaxes(title_text="Count")
        return ui.HTML(fig.to_html(full_html=False, include_plotlyjs="cdn"))

    @output
    @render.ui
    def plot_scatter():
        df = df_store()
        if df is None or df.empty:
            return ui.HTML("")
        sample = df.sample(min(1000, len(df)), random_state=42)
        fig = px.scatter(
            sample, x="trip_distance", y="fare_amount",
            color="tip_amount",
            color_continuous_scale=["#181b24", "#42c9f5", "#f5c542"],
            opacity=0.6,
        )
        fig.update_layout(**PLOTLY_LAYOUT)
        fig.update_xaxes(title_text="Distance (mi)")
        fig.update_yaxes(title_text="Fare ($)")
        return ui.HTML(fig.to_html(full_html=False, include_plotlyjs="cdn"))

    @output
    @render.ui
    def plot_hourly():
        df = df_store()
        if df is None or df.empty:
            return ui.HTML("")
        hrs = pd.to_datetime(df["pickup_time"]).dt.hour.value_counts().sort_index()
        fig = px.bar(
            x=hrs.index, y=hrs.values,
            color_discrete_sequence=["#42f5a7"],
            labels={"x": "Hour", "y": "Trips"},
        )
        fig.update_layout(**PLOTLY_LAYOUT)
        return ui.HTML(fig.to_html(full_html=False, include_plotlyjs="cdn"))

    @output
    @render.ui
    def plot_tip():
        df = df_store()
        if df is None or df.empty:
            return ui.HTML("")
        tip_pct = (df["tip_amount"] / df["fare_amount"].replace(0, float("nan"))) * 100
        fig = px.histogram(
            tip_pct.dropna(), nbins=50,
            color_discrete_sequence=["#f5425a"],
            labels={"value": "Tip %"},
        )
        fig.update_layout(**PLOTLY_LAYOUT)
        fig.update_xaxes(title_text="Tip %", range=[0, 60])
        fig.update_yaxes(title_text="Count")
        return ui.HTML(fig.to_html(full_html=False, include_plotlyjs="cdn"))

    # ── Table ──

    @output
    @render.ui
    def table_preview():
        df = df_store()
        if df is None or df.empty:
            return ui.HTML("")
        return ui.HTML(
            df.head(20).to_html(
                index=False, classes="dataframe", border=0,
                float_format=lambda x: f"{x:,.2f}",
            )
        )


app = App(app_ui, server)
