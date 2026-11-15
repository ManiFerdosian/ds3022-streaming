"""Prefect flow: raw Parquet -> DuckDB (two kinds of duplicates removed) -> one chart.

  python flow.py                 # all of data/raw
  python flow.py data/sample     # the committed sample: its own database and chart
"""
import os
import sys

import duckdb
import matplotlib

matplotlib.use("Agg")  # draw to a file, no window
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from prefect import flow, get_run_logger, task

SRC = sys.argv[1] if len(sys.argv) > 1 else "data/raw"
RAW = SRC.rstrip("/\\") + "/*.parquet"
NAME = os.path.basename(SRC.rstrip("/\\"))  # "raw" or "sample"
TAG = "" if NAME == "raw" else "_" + NAME
DB = f"coinbase{TAG}.duckdb"  # coinbase_sample.duckdb for the sample
CHART = f"charts/btc_buy_pressure{TAG}.png"  # the sample never overwrites my chart


@task(retries=2, retry_delay_seconds=5)
def load_raw():
    with duckdb.connect(DB) as con:  # the flow is the ONLY writer of the database
        con.execute(f"CREATE OR REPLACE TABLE raw_trades AS SELECT * FROM read_parquet('{RAW}')")
        return con.execute("SELECT count(*) FROM raw_trades").fetchone()[0]


@task
def stage():
    """Kind 1: the same Kafka record landed twice. DISTINCT keeps one copy."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE stg_records AS
            SELECT DISTINCT * EXCLUDE (landed_at)
            FROM raw_trades
            WHERE action <> 'malformed'
              AND msg_type = 'match'  -- not subscriptions or last_match
              AND product_id IS NOT NULL AND trade_id IS NOT NULL
        """)
        q = lambda sql: con.execute(sql).fetchone()[0]
        return {"malformed": q("SELECT count(*) FROM raw_trades WHERE action = 'malformed'"),
                "records": q("SELECT count(*) FROM stg_records")}


@task
def entities():
    """Kind 2: the same trade sent twice. One row per (product_id, trade_id)."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE trades AS
            SELECT product_id, trade_id, side, price, size,
                   CAST(event_time AS TIMESTAMP) AS event_time,  -- event time, UTC
                   CAST(raw::JSON ->> '$.data.sequence' AS BIGINT) AS sequence  -- parsed from raw
            FROM stg_records
            QUALIFY row_number() OVER (PARTITION BY product_id, trade_id
                                       ORDER BY kafka_offset DESC) = 1
        """)
        return con.execute("SELECT count(*) FROM trades").fetchone()[0]


@task
def marts():
    with duckdb.connect(DB) as con:
        # side is the maker side, so side = 'sell' means the taker bought
        con.execute("""
            CREATE OR REPLACE TABLE mart_btc_pressure AS
            SELECT time_bucket(INTERVAL '1 minute', event_time) AS minute,
                   count(*) AS trades,
                   round(100.0 * count(*) FILTER (WHERE side = 'sell') / count(*), 1) AS taker_buy_pct,
                   round(avg(price), 2) AS avg_price
            FROM trades
            WHERE product_id = 'BTC-USD'
            GROUP BY minute
            ORDER BY minute
        """)


@task
def chart():
    with duckdb.connect(DB, read_only=True) as con:
        rows = con.execute("SELECT minute, taker_buy_pct, avg_price FROM mart_btc_pressure").fetchall()
    os.makedirs("charts", exist_ok=True)
    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.plot([r[0] for r in rows], [r[1] for r in rows], marker="o", color="#2a7fb8", label="taker buys (% of trades)")
    ax.axhline(50, color="grey", linestyle="--", linewidth=1)
    ax.set_ylabel("taker buys (% of BTC-USD trades)")
    ax.set_xlabel("minute (UTC, event time, 15 Nov 2026)")
    ax2 = ax.twinx()
    ax2.plot([r[0] for r in rows], [r[2] for r in rows], color="#e07b39", label="avg price (USD)")
    ax2.set_ylabel("average BTC-USD price (USD)")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    fig.legend(loc="upper left", bbox_to_anchor=(0.1, 0.9))
    plt.title("BTC-USD taker buys stay near 50% and barely follow the price (Coinbase)")
    plt.tight_layout()
    plt.savefig(CHART, dpi=120)
    plt.close()
    return CHART


@flow(name="coinbase-pipeline")
def pipeline():
    log = get_run_logger()
    raw = load_raw()
    n = stage()
    things = entities()
    marts()
    path = chart()
    log.info("Raw records landed:        %d", raw)
    log.info("Malformed (kept in raw):   %d", n["malformed"])
    log.info("Unique Kafka records:      %d", n["records"])
    log.info("Unique trades:             %d", things)
    log.info("Chart: %s", path)


if __name__ == "__main__":
    pipeline()
