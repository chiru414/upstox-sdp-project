from pyspark import pipelines as dp
import re
from pyspark.sql.functions import (
    col, lag, rank, row_number, round as sql_round,
    to_date, when, lit, count
)
from pyspark.sql.window import Window

CATALOG = spark.conf.get("catalog", "stock_catalog")
SCHEMA = spark.conf.get("schema", "dev")
WATCHLIST = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "SBIN"]

VOL_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"

def _underlying_symbol_expr():
    expr = None
    for w in WATCHLIST:
        cond = col("tradingsymbol").startswith(w)
        expr = when(cond, lit(w)) if expr is None else expr.when(cond, lit(w))
    return expr

# ============================================================
# BRONZE — candles (daily + hourly), via Auto Loader
# ============================================================
CANDLES_SCHEMA = "status STRING, data STRUCT<candles: ARRAY<ARRAY<STRING>>>, _symbol STRING, _ingested_at STRING"

@dp.table(name="candles_daily_bronze")
def candles_daily_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{VOL_ROOT}/candles_daily/_schema")
        .schema(CANDLES_SCHEMA)
        .load(f"{VOL_ROOT}/candles_daily")
    )

@dp.table(name="candles_hourly_bronze")
def candles_hourly_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{VOL_ROOT}/candles_hourly/_schema")
        .schema(CANDLES_SCHEMA)
        .load(f"{VOL_ROOT}/candles_hourly")
    )

# ============================================================
# QUARANTINE — candle responses with no actual candle data
# (e.g. non-trading days, or a symbol lookup that failed)
# ============================================================

@dp.table(name="candles_daily_clean")
@dp.expect_or_drop("has_candles", "size(data.candles) > 0")
def candles_daily_clean():
    return spark.readStream.table("candles_daily_bronze")

@dp.table(name="candles_daily_quarantine")
def candles_daily_quarantine():
    return (
        spark.readStream.table("candles_daily_bronze")
        .where("size(data.candles) = 0 OR data.candles IS NULL")
    )

# ============================================================
# QUARANTINE — hourly candles
# ============================================================
@dp.table(name="candles_hourly_clean")
@dp.expect_or_drop("has_candles", "size(data.candles) > 0")
def candles_hourly_clean():
    return spark.readStream.table("candles_hourly_bronze")

@dp.table(name="candles_hourly_quarantine")
def candles_hourly_quarantine():
    return (
        spark.readStream.table("candles_hourly_bronze")
        .where("size(data.candles) = 0 OR data.candles IS NULL")
    )

# ============================================================
# SILVER — instruments_current, SCD Type 2 via AUTO CDC FROM SNAPSHOT
# Reads each timestamped instrument-master snapshot file, in order
# ============================================================

@dp.table(name="candles_daily_silver")
def candles_daily_silver():
    from pyspark.sql.functions import explode, col

    df = spark.readStream.table("candles_daily_clean")
    exploded = df.select(
        col("_symbol").alias("symbol"),
        col("_ingested_at").alias("ingested_at"),
        explode(col("data.candles")).alias("candle")
    )
    return exploded.select(
        col("symbol"),
        col("ingested_at"),
        col("candle")[0].cast("timestamp").alias("candle_ts"),
        col("candle")[1].cast("double").alias("open"),
        col("candle")[2].cast("double").alias("high"),
        col("candle")[3].cast("double").alias("low"),
        col("candle")[4].cast("double").alias("close"),
        col("candle")[5].cast("long").alias("volume"),
        col("candle")[6].cast("long").alias("open_interest"),
    )

def next_snapshot_and_version(latest_snapshot_version):
    files = dbutils.fs.ls(f"{VOL_ROOT}/instruments/")
    versions = []
    for f in files:
        m = re.search(r"instruments_(\d{8}T\d{6})\.json", f.name)
        if m:
            versions.append((m.group(1), f.path))
    versions.sort(key=lambda x: x[0])

    for version, path in versions:
        if latest_snapshot_version is None or version > latest_snapshot_version:
            df = spark.read.option("multiLine", "true").json(path)
            return (df, version)
    return None

dp.create_streaming_table(
    name="instruments_current",
    schema="""
        instrument_key   STRING,
        exchange_token   STRING,
        tradingsymbol    STRING,
        name             STRING,
        last_price       STRING,
        expiry           STRING,
        strike           STRING,
        tick_size        STRING,
        lot_size         STRING,
        instrument_type  STRING,
        option_type      STRING,
        exchange         STRING,
        __START_AT       STRING,
        __END_AT         STRING
    """
)

dp.create_auto_cdc_from_snapshot_flow(
    target="instruments_current",
    source=next_snapshot_and_version,
    keys=["instrument_key"],
    stored_as_scd_type=2,
)

# ============================================================
# SILVER — hourly candles, flattened
# ============================================================

@dp.table(name="candles_hourly_silver")
def candles_hourly_silver():
    from pyspark.sql.functions import explode, col
    df = spark.readStream.table("candles_hourly_clean")
    exploded = df.select(
        col("_symbol").alias("symbol"),
        col("_ingested_at").alias("ingested_at"),
        explode(col("data.candles")).alias("candle")
    )
    return exploded.select(
        col("symbol"),
        col("ingested_at"),
        col("candle")[0].cast("timestamp").alias("candle_ts"),
        col("candle")[1].cast("double").alias("open"),
        col("candle")[2].cast("double").alias("high"),
        col("candle")[3].cast("double").alias("low"),
        col("candle")[4].cast("double").alias("close"),
        col("candle")[5].cast("long").alias("volume"),
        col("candle")[6].cast("long").alias("open_interest"),
    )

# ============================================================
# GOLD 1 — daily % move, ranked per day (window function objective)
# ============================================================
@dp.materialized_view(name="symbol_daily_performance_gold")
def symbol_daily_performance_gold():
    df = spark.read.table("candles_daily_silver")
    w_lag = Window.partitionBy("symbol").orderBy("candle_ts")
    with_prev = df.withColumn("prev_close", lag("close").over(w_lag))
    with_pct = with_prev.withColumn(
        "pct_change",
        sql_round(((col("close") - col("prev_close")) / col("prev_close")) * 100, 2)
    )
    w_rank = Window.partitionBy("candle_ts").orderBy(col("pct_change").desc())
    return with_pct.withColumn("daily_rank", rank().over(w_rank)).select(
        "symbol", "candle_ts", "close", "prev_close", "pct_change", "daily_rank"
    )

# ============================================================
# GOLD 2 — F&O contracts enriched with underlying's point-in-time price
# (real SCD2 point-in-time join, no account data)
# ============================================================
@dp.materialized_view(name="fo_contracts_enriched_gold")
def fo_contracts_enriched_gold():
    fo = (
        spark.read.table("instruments_current")
        .filter(col("exchange") == "NSE_FO")
        .withColumn("underlying_symbol", _underlying_symbol_expr())
        .withColumn("snapshot_date", to_date(col("__START_AT"), "yyyyMMdd'T'HHmmss"))
    )
    candles = (
        spark.read.table("candles_daily_silver")
        .withColumn("trade_date", to_date(col("candle_ts")))
    )
    joined = fo.join(
        candles,
        (fo.underlying_symbol == candles.symbol) & (candles.trade_date <= fo.snapshot_date),
        "left"
    )
    w = Window.partitionBy("instrument_key").orderBy(col("trade_date").desc())
    ranked = joined.withColumn("rn", row_number().over(w)).filter(col("rn") == 1)
    return ranked.select(
        col("instrument_key"), col("tradingsymbol"), col("underlying_symbol"),
        col("expiry"), col("strike"), col("option_type"),
        col("trade_date").alias("underlying_price_date"),
        col("close").alias("underlying_close_price"),
    )

# ============================================================
# GOLD 3 — active F&O contract counts per underlying (SCD2 as a metric)
# ============================================================
@dp.materialized_view(name="instrument_churn_gold")
def instrument_churn_gold():
    fo = (
        spark.read.table("instruments_current")
        .filter((col("exchange") == "NSE_FO") & col("__END_AT").isNull())
        .withColumn("underlying_symbol", _underlying_symbol_expr())
    )
    return fo.groupBy("underlying_symbol", "__START_AT").agg(
        count("*").alias("active_contract_count")
    ).orderBy("underlying_symbol", "__START_AT")

# ============================================================
# GOLD — intraday volatility (rolling window, distinct from
# the daily Gold's RANK-based approach — variety on purpose)
# ============================================================
@dp.materialized_view(name="intraday_volatility_gold")
def intraday_volatility_gold():
    from pyspark.sql.functions import stddev, avg, col
    from pyspark.sql.window import Window

    df = spark.read.table("candles_hourly_silver")
    w = Window.partitionBy("symbol").orderBy("candle_ts").rowsBetween(-5, 0)

    return df.withColumn(
        "rolling_avg_close_6h", avg("close").over(w)
    ).withColumn(
        "rolling_volatility_6h", stddev("close").over(w)
    ).select(
        "symbol", "candle_ts", "close",
        "rolling_avg_close_6h", "rolling_volatility_6h"
    )