from pyspark import pipelines as dp
from pyspark.sql.functions import col
import re

CATALOG = spark.conf.get("catalog", "stock_catalog")
SCHEMA = spark.conf.get("schema", "dev")

VOL_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"

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
# SILVER — instruments_current, SCD Type 2 via AUTO CDC FROM SNAPSHOT
# Reads each timestamped instrument-master snapshot file, in order
# ============================================================

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