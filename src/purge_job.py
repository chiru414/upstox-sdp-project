import sys
import datetime
from databricks.sdk.runtime import dbutils

CATALOG = sys.argv[1] if len(sys.argv) > 1 else "stock_catalog"
SCHEMA = sys.argv[2] if len(sys.argv) > 2 else "dev"
RETENTION_DAYS = int(sys.argv[3]) if len(sys.argv) > 3 else 7

VOL_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"
CUTOFF = datetime.datetime.utcnow() - datetime.timedelta(days=RETENTION_DAYS)

def purge_old_files(directory):
    deleted = 0
    for f in dbutils.fs.ls(directory):
        if f.name.endswith("_schema") or f.name.endswith("_schema/"):
            continue
        mod_time = datetime.datetime.utcfromtimestamp(f.modificationTime / 1000)
        if mod_time < CUTOFF:
            dbutils.fs.rm(f.path, recurse=True)
            deleted += 1
    print(f"{directory}: deleted {deleted} file(s) older than {RETENTION_DAYS} days")

if __name__ == "__main__":
    for subdir in ["instruments", "candles_daily", "candles_hourly"]:
        purge_old_files(f"{VOL_ROOT}/{subdir}")

    spark.sql(f"""
        DELETE FROM {CATALOG}.{SCHEMA}.candles_daily_quarantine
        WHERE CAST(_ingested_at AS TIMESTAMP) < TIMESTAMP'{CUTOFF.isoformat()}'
    """)
    print("Quarantine table purge complete.")

    spark.sql(f"VACUUM {CATALOG}.{SCHEMA}.candles_daily_silver RETAIN 168 HOURS")
    spark.sql(f"VACUUM {CATALOG}.{SCHEMA}.instruments_current RETAIN 168 HOURS")
    print("VACUUM complete — physically removed old file versions.")