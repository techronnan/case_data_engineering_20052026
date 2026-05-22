# Databricks notebook source
# DBTITLE 1,Landing Upload Sources - TXT


# Assuming Spark session and dbutils are available in the environment
# Reuse common variables from 00 script (SOURCES_PATH, LANDING_PATH, SOURCE_MAP, EXPECTED_FILES)

def process_txt(source_key, config):
    name = config["name"]
    fname = config["file"]
    opts = config.get("options", {})
    src_path = f"{SOURCES_PATH}/{fname}"
    dst_dir = f"{LANDING_PATH}/{name}/{datetime.now().strftime('%Y')}/{datetime.now().strftime('%m')}"
    dst_file = f"{dst_dir}/{name}_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
    # Example: read pipe-delimited text as CSV with custom separator
    sep = opts.get("sep", "|")
    with open(src_path, newline="", encoding="utf-8-sig") as _f:
        rows = list(csv.DictReader(_f, delimiter=sep))
    df = spark.createDataFrame(rows)
    # Write to parquet
    dbutils.fs.mkdirs(dst_dir)
    tmp_dir = f"{dst_dir}/_tmp_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    df.coalesce(1).write.mode("overwrite").parquet(tmp_dir)
    part = [f.path for f in dbutils.fs.ls(tmp_dir) if f.name.endswith(".parquet")][0]
    dbutils.fs.mv(part, dst_file)
    dbutils.fs.rm(tmp_dir, recurse=True)
    print(f"[TXT] {fname} → {dst_file}")

# Main execution: process only entries where format is 'txt' or custom handled as txt
for key, cfg in SOURCE_MAP.items():
    if cfg.get("format") == "txt" or cfg.get("format") == "csv":
        # treat csv as txt if needed; adjust as required
        process_txt(key, cfg)
