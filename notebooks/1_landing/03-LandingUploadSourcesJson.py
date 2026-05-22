# Databricks notebook source
# DBTITLE 1,Landing Upload Sources - JSON (multiline)


# Reuse globals from 00 script (SOURCES_PATH, LANDING_PATH, SOURCE_MAP, EXPECTED_FILES)

def process_json_multi(source_key, config):
    name = config["name"]
    fname = config["file"]
    src_path = f"{SOURCES_PATH}/{fname}"
    dst_dir = f"{LANDING_PATH}/{name}/{datetime.now().strftime('%Y')}/{datetime.now().strftime('%m')}"
    dst_file = f"{dst_dir}/{name}_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
    with open(src_path, "r", encoding="utf-8") as _f:
        data = json.load(_f)
        rows = data if isinstance(data, list) else [data]
    df = spark.createDataFrame(rows)
    dbutils.fs.mkdirs(dst_dir)
    tmp_dir = f"{dst_dir}/_tmp_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    df.coalesce(1).write.mode("overwrite").parquet(tmp_dir)
    part = [f.path for f in dbutils.fs.ls(tmp_dir) if f.name.endswith('.parquet')][0]
    dbutils.fs.mv(part, dst_file)
    dbutils.fs.rm(tmp_dir, recurse=True)
    print(f"[JSON] {fname} → {dst_file}")

# Process only entries where format is 'json' and options indicate multiline
for key, cfg in SOURCE_MAP.items():
    if cfg.get("format") == "json" and cfg.get("options", {}).get("multiLine", False):
        process_json_multi(key, cfg)
