# Spark Analysis – Parquet & JSON Data from HDFS

This part of the project uses **PySpark** to read two of the datasets stored in HDFS (see the **HDFS_Project_Screenshots** folder), analyse them with DataFrame and SQL operations, and write the Gold vs Bitcoin results back to HDFS as CSV.

| Folder | Input (HDFS) | Script | Output (HDFS) |
|---|---|---|---|
| [`parquet/`](parquet) | `/projects/structured/parquet/Gold_vs_Bitcoin.parquet` | `gold_vs_bitcoin_parquet_analysis.py` | `/projects/structured/csv/gold_vs_bitcoin_csv/` |
| [`json/`](json) | `/projects/semi_structured/json/Employees.json` | `employees_json_analysis.py` | — (analysis only) |

```
Spark_Analysis/
├── parquet/
│   ├── scripts/gold_vs_bitcoin_parquet_analysis.py
│   └── screenshots/Gold_vs_Bitcoin_spark_1 … 5
└── json/
    ├── scripts/employees_json_analysis.py
    └── screenshots/Employees_spark_1 … 3
```

## Running

Scripts were run from `~/hdfs_project/spark` on `hadoop-master`, with HDFS as the default file system (so `/projects/...` paths resolve to HDFS):

```bash
spark-submit parquet/scripts/gold_vs_bitcoin_parquet_analysis.py
spark-submit json/scripts/employees_json_analysis.py
```

Both scripts set the log level to `WARN` to keep the output readable.

---

## 1. Gold vs Bitcoin (Parquet) – DataFrame API

Script: [`parquet/scripts/gold_vs_bitcoin_parquet_analysis.py`](parquet/scripts/gold_vs_bitcoin_parquet_analysis.py)

**Steps**
1. Read the Parquet file, show 10 rows, print schema, dtypes and `summary()`.
2. Count nulls per column.
3. Group by minute (`date_trunc("minute", time)`) → average gold, average bitcoin, rows per minute.
4. Overall max / min / average for each asset.
5. Pearson correlation between `gold` and `bitcoin`.
6. Filters: rows where bitcoin > gold; rows where gold > 1.05; top 5 rows by gold.
7. Add `gold_minus_bitcoin` and `leader` (Gold / Bitcoin / Equal), count rows per leader.
8. Write the enriched DataFrame to HDFS as CSV with a header.

**Schema:** `time` timestamp_ntz · `gold` double · `bitcoin` double

**Results**

| Metric | Gold | Bitcoin |
|---|---|---|
| Rows | 517 | 517 |
| Nulls | 0 | 0 |
| Mean | 0.7206 | 0.8822 |
| Std dev | 0.1696 | 0.1712 |
| Min | 0.3005 | 0.4599 |
| Median (50%) | 0.7634 | 0.8799 |
| Max | 1.0487 | 1.3234 |

- **Time span:** 2024-10-28 22:20:00 → 22:28, one row per second (8 full minutes of 60 rows + 37 rows in the last minute = 517).
- **Correlation gold vs bitcoin:** **0.1448** — a weak positive relationship.
- **Gold > 1.05:** no rows (the maximum gold value is 1.0487).
- **Highest gold:** 1.0487 at 22:20:03.
- **Leader counts:** Bitcoin higher in **375** seconds, Gold higher in **142** (no ties).
- **Output:** `/projects/structured/csv/gold_vs_bitcoin_csv/` → `_SUCCESS` + one `part-00000-….csv` (45 KB) with columns `time, gold, bitcoin, gold_minus_bitcoin, leader`.

Verify the output:
```bash
hdfs dfs -ls  /projects/structured/csv/gold_vs_bitcoin_csv
hdfs dfs -cat /projects/structured/csv/gold_vs_bitcoin_csv/part-*.csv | head -5
hdfs dfs -du -h /projects/structured/csv/gold_vs_bitcoin_csv
```

Screenshots: [`parquet/screenshots/`](parquet/screenshots)

---

## 2. Employees (JSON) – Spark SQL

Script: [`json/scripts/employees_json_analysis.py`](json/scripts/employees_json_analysis.py)

**Steps**
1. Read the nested JSON with `multiline=true`, print the schema, register it as `raw_tbl`.
2. Flatten it into the view `employees_tbl` with `explode(employees)` and dot-paths into `profile.contact.address…`. Latitude/longitude are strings in the source, so they are converted with `TRY_CAST(... AS DOUBLE)`.
3. SQL queries: preview, `DESCRIBE`, total count, null checks, employees per country / state / city / email domain, latitude stats, filter `state = 'FL'`.

**Flattened table (`employees_tbl`)**

| Column | Type | Source path |
|---|---|---|
| id, name | string | `emp.id`, `emp.name` |
| email, phone | string | `emp.profile.contact.*` |
| street, city | string | `emp.profile.contact.address.*` |
| state, country | string | `emp.profile.contact.address.location.*` |
| lat, lng | double | `emp.profile.contact.address.location.geo.lat / long` |

**Results**
- Nested schema confirmed: `employees[] → profile → contact → address → location → geo → timezone`, plus `projects[] → tasks[] → assignedTo → skills → experience → certifications`.
- **Total employees:** 1 → `E00001, David Walker, pricevincent@example.org, Harrisshire, FL, USA, lat -50.8298, lng -39.986`.
- **Nulls** in id / name / email: 0 / 0 / 0.
- Per country: USA = 1 · per state: FL = 1 · per city: Harrisshire = 1 · email domain: example.org = 1.
- Latitude max / min / avg: -50.8298 (single record).
- `state = 'FL'` filter returns E00001.

Because the file holds just one employee, the aggregations are trivial, but the same queries work unchanged on a larger file.

Screenshots: [`json/screenshots/`](json/screenshots)

---

## Key Spark techniques used

| Technique | Where |
|---|---|
| `spark.read.parquet` / `.json(multiline)` | Reading from HDFS |
| `printSchema`, `dtypes`, `summary()`, `DESCRIBE` | Schema & profiling |
| `count(when(isNull))` / `SUM(CASE WHEN … IS NULL)` | Null checks |
| `date_trunc` + `groupBy().agg()` | Time bucketing |
| `stat.corr` | Correlation |
| `withColumn` + `when().otherwise()` | Derived columns |
| `explode` + nested dot-paths | Flattening JSON |
| `createOrReplaceTempView` + `spark.sql` | SQL on DataFrames |
| `TRY_CAST`, `SUBSTRING_INDEX` | Safe casting, string parsing |
| `write.mode("overwrite").option("header", True).csv` | Writing results back to HDFS |
