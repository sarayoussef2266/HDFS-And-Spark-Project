# HDFS & Spark Big Data Project

A hands-on Hadoop project: six datasets of different types are stored in **HDFS**, organized as structured / semi-structured / unstructured data, explored with `hdfs dfs` commands, and then analysed with **PySpark** (DataFrame API and Spark SQL), with the results written back to HDFS as CSV.

**Tools:** Hadoop HDFS · Apache Spark (PySpark) · Spark SQL · `jq` · `parquet-tools` · Linux (single node `hadoop-master`)

## Project structure

```
HDFS_And_Spark_Project/
├── HDFS_Project_Screenshots/        Part 1 – storing & exploring data in HDFS
│   ├── README_HDFS.md
│   ├── 00_all_directories/
│   ├── structured/       csv/ · parquet/
│   ├── semi_structured/  json/ · logs/
│   └── unstructured/     images/ · text/
└── Spark_Analysis/                  Part 2 – processing the data with PySpark
    ├── README_Spark.md
    ├── parquet/   scripts/ · screenshots/
    └── json/      scripts/ · screenshots/
```

## Part 1 – HDFS  →  [README_HDFS.md](HDFS_Project_Screenshots/README_HDFS.md)

| Type | Dataset | HDFS path |
|---|---|---|
| Structured | Online food delivery (CSV) | `/projects/structured/csv/` |
| Structured | Gold vs Bitcoin (Parquet) | `/projects/structured/parquet/` |
| Semi-structured | Employees (nested JSON) | `/projects/semi_structured/json/` |
| Semi-structured | Training log | `/projects/semi_structured/logs/` |
| Unstructured | 202 cat images (JPG) | `/projects/unstructured/images/` |
| Unstructured | Sample text | `/projects/unstructured/text/` |

## Part 2 – Spark  →  [README_Spark.md](Spark_Analysis/README_Spark.md)

| Analysis | Script | Highlights |
|---|---|---|
| Gold vs Bitcoin (Parquet, DataFrame API) | [`gold_vs_bitcoin_parquet_analysis.py`](Spark_Analysis/parquet/scripts/gold_vs_bitcoin_parquet_analysis.py) | 517 rows, 0 nulls, per-minute averages, correlation 0.145, Bitcoin led 375 s vs Gold 142 s, output saved as CSV in HDFS |
| Employees (nested JSON, Spark SQL) | [`employees_json_analysis.py`](Spark_Analysis/json/scripts/employees_json_analysis.py) | Flattened with `explode` + nested paths, null checks, group-bys by country/state/city/email domain |
