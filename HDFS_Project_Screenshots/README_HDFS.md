# HDFS Project – Storing & Exploring Structured, Semi-Structured and Unstructured Data

This part of the project loads six datasets of different types into HDFS, organizes them by data type under `/projects`, and inspects each one with `hdfs dfs` commands (plus `jq` for JSON and `parquet-tools` for Parquet).

- **Cluster:** single node `hadoop-master`, user `hadoop`
- **Replication factor:** 1 (every file)
- **Local source folder:** `/home/hadoop/Downloads`

> Every `hdfs dfs` command prints an `SLF4J: Class path contains multiple SLF4J bindings` warning (Hadoop and Tez each ship a `slf4j-reload4j` jar). It is harmless and can be ignored.
> `cat: Unable to write to output stream.` after `| head` is also harmless — `head` simply closed the pipe early.

---

## HDFS directory layout

```
/projects
├── structured/
│   ├── csv/online_food_delivery_dataset.csv          38,976 B
│   └── parquet/Gold_vs_Bitcoin.parquet               11,823 B
├── semi_structured/
│   ├── json/Employees.json                            2,692 B
│   └── logs/Exercise-Data-Augmentation.log          167,710 B
└── unstructured/
    ├── images/cat.1.jpg … cat.99.jpg   (202 files)    3.9 MB
    └── text/Sample_Data.txt                          16,384 B
```

| Type | Format | Why it belongs there |
|---|---|---|
| Structured | CSV, Parquet | Fixed columns / schema |
| Semi-structured | JSON, log | Self-describing or line-based, but no fixed table schema |
| Unstructured | JPG images, free text | No schema at all |

Screenshots: [`00_all_directories/`](00_all_directories)

```bash
hdfs dfs -mkdir -p /projects/{structured/{csv,parquet},semi_structured/{json,logs},unstructured/{images,text}}
hdfs dfs -ls -R /projects
```

---

## Structured data

### CSV – `online_food_delivery_dataset.csv`
Screenshots: [`structured/csv/`](structured/csv)

```bash
hdfs dfs -cat  /projects/structured/csv/online_food_delivery_dataset.csv | head -5
hdfs dfs -tail /projects/structured/csv/online_food_delivery_dataset.csv
hdfs dfs -cat  /projects/structured/csv/online_food_delivery_dataset.csv | wc -l
hdfs dfs -stat "Size: %b bytes, Replication: %r, Last modified: %y" /projects/structured/csv/online_food_delivery_dataset.csv
```

| Check | Result |
|---|---|
| Columns | Age, Gender, Marital Status, Occupation, Monthly Income, Educational Qualifications, Family size, Customer Type, latitude, longitude, Pin code, Output, Feedback, + 1 unnamed Yes/No column |
| Lines (`wc -l`) | 389 (header + 388 records) |
| Size | 38,976 bytes |
| Replication | 1 |

### Parquet – `Gold_vs_Bitcoin.parquet`
Screenshots: [`structured/parquet/`](structured/parquet)

Parquet is binary, so the file was copied out of HDFS and inspected with `parquet-tools`:

```bash
hdfs dfs -get /projects/structured/parquet/Gold_vs_Bitcoin.parquet /tmp/
parquet-tools show    /tmp/Gold_vs_Bitcoin.parquet
parquet-tools inspect /tmp/Gold_vs_Bitcoin.parquet
parquet-tools csv     /tmp/Gold_vs_Bitcoin.parquet > gold_vs_bitcoin_preview.csv
```

| Metadata | Value |
|---|---|
| Created by | DuckDB |
| Rows / columns / row groups | 517 / 3 / 1 |
| `time` | INT64, Timestamp (microseconds, not UTC-adjusted) |
| `gold`, `bitcoin` | DOUBLE |
| Compression | SNAPPY (24% saved on `time`, ~0% on the doubles) |

---

## Semi-structured data

### JSON – `Employees.json`
Screenshots: [`semi_structured/json/`](semi_structured/json)

```bash
hdfs dfs -cat /projects/semi_structured/json/Employees.json | head -c 500
hdfs dfs -cat /projects/semi_structured/json/Employees.json | jq .
hdfs dfs -cat /projects/semi_structured/json/Employees.json | jq 'length'
hdfs dfs -stat "Size: %b bytes, Replication: %r, Last modified: %y" /projects/semi_structured/json/Employees.json
```

- Deeply nested document: `employees[] → profile → contact → address → location → geo → timezone`, plus `projects[] → tasks[]`.
- `jq 'length'` returns **1** — the root object has one key (`employees`).
- Size 2,692 bytes, replication 1.

### Log – `Exercise-Data-Augmentation.log`
Screenshots: [`semi_structured/logs/`](semi_structured/logs)

```bash
hdfs dfs -cat  /projects/semi_structured/logs/Exercise-Data-Augmentation.log | head -5
hdfs dfs -tail /projects/semi_structured/logs/Exercise-Data-Augmentation.log
hdfs dfs -cat  /projects/semi_structured/logs/Exercise-Data-Augmentation.log | wc -l
hdfs dfs -cat  /projects/semi_structured/logs/Exercise-Data-Augmentation.log | grep -i  "error"
hdfs dfs -cat  /projects/semi_structured/logs/Exercise-Data-Augmentation.log | grep -ic "warn"
hdfs dfs -stat "Size: %b bytes, Replication: %r, Last modified: %y" /projects/semi_structured/logs/Exercise-Data-Augmentation.log
```

| Check | Result |
|---|---|
| Content | Keras training log (epochs 40–50, `binary_accuracy` 1.0, `val_binary_accuracy` ≈ 0.89) followed by Jupyter `nbconvert` output |
| Lines | 33 (very long lines – progress bars) |
| Lines containing "error" | 0 |
| Lines containing "warn" | 4 (`FutureWarning` from traitlets) |
| Size | 167,710 bytes |

---

## Unstructured data

### Images – `cat.*.jpg`
Screenshots: [`unstructured/images/`](unstructured/images)

```bash
hdfs dfs -put -f /home/hadoop/Downloads/cat_images/*.jpg /projects/unstructured/images/
ls /home/hadoop/Downloads/cat_images/*.jpg | wc -l                         # 202 local
hdfs dfs -ls /projects/unstructured/images/ | grep -c "\.jpg$"             # 202 in HDFS
hdfs dfs -du -s -h /projects/unstructured/images/                          # 3.9 M
```

All 202 local images arrived in HDFS (counts match), 3.9 MB in total.

### Text – `Sample_Data.txt`
Screenshots: [`unstructured/text/`](unstructured/text)

```bash
hdfs dfs -cat  /projects/unstructured/text/Sample_Data.txt | head -5
hdfs dfs -tail /projects/unstructured/text/Sample_Data.txt
hdfs dfs -cat  /projects/unstructured/text/Sample_Data.txt | wc -lwc
hdfs dfs -stat "Size: %b bytes, Replication: %r, Last modified: %y" /projects/unstructured/text/Sample_Data.txt
```

| Check | Result |
|---|---|
| Content | App request lines, e.g. `INFO app[worker.0] – Request 0 completed in 20ms` (4 workers, 20–26 ms) |
| Lines / words / bytes | 227 / 2,256 / 16,384 |

---

## Command cheat-sheet

| Command | Purpose |
|---|---|
| `hdfs dfs -ls -R <dir>` | Recursive listing |
| `hdfs dfs -put -f <local> <hdfs>` | Upload (overwrite) |
| `hdfs dfs -get <hdfs> <local>` | Download |
| `hdfs dfs -cat <file> \| head` | Read beginning of a file |
| `hdfs dfs -tail <file>` | Last 1 KB of a file |
| `hdfs dfs -du -s -h <dir>` | Total size, human-readable |
| `hdfs dfs -stat "%b %r %y" <file>` | Size, replication, modification time |

Next step: the CSV/Parquet and JSON data are processed with PySpark — see the **Spark_Analysis** folder.
