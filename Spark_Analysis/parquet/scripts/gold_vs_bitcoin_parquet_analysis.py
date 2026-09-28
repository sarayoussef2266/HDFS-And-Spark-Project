from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, desc, when

spark = SparkSession.builder.appName("GoldBitcoinDataFrame").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

gold_df = spark.read.parquet("/projects/structured/parquet/Gold_vs_Bitcoin.parquet")

gold_df.show(10, truncate=False)
gold_df.printSchema()
print(gold_df.dtypes)
gold_df.summary().show()
print("Total rows:", gold_df.count())

gold_df.select(
    F.count(when(col("time").isNull(), "time")).alias("time_nulls"),
    F.count(when(col("gold").isNull(), "gold")).alias("gold_nulls"),
    F.count(when(col("bitcoin").isNull(), "bitcoin")).alias("bitcoin_nulls")
).show()

minute_df = (
    gold_df
    .withColumn("minute", F.date_trunc("minute", F.to_timestamp(col("time"))))
    .groupBy("minute")
    .agg(
        F.avg("gold").alias("avg_gold"),
        F.avg("bitcoin").alias("avg_bitcoin"),
        F.count("*").alias("rows_in_minute")
    )
    .orderBy("minute")
)
minute_df.show(20, truncate=False)

gold_df.select(
    F.max("gold").alias("max_gold"),
    F.min("gold").alias("min_gold"),
    F.avg("gold").alias("avg_gold"),
    F.max("bitcoin").alias("max_bitcoin"),
    F.min("bitcoin").alias("min_bitcoin"),
    F.avg("bitcoin").alias("avg_bitcoin")
).show()

print("Correlation gold vs bitcoin:", gold_df.stat.corr("gold", "bitcoin"))

gold_df.where(col("bitcoin") > col("gold")).show(10, truncate=False)
gold_df.where(col("gold") > 1.05).show(10, truncate=False)
gold_df.orderBy(desc("gold")).show(5, truncate=False)

gold_diff_df = (
    gold_df
    .withColumn("gold_minus_bitcoin", col("gold") - col("bitcoin"))
    .withColumn(
        "leader",
        when(col("gold") > col("bitcoin"), "Gold")
        .when(col("gold") < col("bitcoin"), "Bitcoin")
        .otherwise("Equal")
    )
)
gold_diff_df.show(10, truncate=False)
gold_diff_df.groupBy("leader").count().show()

(
    gold_diff_df.write
    .mode("overwrite")
    .option("header", True)
    .csv("/projects/structured/csv/gold_vs_bitcoin_csv")
)
