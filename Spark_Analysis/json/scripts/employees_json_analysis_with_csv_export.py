from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("EmployeesJsonTables").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw_df = spark.read.option("multiline", True).json("/projects/semi_structured/json/Employees.json")
raw_df.printSchema()
raw_df.createOrReplaceTempView("raw_tbl")

spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW employees_tbl AS
SELECT
    emp.id AS id,
    emp.name AS name,
    emp.profile.contact.email AS email,
    emp.profile.contact.phone AS phone,
    emp.profile.contact.address.street AS street,
    emp.profile.contact.address.city AS city,
    emp.profile.contact.address.location.state AS state,
    emp.profile.contact.address.location.country AS country,
    TRY_CAST(emp.profile.contact.address.location.geo.lat AS DOUBLE) AS lat,
    TRY_CAST(emp.profile.contact.address.location.geo.long AS DOUBLE) AS lng
FROM (SELECT explode(employees) AS emp FROM raw_tbl)
""")

spark.sql("SELECT * FROM employees_tbl LIMIT 10").show(truncate=False)
spark.sql("DESCRIBE employees_tbl").show()
spark.sql("SELECT COUNT(*) AS total_employees FROM employees_tbl").show()

spark.sql("""
SELECT
    SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS id_nulls,
    SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END) AS name_nulls,
    SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS email_nulls
FROM employees_tbl
""").show()

spark.sql("""
SELECT country, COUNT(*) AS num_employees
FROM employees_tbl
GROUP BY country
ORDER BY num_employees DESC
""").show()

spark.sql("""
SELECT state, COUNT(*) AS num_employees
FROM employees_tbl
GROUP BY state
ORDER BY num_employees DESC
LIMIT 10
""").show()

spark.sql("""
SELECT city, COUNT(*) AS num_employees
FROM employees_tbl
GROUP BY city
ORDER BY num_employees DESC
LIMIT 10
""").show()

spark.sql("""
SELECT SUBSTRING_INDEX(email, '@', -1) AS email_domain, COUNT(*) AS num_employees
FROM employees_tbl
GROUP BY SUBSTRING_INDEX(email, '@', -1)
ORDER BY num_employees DESC
LIMIT 10
""").show()

spark.sql("""
SELECT MAX(lat) AS max_lat, MIN(lat) AS min_lat, AVG(lat) AS avg_lat
FROM employees_tbl
""").show()

spark.sql("""
SELECT id, name, city, state
FROM employees_tbl
WHERE state = 'FL'
""").show(10, truncate=False)

(
    spark.table("employees_tbl").write
    .mode("overwrite")
    .option("header", True)
    .csv("/projects/structured/csv/employees_flat_csv")
)
