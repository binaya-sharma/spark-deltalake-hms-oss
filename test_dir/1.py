from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, lit

WAREHOUSE_DIR = "/warehouse"          # mounted local path for managed tables
DELTA_PATH = "/warehouse/events"  # a specific Delta table path

builder = (
    SparkSession.builder
    .appName("spark-delta-local")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")  # ← add this
    .config("spark.ui.port", "4040")
    .config("spark.sql.shuffle.partitions", "1")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# 1) Create a small DataFrame
df = spark.createDataFrame(
    [
        (1, "signup", "2025-09-05"),
        (2, "click",  "2025-09-05"),
        (3, "purchase", "2025-09-05"),
        (4, "refund", "2025-09-05")
    ],
    
    ["user_id", "event", "event_date"]
)

# 2) Write it as a Delta table (overwrite for idempotent local dev)
df.write.format("delta").mode("overwrite").save(DELTA_PATH)

print("Initial write complete. Reading back:")
spark.read.format("delta").load(DELTA_PATH).show(truncate=False)

# 3) Upsert-like pattern: overwrite a partition or do MERGE if you add keys
# For demo: append a new row and then update one
new_df = spark.createDataFrame([(2, "click", "2025-09-06")], ["user_id","event","event_date"])
new_df.write.format("delta").mode("append").save(DELTA_PATH)

print("After append:")
spark.read.format("delta").load(DELTA_PATH).orderBy("user_id","event_date").show(truncate=False)

# 4) Simple update using Delta (SQL)
spark.sql(f"CREATE TABLE IF NOT EXISTS events USING DELTA LOCATION '{DELTA_PATH}'")
spark.sql("UPDATE events SET event = 'signup_bonus' WHERE user_id = 1")

print("After update via SQL:")
spark.table("events").orderBy("user_id","event_date").show(truncate=False)

# 5) Time travel example: read version 0
print("Time travel (version 0):")
spark.read.format("delta").option("versionAsOf", 0).load(DELTA_PATH).show(truncate=False)

input("Press Enter to exit and stop Spark...")

spark.stop()