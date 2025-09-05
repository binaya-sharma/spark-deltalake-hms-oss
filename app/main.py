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
    ], #data
    ["user_id", "event", "event_date"] #schema
)

#df.write.format("delta").mode("overwrite").save(DELTA_PATH)

#spark.read.format("delta").load(DELTA_PATH).show(truncate=False)

'''
df = spark.createDataFrame(
    [
        (4, "refund-append", "2025-09-05") 
    ], #data
    ["user_id", "event", "event_date"] #schema
)
df.write.format("delta").mode("append").save(DELTA_PATH)
'''

spark.read.format("delta").load(DELTA_PATH).show(truncate=False)

















input("Press Enter to exit and stop Spark...")

spark.stop()