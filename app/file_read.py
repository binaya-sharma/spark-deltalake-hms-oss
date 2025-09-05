from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, lit
from pyspark.sql.types import *

WAREHOUSE_DIR = "/warehouse"         # mounted local path for managed tables
CUST_DELTA_PATH = "/warehouse/events/cust"  #different transformation have different path to preserve schema and data

builder = (
    SparkSession.builder
    .appName("spark-delta-local")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
    .config("spark.ui.port", "4040")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.databricks.delta.properties.defaults.enableDeletionVectors", "true")  # enable DVs by default
    .config("spark.databricks.delta.deletionVectors.enabled", "true")  # enable DVs in session
)
spark = builder.getOrCreate()

spark = configure_spark_with_delta_pip(builder).getOrCreate()


spark.sql(""" select * from csv.`/data/events.csv` limit 10 """).show(truncate=False)

spark.sql(""" select sale_id,customer_id,customer_name,gender,age,email  from parquet.`/warehouse/events/cust/part-00000-dac4c156-852c-45b9-825d-179de86454fa-c000.snappy.parquet` limit 10 """).show(truncate=False)