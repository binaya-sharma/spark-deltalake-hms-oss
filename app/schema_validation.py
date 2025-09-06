from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, lit
from pyspark.sql.types import *

WAREHOUSE_DIR = "/warehouse"         # mounted local path for managed tables
CUST_DELTA_PATH = "/warehouse/events/ttv"  #different transformation have different path to preserve schema and data

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

spark = configure_spark_with_delta_pip(builder).getOrCreate()

#spark.sql(""" SELECT ID, ID1, ID2 FROM VALUES (1, 2, 3), (4, 5, 6) AS T(a, b, c) """).show() # value() as t()

# Define the schema for the customer sales data using structured type and fields.
df_schema = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("city", StringType(), True),
    StructField("loyalty_tier", StringType(), True),
    StructField("store_name", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("supplier_name", StringType(), True),
    StructField("employee_name", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("total_sales", DoubleType(), True),
    StructField("coupon_code", StringType(), True),
    StructField("discount_percent", IntegerType(), True),
    StructField("return_policy", StringType(), True),
    StructField("warranty", StringType(), True),
    StructField("customer_frequency", StringType(), True),
    StructField("mode", StringType(), True),
    StructField("sale_date", StringType(), True),  
    StructField("sale_time", StringType(), True) 
])

# Read the customer sales data from a CSV file into a DataFrame using the defined schema.
df = (spark.read
    .option("header", "false")  # no header in the file if header is present, change to true
    .option("quote", '"')       # handle quoted values 
    .option("delimiter", ",")  # comma-separated values
    .option("inferSchema", "false") # we are providing schema, so no need to infer
    .schema(df_schema)
    .csv("/data/raw_data/cust-sample-data.txt") # path to the CSV file
    )

# simple transformation: filter rows where city is Kathmandu and add a new column ingest_date with a constant value
df_city_ktm = df.filter(col("city") == "Kathmandu").withColumn("ingest_date", lit("2025-09-05")) 

df_city_ktm.select('customer_id','customer_name','gender','age','email' ).show(5, truncate=False)

spark.sql("""
          create table if not exists delta_table ( select * from values (1, 2, 3), (4, 5, 6) AS T(a, b, c) )
          """) # hive to be configured for persistant table and metastore

# delta supports schema validation 
# it insert data only if the schema of the data matches the schema of the table but need user need to be sure that the data types are compatible and in order

# The MERGE statement is considered robust because it enforces both column name matching and data type compatibility between the source and target tables. 
# When you run a merge, Delta validates that the columns referenced in the ON, WHEN MATCHED, and WHEN NOT MATCHED clauses exist and that their data types can safely be written. 
# If there’s a mismatch, Spark will fail the operation rather than silently corrupt data. This makes merges safer
# compared to manual upserts, as it reduces schema drift issues and ensures strong data integrity.