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
spark = builder.getOrCreate()

spark = configure_spark_with_delta_pip(builder).getOrCreate()

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

# write the transformed data to a Delta table in overwrite mode 
#df_city_ktm.write.format("delta").mode("overwrite").save(CUST_DELTA_PATH)

# read back the data from the Delta table and show the results
df_ktm_read = spark.read.format("delta").load(CUST_DELTA_PATH)
df_ktm_read.select("customer_id","customer_name","city").show()

# updating record fot time travel
spark.sql("""
          update delta.`/warehouse/events/ttv`
          set city = 'KathmanduNP'
          where city = 'Kathmandu'
          and customer_id = 'C0002'
          """)

spark.sql("""
 Select customer_id, customer_name, gender, age, city from delta.`/warehouse/events/ttv` where city = 'KathmanduNP'
    """).show()

# Show the history of changes made to the Delta table
describe_history = spark.sql("DESCRIBE HISTORY delta.`/warehouse/events/ttv` ")
describe_history.select(
    "version",
    "timestamp",
    "operation",
    "operationParameters",
    "userName"
).show(truncate=False)

# With update also Deletion vector kicks in: so no new parquet file is created
# Instead, a deletion vector is created to mark the deleted rows in the existing parquet file.
# This is more efficient in terms of storage and performance, especially for large datasets.
# To see the deletion vector file, you can check the _delta_log directory of the Delta

# time travel to previous version

# we can go back to time travel to previous version using either version number or timestamp
spark.sql("""
     Select customer_id, customer_name, gender, age, city from delta.`/warehouse/events/ttv` 
            VERSION AS OF 1
          where customer_id = 'C0002'
          """).show()

'''
#Restore to previous version
spark.sql("""
     RESTORE delta.`/warehouse/events/ttv` TO VERSION AS OF 0
          """)  
describe_history = spark.sql("DESCRIBE HISTORY delta.`/warehouse/events/ttv` ")
describe_history.select(
    "version",
    "timestamp",
    "operation",
    "operationParameters",
    "userName"
).show(truncate=False)
'''



