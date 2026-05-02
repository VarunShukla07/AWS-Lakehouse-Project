import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", "s3://varun-lakehouse-a86de32c/curated/")
spark.conf.set("spark.sql.defaultCatalog", "glue_catalog")

BUCKET = "s3://varun-lakehouse-a86de32c"
CATALOG = "glue_catalog.ecommerce_lakehouse"

# Read LOAD file
load_df = spark.read \
    .option("header", "false") \
    .option("multiLine", "true") \
    .option("quote", '"') \
    .option("escape", '"') \
    .csv(f"{BUCKET}/raw/cdc/public/orders/LOAD00000001.csv") \
    .toDF("order_id", "user_id", "order_date", "total_amount", "order_status") \
    .withColumn("Op", lit("I"))

print(f"Load orders: {load_df.count()}")

# Read CDC delta files
try:
    cdc_df = spark.read \
        .option("header", "false") \
        .option("multiLine", "true") \
        .option("quote", '"') \
        .option("escape", '"') \
        .csv(f"{BUCKET}/raw/cdc/public/orders/2026*.csv") \
        .toDF("Op", "order_id", "user_id", "order_date", "total_amount", "order_status")
    print(f"CDC orders: {cdc_df.count()}")
    all_orders = load_df.unionByName(cdc_df)
except Exception as e:
    print(f"No CDC delta files: {e}")
    all_orders = load_df

# Deduplicate — keep latest per order
window = Window.partitionBy("order_id").orderBy(col("order_date").desc())
orders_deduped = all_orders \
    .filter(col("Op") != "D") \
    .withColumn("rn", row_number().over(window)) \
    .filter(col("rn") == 1) \
    .drop("rn", "Op")

print(f"Deduplicated orders: {orders_deduped.count()}")

# Read current users — rename status to avoid ambiguity
users_df = spark.read.format("iceberg") \
    .load(f"{CATALOG}.users_scd2") \
    .filter(col("is_current") == True) \
    .select(
        col("user_id"),
        col("name").alias("user_name"),
        col("email").alias("user_email"),
        col("status").alias("user_status")   # renamed here — no ambiguity
    )

print(f"Current users: {users_df.count()}")

# Join — no duplicate column names now
enriched_df = orders_deduped.join(users_df, on="user_id", how="left")

print(f"Enriched orders: {enriched_df.count()}")
enriched_df.show(5, truncate=False)

# Create Iceberg fact table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.fact_orders (
        order_id STRING,
        user_id STRING,
        order_date TIMESTAMP,
        total_amount DECIMAL(10,2),
        order_status STRING,
        user_name STRING,
        user_email STRING,
        user_status STRING
    ) USING iceberg
    PARTITIONED BY (days(order_date))
    LOCATION '{BUCKET}/curated/fact_orders/'
""")

enriched_df.select(
    col("order_id"),
    col("user_id"),
    col("order_date").cast("timestamp"),
    col("total_amount").cast("decimal(10,2)"),
    col("order_status"),
    col("user_name"),
    col("user_email"),
    col("user_status")
).writeTo(f"{CATALOG}.fact_orders") \
    .using("iceberg") \
    .createOrReplace()

print("Fact orders job complete!")

spark.sql(f"""
    SELECT order_status, COUNT(*) as count,
           ROUND(AVG(total_amount), 2) as avg_value
    FROM {CATALOG}.fact_orders
    GROUP BY order_status
    ORDER BY count DESC
""").show()

job.commit()