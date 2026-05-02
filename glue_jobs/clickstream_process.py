import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, count, when, date_trunc

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

# Read gzip JSON — Spark auto-detects gzip
clickstream_df = spark.read.json(f"{BUCKET}/raw/clickstream/")

print(f"Raw events: {clickstream_df.count()}")
clickstream_df.printSchema()

# Deduplicate
deduped_df = clickstream_df.dropDuplicates(["event_id"])
print(f"After dedup: {deduped_df.count()}")

# Cast timestamp
deduped_df = deduped_df \
    .withColumn("event_time", col("timestamp").cast("timestamp")) \
    .withColumn("session_hour", date_trunc("hour", col("timestamp").cast("timestamp")))

# Sessionize (batch-safe)
sessionized_df = deduped_df.groupBy(
    col("user_id"),
    col("session_id"),
    col("session_hour"),
    col("device"),
    col("referrer")
).agg(
    count("*").alias("event_count"),
    count(when(col("event_type") == "click", 1)).alias("clicks"),
    count(when(col("event_type") == "add_to_cart", 1)).alias("add_to_carts"),
    count(when(col("event_type") == "purchase", 1)).alias("purchases"),
    count(when(col("event_type") == "view", 1)).alias("views")
)

print(f"Sessions: {sessionized_df.count()}")
sessionized_df.show(10, truncate=False)

# Create Iceberg table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.sessions (
        user_id STRING,
        session_id STRING,
        session_hour TIMESTAMP,
        device STRING,
        referrer STRING,
        event_count BIGINT,
        clicks BIGINT,
        add_to_carts BIGINT,
        purchases BIGINT,
        views BIGINT
    ) USING iceberg
    PARTITIONED BY (days(session_hour))
    LOCATION '{BUCKET}/curated/sessions/'
""")

sessionized_df.writeTo(f"{CATALOG}.sessions") \
    .using("iceberg") \
    .createOrReplace()

print("Clickstream job complete!")
job.commit()