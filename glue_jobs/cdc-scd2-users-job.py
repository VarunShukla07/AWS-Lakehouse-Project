import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lit, current_timestamp

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Iceberg catalog config (runtime configs only — extensions already set by --datalake-formats)
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", "s3://varun-lakehouse-a86de32c/curated/")
spark.conf.set("spark.sql.defaultCatalog", "glue_catalog")

BUCKET = "s3://varun-lakehouse-a86de32c"
CATALOG = "glue_catalog.ecommerce_lakehouse"

# ── Read LOAD file (multiline address, no header, no Op column) ──
load_df = spark.read \
    .option("header", "false") \
    .option("multiLine", "true") \
    .option("quote", '"') \
    .option("escape", '"') \
    .csv(f"{BUCKET}/raw/cdc/public/users/LOAD00000001.csv") \
    .toDF("user_id", "name", "email", "address", "created_at", "status") \
    .withColumn("Op", lit("I"))

# ── Read CDC delta files (has Op as first column) ────────────────
cdc_df = spark.read \
    .option("header", "false") \
    .option("multiLine", "true") \
    .option("quote", '"') \
    .option("escape", '"') \
    .csv(f"{BUCKET}/raw/cdc/public/users/20260429-142736970.csv") \
    .toDF("Op", "user_id", "name", "email", "address", "created_at", "status")

print(f"Load records: {load_df.count()}")
print(f"CDC records: {cdc_df.count()}")

# ── Combine using unionByName ────────────────────────────────────
all_df = load_df.unionByName(cdc_df)
all_df.groupBy("Op").count().show()

# ── Create Iceberg table if not exists ──────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.users_scd2 (
        user_id STRING,
        name STRING,
        email STRING,
        address STRING,
        status STRING,
        created_at TIMESTAMP,
        effective_date TIMESTAMP,
        expiration_date TIMESTAMP,
        is_current BOOLEAN
    ) USING iceberg
    PARTITIONED BY (days(effective_date))
    LOCATION '{BUCKET}/curated/users_scd2/'
""")

# ── Process Inserts ──────────────────────────────────────────────
inserts = all_df.filter(col("Op") == "I").select(
    col("user_id"),
    col("name"),
    col("email"),
    col("address"),
    col("status"),
    col("created_at").cast("timestamp"),
    current_timestamp().alias("effective_date"),
    lit(None).cast("timestamp").alias("expiration_date"),
    lit(True).alias("is_current")
)

inserts.writeTo(f"{CATALOG}.users_scd2").using("iceberg").append()
print(f"Inserted records")

# ── Process Updates via MERGE (SCD2) ────────────────────────────
updates = all_df.filter(col("Op") == "U").select(
    col("user_id"),
    col("name"),
    col("email"),
    col("address"),
    col("status"),
    col("created_at").cast("timestamp"),
    current_timestamp().alias("effective_date"),
    lit(None).cast("timestamp").alias("expiration_date"),
    lit(True).alias("is_current")
)

if not updates.rdd.isEmpty():
    updates.createOrReplaceTempView("updates_view")

    # Step 1: Close old records
    spark.sql(f"""
        MERGE INTO {CATALOG}.users_scd2 t
        USING updates_view s
        ON t.user_id = s.user_id AND t.is_current = true
        WHEN MATCHED THEN UPDATE SET
            t.expiration_date = current_timestamp(),
            t.is_current = false
    """)

    # Step 2: Insert new versions
    updates.writeTo(f"{CATALOG}.users_scd2").using("iceberg").append()
    print("Updates processed via MERGE")

# ── Process Deletes (soft delete) ───────────────────────────────
deletes = all_df.filter(col("Op") == "D")

if not deletes.rdd.isEmpty():
    deletes.createOrReplaceTempView("deletes_view")

    spark.sql(f"""
        MERGE INTO {CATALOG}.users_scd2 t
        USING deletes_view s
        ON t.user_id = s.user_id AND t.is_current = true
        WHEN MATCHED THEN UPDATE SET
            t.expiration_date = current_timestamp(),
            t.is_current = false
    """)
    print("Deletes processed")

# ── Validate ────────────────────────────────────────────────────
print("\nFinal counts by is_current:")
spark.sql(f"""
    SELECT is_current, COUNT(*) as count
    FROM {CATALOG}.users_scd2
    GROUP BY is_current
""").show()

print("\nSample current records:")
spark.sql(f"""
    SELECT user_id, name, email, status, effective_date, is_current
    FROM {CATALOG}.users_scd2
    WHERE is_current = true
    LIMIT 5
""").show(truncate=False)

job.commit()