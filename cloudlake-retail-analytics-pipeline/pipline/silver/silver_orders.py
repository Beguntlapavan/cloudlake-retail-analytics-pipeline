# silver_orders.py
# Reads: bronze_catalog.01_bronze.orders
# Writes: silver_catalog.02_silver.fact_orders  (Delta MERGE)



from pyspark.sql.functions import col, to_timestamp, trim, upper, when
from pyspark.sql.window   import Window
from pyspark.sql.functions import row_number, desc, current_timestamp
from delta.tables          import DeltaTable

SILVER_PATH = f'{DELTA_BASE}/silver/fact_orders'
JOB_NAME    = 'silver_orders'

try:
    log_job_start(JOB_NAME)

    # ── 1. READ FROM BRONZE ──────────────────────────────
    bronze_df = spark.read.table(f'{BRONZE_DB}.orders')

    # ── 2. CLEAN ─────────────────────────────────────────
    cleaned = (
        bronze_df
        .filter(col('order_id').isNotNull())              # drop null PKs
        .filter(col('total_amount').cast('double') > 0)   # drop bad amounts
        .withColumn('order_date',
            to_timestamp(col('order_date'),'yyyy-MM-dd HH:mm:ss'))
        .withColumn('total_amount', col('total_amount').cast('double'))
        .withColumn('status', upper(trim(col('status'))))
        .withColumn('delivery_city', trim(col('delivery_city')))
        .withColumn('order_year',
            col('order_date').cast('date').substr(1,4))
        .withColumn('order_month',
            col('order_date').cast('date').substr(6,2))
    )

    # ── 3. DEDUPLICATE ───────────────────────────────────
    # Keep only the latest version of each order_id
    window = Window.partitionBy('order_id').orderBy(desc('_ingested_at'))
    deduped = (
        cleaned
        .withColumn('rn', row_number().over(window))
        .filter(col('rn') == 1)
        .drop('rn','_source_file','_layer')
    )

    # ── 4. DELTA MERGE (UPSERT) ──────────────────────────
    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        silver = DeltaTable.forPath(spark, SILVER_PATH)
        (
            silver.alias('target')
            .merge(
                deduped.alias('source'),
                'target.order_id = source.order_id'
            )
            .whenMatchedUpdateAll()     # update if record changed
            .whenNotMatchedInsertAll()  # insert if brand new
            .execute()
        )
    else:
        # First run — write directly, partitioned for performance
        (
            deduped.write.format('delta')
            .partitionBy('order_year','order_month')
            .save(SILVER_PATH)
        )
        spark.sql(
            f'CREATE TABLE IF NOT EXISTS {SILVER_DB}.fact_orders'
            f' USING DELTA LOCATION "{SILVER_PATH}"'
        )

    row_count = spark.read.table(f'{SILVER_DB}.fact_orders').count()
    log_job_success(JOB_NAME, row_count)
    print(f'fact_orders loaded: {row_count} rows')

except Exception as e:
    log_job_failure(JOB_NAME, e)
    raise
