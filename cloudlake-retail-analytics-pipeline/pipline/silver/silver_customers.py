# silver_customers.py
# Reads: bronze_catalog.01_bronze.customers
# Writes: silver_catalog.02_silver.dim_customers  (Delta MERGE)



from pyspark.sql.functions import col, lower, trim, to_date, datediff
from pyspark.sql.functions import current_date, current_timestamp
from delta.tables          import DeltaTable

SILVER_PATH = f'{DELTA_BASE}/silver/dim_customers'
JOB_NAME    = 'silver_customers'

try:
    log_job_start(JOB_NAME)

    bronze_df = spark.read.table(f'{BRONZE_DB}.customers')

    cleaned = (
        bronze_df
        .filter(col('customer_id').isNotNull())
        .filter(col('email').isNotNull())
        .withColumn('email',       lower(trim(col('email'))))
        .withColumn('name',        trim(col('name')))
        .withColumn('city',        trim(col('city')))
        .withColumn('phone',       col('phone').cast('string'))
        .withColumn('signup_date', to_date(col('signup_date'),'yyyy-MM-dd'))
        .withColumn('is_active',   col('is_active').cast('boolean'))
        .withColumn('customer_age_days',           # how long they've been a customer
            datediff(current_date(), col('signup_date')))
        .dropDuplicates(['customer_id'])           # one row per customer
        .drop('_source_file','_layer')
    )

    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        target = DeltaTable.forPath(spark, SILVER_PATH)
        (
            target.alias('t')
            .merge(cleaned.alias('s'), 't.customer_id = s.customer_id')
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        cleaned.write.format('delta').save(SILVER_PATH)
        spark.sql(
            f'CREATE TABLE IF NOT EXISTS {SILVER_DB}.dim_customers'
            f' USING DELTA LOCATION "{SILVER_PATH}"'
        )

    row_count = spark.read.table(f'{SILVER_DB}.dim_customers').count()
    log_job_success(JOB_NAME, row_count)

except Exception as e:
    log_job_failure(JOB_NAME, e)
    raise
