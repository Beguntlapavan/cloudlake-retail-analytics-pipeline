# silver_restaurants.py
# Reads: bronze_catalog.01_bronze.restaurants
# Writes: silver_catalog.02_silver.dim_restaurants  (Delta MERGE)



from pyspark.sql.functions import col, trim, lower
from delta.tables          import DeltaTable

SILVER_PATH = f'{DELTA_BASE}/silver/dim_restaurants'
JOB_NAME    = 'silver_restaurants'

try:
    log_job_start(JOB_NAME)
    bronze_df = spark.read.table(f'{BRONZE_DB}.restaurants')

    cleaned = (
        bronze_df
        .filter(col('restaurant_id').isNotNull())
        .withColumn('restaurant_name', trim(col('restaurant_name')))
        .withColumn('city',            trim(col('city')))
        .withColumn('cuisine',         trim(col('cuisine')))
        .withColumn('avg_rating',      col('avg_rating').cast('double'))
        .withColumn('is_active',       col('is_active').cast('boolean'))
        .dropDuplicates(['restaurant_id'])
        .drop('_source_file','_layer')
    )

    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        target = DeltaTable.forPath(spark, SILVER_PATH)
        (
            target.alias('t')
            .merge(cleaned.alias('s'), 't.restaurant_id = s.restaurant_id')
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        cleaned.write.format('delta').save(SILVER_PATH)
        spark.sql(
            f'CREATE TABLE IF NOT EXISTS {SILVER_DB}.dim_restaurants'
            f' USING DELTA LOCATION "{SILVER_PATH}"'
        )

    log_job_success(JOB_NAME, spark.read.table(f'{SILVER_DB}.dim_restaurants').count())

except Exception as e:
    log_job_failure(JOB_NAME, e)
    raise
