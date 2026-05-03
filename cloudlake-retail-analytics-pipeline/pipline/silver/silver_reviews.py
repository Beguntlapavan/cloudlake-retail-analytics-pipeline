# silver_reviews.py
# Reads: bronze_catalog.01_bronze.reviews
# Writes: silver_catalog.02_silver.fact_reviews  (Delta MERGE)



from pyspark.sql.functions import col, to_date, trim, when
from delta.tables          import DeltaTable

SILVER_PATH = f'{DELTA_BASE}/silver/fact_reviews'
JOB_NAME    = 'silver_reviews'

try:
    log_job_start(JOB_NAME)
    bronze_df = spark.read.table(f'{BRONZE_DB}.reviews')

    cleaned = (
        bronze_df
        .filter(col('review_id').isNotNull())
        .filter(col('rating').cast('int').between(1,5)) # valid rating 1-5 only
        .withColumn('rating',      col('rating').cast('int'))
        .withColumn('review_text', trim(col('review_text')))
        .withColumn('review_date', to_date(col('review_date'),'yyyy-MM-dd'))
        .withColumn('sentiment_label',             # rule-based quick label
            when(col('rating') >= 4, 'POSITIVE')
            .when(col('rating') == 3, 'NEUTRAL')
            .otherwise('NEGATIVE'))
        .dropDuplicates(['review_id'])
        .drop('_source_file','_layer')
    )

    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        target = DeltaTable.forPath(spark, SILVER_PATH)
        (
            target.alias('t')
            .merge(cleaned.alias('s'), 't.review_id = s.review_id')
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        cleaned.write.format('delta').save(SILVER_PATH)
        spark.sql(
            f'CREATE TABLE IF NOT EXISTS {SILVER_DB}.fact_reviews'
            f' USING DELTA LOCATION "{SILVER_PATH}"'
        )

    log_job_success(JOB_NAME, spark.read.table(f'{SILVER_DB}.fact_reviews').count())

except Exception as e:
    log_job_failure(JOB_NAME, e)
    raise
