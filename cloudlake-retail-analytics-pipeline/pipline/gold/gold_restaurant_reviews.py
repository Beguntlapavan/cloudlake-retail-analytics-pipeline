# gold_restaurant_reviews.py
# Reads: dim_restaurants  +  fact_reviews
# Writes: gold_catalog.03_gold.d_restaurant_reviews  (full overwrite daily)



from pyspark.sql.functions import (col, count, avg, round, sum as _sum,
                                    when, current_timestamp)

JOB_NAME = 'gold_restaurant_reviews'

try:
    log_job_start(JOB_NAME)

    rests   = spark.read.table(f'{SILVER_DB}.dim_restaurants')
    reviews = spark.read.table(f'{SILVER_DB}.fact_reviews')

    review_agg = (
        reviews
        .groupBy('restaurant_id')
        .agg(
            count('review_id').alias('total_reviews'),
            round(avg('rating'),2).alias('avg_rating'),
            _sum(when(col('rating') >= 4, 1).otherwise(0)).alias('positive_count'),
            _sum(when(col('rating') == 3, 1).otherwise(0)).alias('neutral_count'),
            _sum(when(col('rating') <= 2, 1).otherwise(0)).alias('negative_count'),
            _sum(when(col('sentiment_label')=='POSITIVE',1).otherwise(0))
                .alias('sentiment_positive'),
        )
    )

    gold_df = (
        rests
        .join(review_agg, 'restaurant_id', 'left')
        .select(
            'restaurant_id','restaurant_name','city','cuisine',
            'total_reviews','avg_rating',
            'positive_count','neutral_count','negative_count',
            'sentiment_positive',
        )
        .withColumn('_updated_at', current_timestamp())
    )

    (
        gold_df.write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema','true')
        .saveAsTable(f'{GOLD_DB}.d_restaurant_reviews')
    )

    print('=== d_restaurant_reviews ===')
    gold_df.show(truncate=False)

    log_job_success(JOB_NAME, gold_df.count())

except Exception as e:
    log_job_failure(JOB_NAME, e)
    raise