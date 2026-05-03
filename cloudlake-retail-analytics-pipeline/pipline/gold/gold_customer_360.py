# gold_customer_360.py
# Reads: fact_orders  +  dim_customers
# Writes: gold_catalog.03_gold.d_customer_360  (full overwrite daily)


from pyspark.sql.functions import (col, count, sum as _sum, avg, max as _max,
                                    round, datediff, current_date, when,
                                    current_timestamp)

JOB_NAME = 'gold_customer_360'

try:
    log_job_start(JOB_NAME)

    orders = spark.read.table(f'{SILVER_DB}.fact_orders')
    custs  = spark.read.table(f'{SILVER_DB}.dim_customers')

    # ── ORDER STATS PER CUSTOMER ──────────────────────────
    order_stats = (
        orders
        .filter(col('status') == 'DELIVERED')
        .groupBy('customer_id')
        .agg(
            count('order_id'         ).alias('total_orders'),
            round(_sum('total_amount'),2).alias('lifetime_value'),
            round(avg('total_amount'),2) .alias('avg_order_value'),
            _max('order_date'         ).alias('last_order_date'),
        )
    )

    # ── JOIN + ENRICH ─────────────────────────────────────
    gold_df = (
        custs
        .join(order_stats, 'customer_id', 'left')

        # days since last order (NULL = never ordered)
        .withColumn('days_since_last_order',
            datediff(current_date(), col('last_order_date')))

        # Churn risk — simple rule-based scoring
        .withColumn('churn_risk',
            when(col('days_since_last_order') > 90, 'HIGH'  )
            .when(col('days_since_last_order') > 45, 'MEDIUM')
            .when(col('last_order_date').isNull(),  'HIGH'  )
            .otherwise('LOW'))

        # Customer value segment
        .withColumn('customer_segment',
            when(col('lifetime_value') > 5000, 'VIP'    )
            .when(col('lifetime_value') > 1000, 'REGULAR')
            .when(col('lifetime_value').isNull(),'NEW'  )
            .otherwise('NEW'))

        .withColumn('_updated_at', current_timestamp())
    )

    (
        gold_df.write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema','true')
        .saveAsTable(f'{GOLD_DB}.d_customer_360')
    )

    print('=== d_customer_360 (top 5) ===')
    gold_df.select('customer_id','name','city','total_orders',
                   'lifetime_value','churn_risk','customer_segment').show(5, truncate=False)

    log_job_success(JOB_NAME, gold_df.count())

except Exception as e:
    log_job_failure(JOB_NAME, e)
    raise
