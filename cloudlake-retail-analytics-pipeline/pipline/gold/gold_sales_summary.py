# gold_sales_summary.py
# Reads: fact_orders  +  dim_restaurants
# Writes: gold_catalog.03_gold.d_sales_summary  (full overwrite daily)



from pyspark.sql.functions import (col, count, sum as _sum, avg, round,
                                    date_format, current_timestamp)

JOB_NAME = 'gold_sales_summary'

try:
    log_job_start(JOB_NAME)

    # ── READ SILVER TABLES ────────────────────────────────
    orders = spark.read.table(f'{SILVER_DB}.fact_orders')
    rests  = spark.read.table(f'{SILVER_DB}.dim_restaurants')

    # ── JOIN + FILTER ─────────────────────────────────────
    # Only DELIVERED orders count as revenue
    gold_df = (
        orders
        .filter(col('status') == 'DELIVERED')
        .join(rests, 'restaurant_id', 'left')
        .groupBy(
            date_format(col('order_date'), 'yyyy-MM-dd').alias('sale_date'),
            col('restaurant_id'),
            col('restaurant_name'),
            col('city'),
            col('cuisine')
        )
        .agg(
            count('order_id'       ).alias('total_orders'),
            round(_sum('total_amount'),2).alias('total_revenue'),
            round(avg('total_amount'),2) .alias('avg_order_value'),
        )
        .withColumn('_updated_at', current_timestamp())
        .orderBy('sale_date','total_revenue', ascending=[True, False])
    )

    # ── WRITE GOLD (overwrite — Gold is always a fresh roll-up) ──
    (
        gold_df.write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema','true')
        .saveAsTable(f'{GOLD_DB}.d_sales_summary')
    )

    # ── QUICK PREVIEW ────────────────────────────────────
    print('=== d_sales_summary (top 5) ===')
    gold_df.show(5, truncate=False)

    log_job_success(JOB_NAME, gold_df.count())

except Exception as e:
    log_job_failure(JOB_NAME, e)
    raise
