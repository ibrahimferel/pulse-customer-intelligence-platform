# Kita bagi menjadi 3 processed data utama:

# analytics_orders:
# - Delivery Analysis
# - NLP Analysis
# - CX Score
# - Payment Analytics
# - Customer Segmentation

# customer_geo:
# - Geo Analytics

# seller_proximity:
# - Seller Proximity

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import os

def process_dustiniadelixia():
    spark = SparkSession.builder \
        .appName(
            "DustiniaDelixia_Process"
        ) \
        .config(
            "spark.driver.memory",
            "1g"
        ) \
        .getOrCreate()

    raw_dir = "/opt/airflow/data_lake/raw"

    print("Reading raw parquet...")

    orders_df = spark.read.parquet(
        f"{raw_dir}/orders/*.parquet"
    )

    items_df = spark.read.parquet(
        f"{raw_dir}/order_items/*.parquet"
    )

    customers_df = spark.read.parquet(
        f"{raw_dir}/customers/*.parquet"
    )

    sellers_df = spark.read.parquet(
        f"{raw_dir}/sellers/*.parquet"
    )

    payments_df = spark.read.parquet(
        f"{raw_dir}/order_payments/*.parquet"
    )

    reviews_df = spark.read.parquet(
        f"{raw_dir}/order_reviews/*.parquet"
    )

    geo_df = spark.read.parquet(
        f"{raw_dir}/geolocation/*.parquet"
    )

    items_agg = (
        items_df
        .groupBy("order_id")
        .agg(
            F.sum("price")
                .alias("total_price"),

            F.sum("freight_value")
                .alias("total_freight")
        )
    )

    payments_agg = (
        payments_df
        .groupBy("order_id")
        .agg(
            F.sum("payment_value")
                .alias("payment_value"),

            F.max("payment_installments")
                .alias("payment_installments"),

            F.max("payment_sequential")
                .alias("payment_sequential"),

            F.first("payment_type")
                .alias("payment_type")
        )
    )

    print("Building analytics_orders...")

    analytics_orders = (
        orders_df

        .join(
            items_agg,
            "order_id",
            "left"
        )

        .join(
            payments_agg,
            "order_id",
            "left"
        )

        .join(
            reviews_df,
            "order_id",
            "left"
        )

        .join(
            customers_df,
            "customer_id",
            "left"
        )
    )

    analytics_orders = (
        analytics_orders
        .dropDuplicates()
    )

    analytics_orders = (
        analytics_orders.fillna({
            "review_score":0,
            "payment_value":0
        })
    )

    analytics_orders = (
        analytics_orders
        .withColumn(
            "order_purchase_timestamp",
            F.to_timestamp(
                "order_purchase_timestamp"
            )
        )
    )

    analytics_orders = (
        analytics_orders
        .withColumn(
            "order_delivered_customer_date",
            F.to_timestamp(
                "order_delivered_customer_date"
            )
        )
    )

    analytics_orders = (
        analytics_orders
        .withColumn(
            "order_estimated_delivery_date",
            F.to_timestamp(
                "order_estimated_delivery_date"
            )
        )
    )

    print("Preparing geolocation...")

    geo_df = (
        geo_df
        .groupBy(
            "geolocation_zip_code_prefix"
        )
        .agg(

            F.avg(
                "geolocation_lat"
            ).alias(
                "geolocation_lat"
            ),

            F.avg(
                "geolocation_lng"
            ).alias(
                "geolocation_lng"
            )
        )
    )

    print("Building customer_geo...")

    customer_geo = (
        customers_df
        .join(
            geo_df,
            customers_df[
                "customer_zip_code_prefix"
            ]

            ==

            geo_df[
                "geolocation_zip_code_prefix"
            ],
            "left"
        )
        .select(
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
            "geolocation_lat",
            "geolocation_lng"
        )
    )

    print("Preparing and building seller_proximity...")

    seller_proximity = (
        orders_df

        .join(
            items_df,
            "order_id",
            "left"
        )

        .join(
            customers_df,
            "customer_id",
            "left"
        )

        .join(
            sellers_df,
            "seller_id",
            "left"
        )

        .select(
            "order_id",
            "customer_id",
            "seller_id",

            "customer_state",
            "seller_state",

            "price",
            "freight_value"
        )
    )

    seller_proximity = (
        seller_proximity
        .withColumn(
            "same_state",
            F.when(
                F.col("customer_state")
                ==
                F.col("seller_state"),
                1
            ).otherwise(0)
        )
    )

    processed_dir = (
        "/opt/airflow/data_lake/processed"
    )

    os.makedirs(
        processed_dir,
        exist_ok=True
    )

    print("Saving processed layer...")

    analytics_orders.write \
        .mode("overwrite") \
        .parquet(
            f"{processed_dir}/analytics_orders"
        )

    customer_geo.write \
        .mode("overwrite") \
        .parquet(
            f"{processed_dir}/customer_geo"
        )

    seller_proximity.write \
        .mode("overwrite") \
        .parquet(
            f"{processed_dir}/seller_proximity"
        )

    spark.stop()

if __name__ == "__main__":
    process_dustiniadelixia()