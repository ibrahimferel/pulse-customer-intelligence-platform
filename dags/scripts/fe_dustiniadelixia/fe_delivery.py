from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import os

def build_delivery_features():
    spark = (
        SparkSession.builder
        .appName(
            "DeliveryFeatures"
        )
        .getOrCreate()
    )

    print(
        "Reading analytics_orders..."
    )

    analytics_orders = (
        spark.read.parquet(
            "/opt/airflow/data_lake/processed/analytics_orders"
        )
    )

    print(
        "Building delivery features..."
    )

    delivery_df = analytics_orders

    # actual_delivery_days

    delivery_df = (
        delivery_df
        .withColumn(
            "actual_delivery_days",
            F.datediff(
                F.col(
                    "order_delivered_customer_date"
                ),
                F.col(
                    "order_purchase_timestamp"
                )
            )
        )
    )

    # delivery_delay_days

    delivery_df = (
        delivery_df
        .withColumn(
            "delivery_delay_days",
            F.datediff(
                F.col(
                    "order_delivered_customer_date"
                ),
                F.col(
                    "order_estimated_delivery_date"
                )
            )
        )
    )

    # delay_bucket

    delivery_df = (
        delivery_df
        .withColumn(
            "delay_bucket",

            F.when(
                F.col(
                    "delivery_delay_days"
                ) <= 0,
                "On Time"
            )

            .when(
                F.col(
                    "delivery_delay_days"
                ) <= 3,
                "1-3 Days Late"
            )

            .when(
                F.col(
                    "delivery_delay_days"
                ) <= 7,
                "4-7 Days Late"
            )

            .otherwise(
                "Critical Delay"
            )
        )
    )

    # purchase_month

    delivery_df = (
        delivery_df
        .withColumn(
            "purchase_month",
            F.date_format(
                "order_purchase_timestamp",
                "yyyy-MM"
            )
        )
    )

    # cancellation_flag

    delivery_df = (
        delivery_df
        .withColumn(
            "cancellation_flag",

            F.when(
                F.col(
                    "order_status"
                ).isin(
                    "canceled",
                    "unavailable"
                ),
                1
            )
            .otherwise(0)
        )
    )

    # purchase_count_per_customer

    purchase_count = (
        delivery_df
        .groupBy(
            "customer_unique_id"
        )
        .agg(
            F.countDistinct(
                "order_id"
            )
            .alias(
                "purchase_count_per_customer"
            )
        )
    )

    delivery_df = (
        delivery_df
        .join(
            purchase_count,
            "customer_id",
            "left"
        )
    )

    # repeat_customer

    delivery_df = (
        delivery_df
        .withColumn(
            "repeat_customer",

            F.when(
                F.col(
                    "purchase_count_per_customer"
                ) > 1,
                1
            )
            .otherwise(0)
        )
    )

    delivery_df = delivery_df.select(
        "order_id",
        "customer_id",
        "customer_state",
        "customer_city",
        "order_delivered_customer_date",
        "order_purchase_timestamp",
        "order_estimated_delivery_date",
        "order_status",
        "payment_type",
        "review_score",
        "actual_delivery_days",
        "delivery_delay_days",
        "delay_bucket",
        "purchase_month",
        "cancellation_flag",
        "purchase_count_per_customer",
        "repeat_customer"
    )

    output_dir = (
        "/opt/airflow/data_lake/features/delivery_features"
    )

    os.makedirs(
        output_dir,
        exist_ok=True
    )

    print(
        "Saving delivery features..."
    )

    (
        delivery_df
        .write
        .mode(
            "overwrite"
        )
        .parquet(
            output_dir
        )
    )

    print(
        "Delivery feature engineering completed."
    )

    spark.stop()


if __name__ == "__main__":
    build_delivery_features()