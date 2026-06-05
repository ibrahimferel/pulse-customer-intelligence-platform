from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_customer_features():

    spark = (
        SparkSession.builder
        .appName("CustomerFeatures")
        .getOrCreate()
    )

    processed_dir = (
        "data_lake/processed"
    )

    analytics_orders = spark.read.parquet(
        f"{processed_dir}/analytics_orders"
    )

    # ==========================
    # delivered orders only
    # ==========================

    customer_df = (
        analytics_orders
        .filter(
            F.col("order_status") == "delivered"
        )
    )

    # ==========================
    # customer aggregation
    # ==========================

    customer_features = (
        customer_df
        .groupBy(
            "customer_unique_id"
        )
        .agg(

            F.countDistinct(
                "order_id"
            ).alias(
                "frequency"
            ),

            F.sum(
                "payment_value"
            ).alias(
                "monetary"
            ),

            F.avg(
                "payment_value"
            ).alias(
                "avg_order_value"
            ),

            F.min(
                "order_purchase_timestamp"
            ).alias(
                "first_purchase_date"
            ),

            F.max(
                "order_purchase_timestamp"
            ).alias(
                "last_purchase_date"
            ),

            F.avg(
                "review_score"
            ).alias(
                "avg_review_score"
            ),

            F.first(
                "payment_type"
            ).alias(
                "preferred_payment_type"
            )
        )
    )

    # ==========================
    # recency
    # ==========================

    max_date = (
        customer_df
        .agg(
            F.max(
                "order_purchase_timestamp"
            )
        )
        .collect()[0][0]
    )

    customer_features = (
        customer_features
        .withColumn(
            "recency_days",

            F.datediff(
                F.lit(max_date),
                F.col(
                    "last_purchase_date"
                )
            )
        )
    )

    # ==========================
    # tenure
    # ==========================

    customer_features = (
        customer_features
        .withColumn(
            "customer_tenure_days",

            F.datediff(
                F.col(
                    "last_purchase_date"
                ),
                F.col(
                    "first_purchase_date"
                )
            )
        )
    )

    # ==========================
    # repeat customer
    # ==========================

    customer_features = (
        customer_features
        .withColumn(
            "repeat_customer",

            F.when(
                F.col(
                    "frequency"
                ) > 1,
                1
            ).otherwise(0)
        )
    )

    customer_features = (
        customer_features
        .fillna({
            "preferred_payment_type": "unknown"
        })
    )

    output_path = (
        "data_lake/features/customer_features"
    )

    (
        customer_features
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    spark.stop()


if __name__ == "__main__":
    build_customer_features()