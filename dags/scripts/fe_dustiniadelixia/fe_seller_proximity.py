from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_seller_features():

    spark = (
        SparkSession.builder
        .appName("SellerFeatures")
        .getOrCreate()
    )

    seller_df = spark.read.parquet(
        "/opt/airflow/data_lake/processed/seller_proximity"
    )

    # seller count per order

    seller_count_df = (
        seller_df
        .groupBy("order_id")
        .agg(
            F.countDistinct(
                "seller_id"
            ).alias(
                "seller_count_per_order"
            )
        )
    )

    seller_df = (
        seller_df
        .join(
            seller_count_df,
            "order_id",
            "left"
        )
    )

    # multi seller order flag

    seller_df = (
        seller_df
        .withColumn(
            "multi_seller_order_flag",

            F.when(
                F.col(
                    "seller_count_per_order"
                ) > 1,
                1
            ).otherwise(0)
        )
    )

    # interstate order

    seller_df = (
        seller_df
        .withColumn(
            "interstate_order",

            F.when(
                F.col("same_state") == 0,
                1
            ).otherwise(0)
        )
    )

    # freight ratio

    seller_df = (
        seller_df
        .withColumn(
            "freight_ratio",

            F.when(
                F.col("price") > 0,

                F.round(
                    F.col("freight_value")
                    /
                    F.col("price"),
                    4
                )
            )
        )
    )

    # high freight flag

    seller_df = (
        seller_df
        .withColumn(
            "high_freight_flag",

            F.when(
                F.col("freight_ratio") > 0.30,
                1
            ).otherwise(0)
        )
    )

    # freight bucket

    seller_df = (
        seller_df
        .withColumn(
            "freight_bucket",

            F.when(
                F.col("freight_ratio") <= 0.10,
                "Low"
            )

            .when(
                F.col("freight_ratio") <= 0.30,
                "Medium"
            )

            .otherwise(
                "High"
            )
        )
    )

    # keep useful columns

    seller_df = (
        seller_df
        .select(
            "order_id",
            "customer_id",
            "seller_id",

            "customer_state",
            "seller_state",

            "same_state",
            "interstate_order",

            "seller_count_per_order",
            "multi_seller_order_flag",

            "price",
            "freight_value",
            "freight_ratio",

            "high_freight_flag",
            "freight_bucket"
        )
    )

    output_path = (
        "/opt/airflow/data_lake/features/seller_features"
    )

    (
        seller_df
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    spark.stop()


if __name__ == "__main__":
    build_seller_features()