from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def build_payment_features():

    spark = (
        SparkSession.builder
        .appName("PaymentFeatures")
        .getOrCreate()
    )

    raw_dir = "/opt/airflow/data_lake/raw"

    payments_df = spark.read.parquet(
        f"{raw_dir}/order_payments/*.parquet"
    )

    # aggregate payment

    payment_features = (
        payments_df
        .groupBy("order_id")
        .agg(

            F.count("*")
            .alias(
                "payment_count_per_order"
            ),

            F.sum(
                "payment_value"
            ).alias(
                "payment_value"
            ),

            F.max(
                "payment_installments"
            ).alias(
                "payment_installments"
            ),

            F.first(
                "payment_type"
            ).alias(
                "payment_type"
            )
        )
    )

    # multi_payment_flag

    payment_features = (
        payment_features
        .withColumn(
            "multi_payment_flag",

            F.when(
                F.col(
                    "payment_count_per_order"
                ) > 1,
                1
            ).otherwise(0)
        )
    )

    # installment_group

    payment_features = (
        payment_features
        .withColumn(
            "installment_group",

            F.when(
                F.col(
                    "payment_installments"
                ) == 1,
                "1"
            )

            .when(
                F.col(
                    "payment_installments"
                ).between(2, 6),
                "2-6"
            )

            .when(
                F.col(
                    "payment_installments"
                ).between(7, 12),
                "7-12"
            )

            .otherwise(
                "12+"
            )
        )
    )

    # high_value_payment_flag

    q95 = (
        payment_features
        .approxQuantile(
            "payment_value",
            [0.95],
            0.01
        )[0]
    )

    payment_features = (
        payment_features
        .withColumn(
            "high_value_payment_flag",

            F.when(
                F.col(
                    "payment_value"
                ) > q95,
                1
            ).otherwise(0)
        )
    )

    output_path = (
        "/opt/airflow/data_lake/features/payment_features"
    )

    (
        payment_features
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    spark.stop()

if __name__ == "__main__":
    build_payment_features()