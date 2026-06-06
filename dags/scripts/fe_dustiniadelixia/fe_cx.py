from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_cx_features():

    spark = (
        SparkSession.builder
        .appName("CXFeatures")
        .getOrCreate()
    )

    # read features

    delivery_df = spark.read.parquet(
        "/opt/airflow/data_lake/features/delivery_features"
    )

    nlp_df = spark.read.parquet(
        "/opt/airflow/data_lake/features/nlp_features"
    )

    customer_df = spark.read.parquet(
        "/opt/airflow/data_lake/features/customer_features"
    )

    analytics_orders = spark.read.parquet(
        "/opt/airflow/data_lake/processed/analytics_orders"
    )

    # keep required columns

    nlp_df = (
        nlp_df
        .select(
            "order_id",
            "complaint_topic"
        )
    )

    customer_df = (
        customer_df
        .select(
            "customer_unique_id",
            "frequency",
            "monetary",
            "recency_days",
            "customer_tenure_days",
            "repeat_customer"
        )
    )

    customer_lookup = (
        analytics_orders
        .select(
            "order_id",
            "customer_unique_id"
        )
        .dropDuplicates()
    )

    # join all

    cx_df = (
        delivery_df

        .join(
            nlp_df,
            "order_id",
            "left"
        )

        .join(
            customer_lookup,
            "order_id",
            "left"
        )
    )

    # late delivery flag

    cx_df = (
        cx_df
        .withColumn(
            "is_late_delivery",

            F.when(
                F.col(
                    "delivery_delay_days"
                ) > 0,
                1
            ).otherwise(0)
        )
    )

    # complaint flag

    cx_df = (
        cx_df
        .withColumn(
            "has_complaint",

            F.when(
                F.col(
                    "complaint_topic"
                ) != "other",
                1
            ).otherwise(0)
        )
    )

    # fill missing values

    cx_df = (
        cx_df
        .withColumn(
            "delivery_penalty_days",

            F.when(
                F.col("delivery_delay_days") > 0,
                F.col("delivery_delay_days")
            ).otherwise(0)
        )
    )

    # customer aggregation

    customer_cx = (
        cx_df

        .groupBy(
            "customer_unique_id"
        )

        .agg(

            F.avg(
                "review_score"
            ).alias(
                "avg_review_score"
            ),

            F.avg(
                "delivery_penalty_days"
            ).alias(
                "avg_delivery_penalty_days"
            ),

            F.avg(
                "is_late_delivery"
            ).alias(
                "late_delivery_rate"
            ),

            F.avg(
                "has_complaint"
            ).alias(
                "complaint_rate"
            )
        )
    )

    customer_cx = (
        customer_cx
        .join(
            customer_df,
            "customer_unique_id",
            "inner"
        )
    )

    customer_cx = (
        customer_cx

        # review

        .withColumn(
            "review_component",
            F.col(
                "avg_review_score"
            ) / 5.0
        )

        # delivery

        .withColumn(
            "delivery_component",

            F.when(
                F.col(
                    "avg_delivery_penalty_days"
                ) <= 0,
                1.0
            )

            .when(
                F.col(
                    "avg_delivery_penalty_days"
                ) <= 3,
                0.8
            )

            .when(
                F.col(
                    "avg_delivery_penalty_days"
                ) <= 7,
                0.6
            )

            .otherwise(0.3)
        )

        # complaint

        .withColumn(
            "complaint_component",

            1 - F.col(
                "complaint_rate"
            )
        )

        # loyalty sub-scores        

        .withColumn(
            "frequency_score",

            F.when(
                F.col("frequency") >= 3,
                1.0
            )

            .when(
                F.col("frequency") == 2,
                0.7
            )

            .otherwise(0.3)
        )

        .withColumn(
            "tenure_score",

            F.when(
                F.col("customer_tenure_days") >= 180,
                1.0
            )

            .when(
                F.col("customer_tenure_days") >= 90,
                0.8
            )

            .when(
                F.col("customer_tenure_days") >= 30,
                0.6
            )

            .when(
                F.col("customer_tenure_days") >= 7,
                0.4
            )

            .otherwise(0.2)
        )

        .withColumn(
            "recency_score",

            F.when(
                F.col("recency_days") <= 30,
                1.0
            )

            .when(
                F.col("recency_days") <= 90,
                0.8
            )

            .when(
                F.col("recency_days") <= 180,
                0.6
            )

            .when(
                F.col("recency_days") <= 365,
                0.4
            )

            .otherwise(0.2)
        )

        # loyalty

        .withColumn(
            "loyalty_component",

            (
                F.col("frequency_score") * 0.5
                +
                F.col("tenure_score") * 0.3
                +
                F.col("recency_score") * 0.2
            )
        )

        # cx score

        .withColumn(
            "cx_score",

            (
                F.col("review_component")    * 0.4
                +
                F.col("delivery_component")  * 0.3
                +
                F.col("complaint_component") * 0.2
                +
                F.col("loyalty_component")   * 0.1
            ) * 100
        )
    )

    # cx category

    customer_cx = (
        customer_cx
        .withColumn(
            "cx_category",

            F.when(
                F.col(
                    "cx_score"
                ) >= 90,
                "Excellent"
            )

            .when(
                F.col(
                    "cx_score"
                ) >= 70,
                "Good"
            )

            .when(
                F.col(
                    "cx_score"
                ) >= 50,
                "Fair"
            )

            .otherwise(
                "Poor"
            )
        )
    )

    (
        customer_cx
        .write
        .mode("overwrite")
        .parquet(
            "/opt/airflow/data_lake/features/cx_features"
        )
    )

    spark.stop()


if __name__ == "__main__":
    build_cx_features()