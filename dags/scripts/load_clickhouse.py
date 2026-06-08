from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from clickhouse_driver import Client

import pandas as pd


def normalize_spark_df(spark_df):
    for field in spark_df.schema.fields:
        if isinstance(field.dataType, (T.TimestampType, T.DateType)):
            spark_df = spark_df.withColumn(
                field.name,
                F.when(
                    F.col(field.name).isNull(),
                    F.lit(None).cast(T.StringType())
                ).otherwise(
                    F.date_format(
                        F.col(field.name),
                        "yyyy-MM-dd HH:mm:ss"
                    )
                )
            )
    return spark_df


def overwrite_table(client, spark_df, table_name):
    print(f"Loading {table_name}")

    # datetime ke string di Spark
    spark_df = normalize_spark_df(spark_df)

    # abis itu ke pandas
    pdf = spark_df.toPandas()

    # cukup tangkap None/NaN/NA 
    def to_native(x):
        if x is None:
            return None
        try:
            if pd.isna(x):
                return None
        except (TypeError, ValueError):
            pass
        if isinstance(x, float) and x == int(x):
            return int(x)
        if hasattr(x, "item"):
            return x.item()
        return x

    for col in pdf.columns:
        pdf[col] = pdf[col].apply(to_native)

    rows = list(pdf.itertuples(index=False, name=None))

    client.execute(f"TRUNCATE TABLE analytics.{table_name}")

    if rows:
        client.execute(
            f"INSERT INTO analytics.{table_name} VALUES",
            rows
        )

    print(f"Done {table_name}: {len(rows)} rows")


def create_tables(client):

    for table in [
        "customer_features",
        "delivery_features",
        "payment_features",
        "nlp_features",
        "geo_features",
        "seller_features",
        "cx_features",
    ]:
        client.execute(f"DROP TABLE IF EXISTS analytics.{table}")

    print("Creating ClickHouse tables...")

    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.customer_features
        (
            customer_unique_id      String,
            frequency               UInt32,
            monetary                Float64,
            avg_order_value         Float64,
            first_purchase_date     Nullable(String),
            last_purchase_date      Nullable(String),
            avg_review_score        Nullable(Float64),
            preferred_payment_type  String,
            recency_days            Int32,
            customer_tenure_days    Int32,
            repeat_customer         UInt8
        )
        ENGINE = MergeTree()
        ORDER BY customer_unique_id;
    ''')

    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.delivery_features
        (
            order_id                        String,
            customer_id                     Nullable(String),
            customer_unique_id              Nullable(String),
            customer_state                  Nullable(String),
            customer_city                   Nullable(String),
            order_delivered_customer_date   Nullable(String),
            order_purchase_timestamp        Nullable(String),
            order_estimated_delivery_date   Nullable(String),
            order_status                    Nullable(String),
            payment_type                    Nullable(String),
            review_score                    Nullable(Float64),
            actual_delivery_days            Nullable(Float64),
            delivery_delay_days             Nullable(Float64),
            delay_bucket                    Nullable(String),
            purchase_month                  Nullable(String),
            cancellation_flag               UInt8,
            purchase_count_per_customer     UInt32,
            repeat_customer                 UInt8
        )
        ENGINE = MergeTree()
        ORDER BY order_id;
    ''')

    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.payment_features
        (
            order_id                String,
            payment_count_per_order UInt32,
            payment_value           Float64,
            payment_installments    UInt32,
            payment_type            Nullable(String),
            multi_payment_flag      UInt8,
            installment_group       Nullable(String),
            high_value_payment_flag UInt8
        )
        ENGINE = MergeTree()
        ORDER BY order_id;
    ''')

    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.nlp_features
        (
            order_id                String,
            review_score            Nullable(Float64),
            review_comment_message  Nullable(String),
            review_text_clean       Nullable(String),
            has_text                UInt8,
            complaint_topic         Nullable(String)
        )
        ENGINE = MergeTree()
        ORDER BY order_id;
    ''')

    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.geo_features
        (
            customer_id              String,
            customer_unique_id       String,
            customer_zip_code_prefix Nullable(Float64),
            customer_city            Nullable(String),
            customer_state           Nullable(String),
            geolocation_lat          Nullable(Float64),
            geolocation_lng          Nullable(Float64)
        )
        ENGINE = MergeTree()
        ORDER BY customer_unique_id;
    ''')

    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.seller_features
        (
            order_id                String,
            customer_id             Nullable(String),
            seller_id               Nullable(String),
            customer_state          Nullable(String),
            seller_state            Nullable(String),
            same_state              Nullable(Float64),
            interstate_order        Nullable(Float64),
            seller_count_per_order  Nullable(Float64),
            multi_seller_order_flag Nullable(Float64),
            price                   Nullable(Float64),
            freight_value           Nullable(Float64),
            freight_ratio           Nullable(Float64),
            high_freight_flag       Nullable(Float64),
            freight_bucket          Nullable(String)
        )
        ENGINE = MergeTree()
        ORDER BY order_id;
    ''')

    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.cx_features
        (
            customer_unique_id          String,
            avg_review_score            Nullable(Float64),
            avg_delivery_penalty_days   Nullable(Float64),
            late_delivery_rate          Nullable(Float64),
            complaint_rate              Nullable(Float64),
            frequency                   UInt32,
            monetary                    Float64,
            recency_days                Int32,
            customer_tenure_days        Int32,
            repeat_customer             UInt8,
            review_component            Nullable(Float64),
            delivery_component          Nullable(Float64),
            complaint_component         Nullable(Float64),
            frequency_score             Float64,
            tenure_score                Float64,
            recency_score               Float64,
            loyalty_component           Float64,
            cx_score                    Nullable(Float64),
            cx_category                 String
        )
        ENGINE = MergeTree()
        ORDER BY customer_unique_id;
    ''')


def load_clickhouse():
    spark = (
        SparkSession.builder
        .appName("LoadClickHouse")
        .getOrCreate()
    )

    try:
        client = Client(
            host="clickhouse-server",
            user="admin",
            password="rahasia"
        )

        client.execute("CREATE DATABASE IF NOT EXISTS analytics")
        create_tables(client)

        print("Reading parquet files...")

        customer_df = spark.read.parquet(
            "/opt/airflow/data_lake/features/customer_features"
        )
        delivery_df = spark.read.parquet(
            "/opt/airflow/data_lake/features/delivery_features"
        )
        payment_df = spark.read.parquet(
            "/opt/airflow/data_lake/features/payment_features"
        )
        nlp_df = spark.read.parquet(
            "/opt/airflow/data_lake/features/nlp_features"
        )
        seller_df = spark.read.parquet(
            "/opt/airflow/data_lake/features/seller_features"
        )
        cx_df = spark.read.parquet(
            "/opt/airflow/data_lake/features/cx_features"
        )
        geo_df = spark.read.parquet(
            "/opt/airflow/data_lake/processed/customer_geo"
        )

        print("Loading feature tables...")

        overwrite_table(client, customer_df, "customer_features")
        overwrite_table(client, delivery_df, "delivery_features")
        overwrite_table(client, payment_df,  "payment_features")
        overwrite_table(client, nlp_df,      "nlp_features")
        overwrite_table(client, seller_df,   "seller_features")
        overwrite_table(client, cx_df,       "cx_features")
        overwrite_table(client, geo_df,      "geo_features")

    finally:
        spark.stop()


if __name__ == "__main__":
    load_clickhouse()