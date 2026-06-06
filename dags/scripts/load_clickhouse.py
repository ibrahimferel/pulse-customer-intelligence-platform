from pyspark.sql import SparkSession
from clickhouse_driver import Client

import pandas as pd

def overwrite_table(
        client,
        spark_df,
        table_name
    ):
        print(
            f"Loading {table_name}"
        )

        pdf = spark_df.toPandas()

        pdf = pdf.where(
            pd.notnull(pdf),
            None
        )

        rows = list(
            pdf.itertuples(
                index=False,
                name=None
            )
        )

        client.execute(
            f"TRUNCATE TABLE analytics.{table_name}"
        )

        if rows:

            client.execute(
                f"""
                INSERT INTO analytics.{table_name}
                VALUES
                """,
                rows
            )

        print(
            f"Done {table_name}: {len(rows)} rows"
        )    

def create_tables(client):
        
        print("Creating ClickHouse tables...")

        client.execute('''
            CREATE TABLE IF NOT EXISTS analytics.customer_features
            (
                customer_unique_id String,
                frequency UInt32,
                monetary Float64,
                avg_order_value Float64,
                first_purchase_date Nullable(DateTime),
                last_purchase_date Nullable(DateTime),
                avg_review_score Nullable(Float64),
                preferred_payment_type String,
                recency_days Int32,
                customer_tenure_days Int32,
                repeat_customer UInt8
            )
            ENGINE = MergeTree()
            ORDER BY customer_unique_id;
        ''')
        client.execute('''
            CREATE TABLE IF NOT EXISTS analytics.delivery_features
            (
                order_id String,
                customer_id String,
                customer_state String,
                customer_city String,
                order_delivered_customer_date Nullable(DateTime),
                order_purchase_timestamp Nullable(DateTime),
                order_estimated_delivery_date Nullable(DateTime),
                order_status String,
                payment_type String,
                review_score Int32,
                actual_delivery_days Int32,
                delivery_delay_days Int32,
                delay_bucket String,
                purchase_month String,
                cancellation_flag UInt8,
                purchase_count_per_customer UInt32,
                repeat_customer UInt8
            )
            ENGINE = MergeTree()
            ORDER BY order_id;
        ''')
        client.execute('''
            CREATE TABLE IF NOT EXISTS analytics.payment_features
            (
                order_id String,
                payment_count_per_order UInt32,
                payment_value Float64,
                payment_installments UInt32,
                payment_type String,
                multi_payment_flag UInt8,
                installment_group String,
                high_value_payment_flag UInt8
            )
            ENGINE = MergeTree()
            ORDER BY order_id;
        ''')
        client.execute('''
            CREATE TABLE IF NOT EXISTS analytics.nlp_features
            (
                order_id String,
                review_score Int32,
                review_comment_message String,
                review_text_clean String,
                has_text UInt8,
                complaint_topic String
            )
            ENGINE = MergeTree()
            ORDER BY order_id;
        ''')
        client.execute('''
            CREATE TABLE IF NOT EXISTS analytics.geo_features
            (
                customer_id String,
                customer_unique_id String,
                customer_zip_code_prefix UInt32,
                customer_city String,
                customer_state String,
                geolocation_lat Nullable(Float64),
                geolocation_lng Nullable(Float64)
            )
            ENGINE = MergeTree()
            ORDER BY customer_unique_id;
        ''')
        client.execute('''
            CREATE TABLE IF NOT EXISTS analytics.seller_features
            (
                order_id String,
                customer_id String,
                seller_id String,
                customer_state String,
                seller_state String,
                same_state UInt8,
                interstate_order UInt8,
                seller_count_per_order UInt32,
                multi_seller_order_flag UInt8,
                price Float64,
                freight_value Float64,
                freight_ratio Float64,
                high_freight_flag UInt8,
                freight_bucket String
            )
            ENGINE = MergeTree()
            ORDER BY (order_id, seller_id);
        ''')
        client.execute('''
            CREATE TABLE IF NOT EXISTS analytics.cx_features
            (
                customer_unique_id String,
                avg_review_score Float64,
                avg_delivery_penalty_days Float64,
                late_delivery_rate Float64,
                complaint_rate Float64,
                frequency UInt32,
                monetary Float64,
                recency_days Int32,
                customer_tenure_days Int32,
                repeat_customer UInt8,
                review_component Float64,
                delivery_component Float64,
                complaint_component Float64,
                frequency_score Float64,
                tenure_score Float64,
                recency_score Float64,
                loyalty_component Float64,
                cx_score Float64,
                cx_category String
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

            client.execute(
                "CREATE DATABASE IF NOT EXISTS analytics"
            )

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
            overwrite_table(client, payment_df, "payment_features")
            overwrite_table(client, nlp_df, "nlp_features")
            overwrite_table(client, seller_df, "seller_features")
            overwrite_table(client, cx_df, "cx_features")
            overwrite_table(client, geo_df, "geo_features")

        finally:
            spark.stop()


if __name__ == "__main__":
        load_clickhouse()        