from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "ibrahimferel",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="dustiniadelixia_pipeline",
    default_args=default_args,
    schedule="@once",
    catchup=False
) as dag:

    fetch_data = BashOperator(
        task_id="fetch_data",
        bash_command="python /opt/airflow/dags/scripts/fetch_dustiniadelixia_stream.py"
    )

    process_data = BashOperator(
        task_id="process_data",
        bash_command="python /opt/airflow/dags/scripts/process_dustiniadelixia_spark.py"
    )

    customer_features = BashOperator(
        task_id="customer_features",
        bash_command="python /opt/airflow/dags/scripts/fe_dustiniadelixia/fe_customer_segmentation.py"
    )

    delivery_features = BashOperator(
        task_id="delivery_features",
        bash_command="python /opt/airflow/dags/scripts/fe_dustiniadelixia/fe_delivery.py"
    )

    payment_features = BashOperator(
        task_id="payment_features",
        bash_command="python /opt/airflow/dags/scripts/fe_dustiniadelixia/fe_payment.py"
    )

    seller_features = BashOperator(
        task_id="seller_features",
        bash_command="python /opt/airflow/dags/scripts/fe_dustiniadelixia/fe_seller_proximity.py"
    )

    nlp_features = BashOperator(
        task_id="nlp_features",
        bash_command="python /opt/airflow/dags/scripts/fe_dustiniadelixia/fe_nlp.py"
    )

    cx_features = BashOperator(
        task_id="cx_features",
        bash_command="python /opt/airflow/dags/scripts/fe_dustiniadelixia/fe_cx.py"
    )

    load_clickhouse = BashOperator(
        task_id="load_clickhouse",
        bash_command="python /opt/airflow/dags/scripts/load_clickhouse.py"
    )

    fetch_data >> process_data

    process_data >> [
        customer_features,
        delivery_features,
        payment_features,
        seller_features,
        nlp_features
    ]

    [
        customer_features,
        delivery_features,
        nlp_features
    ] >> cx_features

    [
        customer_features,
        delivery_features,
        payment_features,
        seller_features,
        nlp_features,
        cx_features,
        process_data
    ] >> load_clickhouse