from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_nlp_features():

    spark = (
        SparkSession.builder
        .appName("NLPFeatures")
        .getOrCreate()
    )

    processed_dir = (
        "data_lake/processed"
    )

    analytics_orders = spark.read.parquet(
        f"{processed_dir}/analytics_orders"
    )

    nlp_df = (
        analytics_orders
        .select(
            "order_id",
            "review_score",
            "review_comment_message"
        )
    )

    # ==========================
    # review_text_clean
    # ==========================

    nlp_df = (
        nlp_df
        .withColumn(
            "review_text_clean",

            F.lower(
                F.coalesce(
                    F.col(
                        "review_comment_message"
                    ),
                    F.lit("")
                )
            )
        )
    )

    # remove urls

    nlp_df = (
        nlp_df
        .withColumn(
            "review_text_clean",

            F.regexp_replace(
                "review_text_clean",
                r"http\S+",
                ""
            )
        )
    )

    # remove punctuation

    nlp_df = (
        nlp_df
        .withColumn(
            "review_text_clean",

            F.regexp_replace(
                "review_text_clean",
                r"[^a-zA-ZÀ-ÿ0-9\s]",
                " "
            )
        )
    )

    # remove extra spaces

    nlp_df = (
        nlp_df
        .withColumn(
            "review_text_clean",

            F.trim(
                F.regexp_replace(
                    "review_text_clean",
                    r"\s+",
                    " "
                )
            )
        )
    )

    # ==========================
    # has_text
    # ==========================

    nlp_df = (
        nlp_df
        .withColumn(
            "has_text",

            F.when(
                F.length(
                    "review_text_clean"
                ) > 0,
                1
            ).otherwise(0)
        )
    )

    DELIVERY_REGEX = (
        "delivery|delay|late|"
        "entrega|atraso|demora|"
        "prazo|recebi|recebido"
    )

    REFUND_REGEX = (
        "refund|money|"
        "reembolso|devolucao|"
        "devolver|estorno"
    )

    DAMAGED_REGEX = (
        "damage|broken|defect|"
        "quebrado|defeito|"
        "danificado|avariado"
    )

    PACKAGING_REGEX = (
        "package|packaging|"
        "embalagem|caixa|pacote"
    )

    SELLER_REGEX = (
        "seller|service|"
        "vendedor|atendimento|"
        "suporte|contato"
    )

    # ==========================
    # complaint_topic
    # ==========================

    nlp_df = (
        nlp_df
        .withColumn(

            "complaint_topic",

            F.when(
                F.col(
                    "review_text_clean"
                ).rlike(
                    DELIVERY_REGEX
                ),
                "delivery"
            )

            .when(
                F.col(
                    "review_text_clean"
                ).rlike(
                    REFUND_REGEX
                ),
                "refund"
            )

            .when(
                F.col(
                    "review_text_clean"
                ).rlike(
                    DAMAGED_REGEX
                ),
                "damaged_product"
            )

            .when(
                F.col(
                    "review_text_clean"
                ).rlike(
                    PACKAGING_REGEX
                ),
                "packaging"
            )

            .when(
                F.col(
                    "review_text_clean"
                ).rlike(
                    SELLER_REGEX
                ),
                "seller_communication"
            )

            .otherwise(
                "other"
            )
        )
    )

    output_path = (
        "data_lake/features/nlp_features"
    )

    (
        nlp_df
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    spark.stop()


if __name__ == "__main__":
    build_nlp_features()