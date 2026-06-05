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
        "delivery|delay|late|shipped|transit|tracking|dispatch|carrier|courier|"
        "entrega|atraso|demora|prazo|recebi|recebido|"
        "não chegou|nao chegou|não recebi|nao recebi|chegou|chegada|"
        "enviado|enviaram|enviou|envio|correio|transportadora|"
        "postado|saiu|rastreio|rastreamento|extraviado|devolvido ao remetente|"
        "frete|pendente de entrega|fora do prazo|"
        "atrasada|atrasado|demorou|demorado|aguardando|"
        "não entregue|nao entregue|não recebido|nao recebido"
    )

    REFUND_REGEX = (
        "refund|money|cancel|chargeback|"
        "reembolso|devolucao|devolver|estorno|"
        "cancelar|cancelamento|cancelei|dinheiro de volta|devolução|"
        "devolvi|devolvendo|reembolsar|ressarcimento|ressarcir|estornar|"
        "aguardo estorno|cartão|crédito|pix|pagamento|cobrado|cobrado errado|"
        "trocar|troca|substituir|substituição|substituicao"
    )

    DAMAGED_REGEX = (
        "damage|broken|defect|fault|crack|defective|malfunction|burned|"
        "quebrado|defeito|danificado|avariado|"
        "trincado|riscado|amassado|rachado|arranhado|estragado|inutilizável|"
        "não funciona|nao funciona|parou de funcionar|veio com defeito|"
        "veio quebrado|produto com defeito|falha|falhou|não liga|nao liga|"
        "não acende|nao acende|não funcional|falso|pirata|"
        "errado|diferente|faltando peça|faltou peça|faltou parafuso|"
        "incompleto|peça faltando|peca faltando|produto falso|não original|nao original"
    )

    PACKAGING_REGEX = (
        "package|packaging|box|protection|"
        "embalagem|caixa|pacote|"
        "sem proteção|sem embalagem|mal embalado|embalagem danificada|"
        "caixa amassada|caixa rasgada|caixa aberta|embalagem aberta|"
        "sem caixa|sem manual|nota fiscal|lacre|plástico bolha|"
        "bem embalado|embalagem original|invólucro|"
        "embalado|embalada|amassada|rasgada|violada|sem protecao|protegido"
    )

    SELLER_REGEX = (
        "seller|service|communication|response|"
        "vendedor|atendimento|suporte|contato|"
        "loja|assistência|SAC|chat|whatsapp|email|e-mail|"
        "não respondeu|nao respondeu|sem resposta|ligação|telefone|"
        "ignorado|descaso|prazo de resposta|demora na resposta|promessa|garantia|"
        "retorno|retornar|responder|respondeu|resposta|"
        "informaram|informação|informacao|atendente|comunicação|comunicacao"
    )

    PRODUCT_REGEX = (
        "wrong product|different product|missing item|"
        "produto errado|produto diferente|"
        "veio diferente|veio outro|modelo diferente|"
        "cor errada|tamanho errado|"
        "faltou|faltando|incompleto|"
        "veio apenas|recebi apenas|"
        "não corresponde|nao corresponde|"
        "diferente do anúncio|diferente do anunciado|"
        "produto falso|falsificado|pirata|"
        "peça faltando|faltam peças|"
        "acessório faltando|item faltando|"
        "quantidade errada|veio menos|"
        "veio uma unidade|veio só uma|"
        "não era o que pedi|nao era o que pedi|"
        "produto trocado|trocaram meu produto|"
        "cor diferente|tamanho diferente"
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

            .when(
                F.col(
                    "review_text_clean"
                ).rlike(
                    PRODUCT_REGEX
                ),
                "product_issue"
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