from pyspark.sql import SparkSession
import logging
import json
from config import GCS_BUCKET
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col

from manage_offset import load_last_offset, save_last_offset
from utils import convert_debezium_schema_to_spark

# Configura o logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Inicializa a sessão do Spark
spark = SparkSession.builder.appName("CDC Batch Ingestion").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
# Tópicos e partições a serem processados
topics = [
    "postgres.public.albums",
    "postgres.public.albums_genres",
    "postgres.public.artists",
    "postgres.public.genres",
    "postgres.public.tracks",
]
partition = 0  # Assumindo que cada tópico tem apenas uma partição

# Processa cada tópico
for topic in topics:

    logger.info(f"Processando tópico: {topic}")

    # Carrega o último offset do tópico
    last_offset = load_last_offset(topic, partition)
    logger.info(f"Último offset carregado para {topic}: {last_offset}")

    # Configurações do Kafka
    kafka_options = {
        "kafka.bootstrap.servers": "kafka:9092",
        "subscribe": topic,
        "startingOffsets": (
            "earliest" if last_offset is None else f'{{"{topic}": {{"0": {last_offset}}}}}'
        ),  # Começa do último offset
        "endingOffsets": "latest",  # Consome até o offset mais recente
    }
    # Lê as mensagens do Kafka
    df = spark.read.format("kafka").options(**kafka_options).load()

    logger.info(f"Número de mensagens lidas do tópico {topic}: {df.count()}")

    # Processa as mensagens
    if df.count() > 0:

        # Converte a coluna `value` para string
        df = df.select(col("offset"), col("value").cast("string").alias("value"))

        # Define um schema básico para extrair o schema e o payload da mensagem
        message_schema = StructType(
            [StructField("schema", StringType(), True), StructField("payload", StringType(), True)]
        )

        # Aplica o message_schema para extrair as chaves "schema" e "payload"
        df = df.select(col("offset"), from_json(col("value"), message_schema).alias("data"))

        # Extrai o schema da mensagem
        schema_json = df.select("data.schema").head()[0]

        # Converte o schema JSON para um dicionário Python
        debezium_schema = json.loads(schema_json)

        # Converte o schema do Debezium para o formato do Spark
        spark_schema = convert_debezium_schema_to_spark(debezium_schema)

        # Aplica o schema ao payload
        df = df.select(col("offset"), from_json(col("data.payload"), spark_schema).alias("payload"))

        logger.info("Schema de cdc_df: \n")
        df.printSchema()
        logger.info(f"Count: {df.count()}")

        logger.info(f"cdc_df para {topic}:")
        df.show(n=3, truncate=False, vertical=True)

        cdc_unified_df = df.selectExpr(
            """
                CASE
                    WHEN payload.op = 'd' THEN payload.before
                    ELSE payload.after
                END AS payload
            """,
            "payload.op AS operation",
            "payload.ts_ms",
        ).select("payload.*", "operation", "ts_ms")

        # Calcula o número de registros para cada tipo de operação
        num_inserts = cdc_unified_df.filter(col("operation") == "c").count()
        num_updates = cdc_unified_df.filter(col("operation") == "u").count()
        num_deletes = cdc_unified_df.filter(col("operation") == "d").count()

        # Loga os resultados
        logger.info(f"Número de inserts para {topic}: {num_inserts}")
        logger.info(f"Número de updates para {topic}: {num_updates}")
        logger.info(f"Número de deletes para {topic}: {num_deletes}")

        # Salva os dados particionados
        if cdc_unified_df.count() > 0:
            logger.info(f"Salvando dados na landing zone para {topic}...")
            cdc_unified_df.write.format("parquet").mode("append").save(f"{GCS_BUCKET}/cdc/{topic}/")

        max_offset = df.selectExpr("max(offset)").collect()[0][0]
        if max_offset is not None:
            logger.info(f"Salvando último offset para {topic}: {max_offset}")
            save_last_offset(topic, partition, max_offset)
    else:
        print(f"Nenhuma nova mensagem para processar no tópico {topic}.")
