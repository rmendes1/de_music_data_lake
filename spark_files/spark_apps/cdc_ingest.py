from pyspark.sql import SparkSession
import logging
from config import GCS_BUCKET
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, IntegerType
from pyspark.sql.functions import from_json, col, year, month, dayofmonth, from_unixtime

from manage_offset import load_last_offset, save_last_offset
from utils import get_schema_for_topic

# Configura o logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Inicializa a sessão do Spark
spark = SparkSession.builder.appName("CDC Batch Ingestion").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
# Tópicos e partições a serem processados
topics = [
        # "postgres.public.albums",
        #   "postgres.public.albums_genres",
        #   "postgres.public.artists",
        #   "postgres.public.genres",
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
        "startingOffsets": "earliest" if last_offset is None else f'{{"{topic}": {{"0": {last_offset}}}}}',  # Começa do último offset
        "endingOffsets": "latest"  # Consome até o offset mais recente
    }
    # Lê as mensagens do Kafka
    df = spark.read \
        .format("kafka") \
        .options(**kafka_options) \
        .load()

    logger.info(f"Número de mensagens lidas do tópico {topic}: {df.count()}")

    # Processa as mensagens
    if df.count() > 0:
        logger.info(f"Pegando schema para topico: {topic.split('.')[-1]}")
        try:
            schema = StructType([
                StructField("payload", StructType([
                    StructField("before", get_schema_for_topic(topic.split(".")[-1])),
                    StructField("after", get_schema_for_topic(topic.split(".")[-1])),
                    StructField("ts_ms", StringType()),
                    StructField("op", StringType())
                ]))
            ])
        except ValueError as e:
            logger.error(f"Error loading schema for topic {topic}: {e}")
            continue

        cdc_df = df.selectExpr(f"CAST(value AS STRING) as json") \
            .select(from_json(col("json"), schema).alias("data"))

        logger.info(f"Schema de cdc_df: \n")
        cdc_df.printSchema()

        logger.info(f"cdc_df para {topic}:")
        cdc_df.show(n=1, truncate=False, vertical=True)
        logger.info(cdc_df.select("data.payload").show(n=1, truncate=False, vertical=True))

        # Separa os dados por operação (I, U, D)
        inserts_df = cdc_df.filter(col("data.payload.op") == "c") \
            .select("data.payload.after.*", "data.payload.ts_ms")

        updates_df = cdc_df.filter(col("data.payload.op") == "u") \
            .select("data.payload.after.*", "data.payload.ts_ms")

        deletes_df = cdc_df.filter(col("data.payload.op") == "d") \
            .select("data.payload.before.*", "data.payload.ts_ms")

        # Logs para confirmar o número de registros processados
        logger.info(f"Número de inserts para {topic}: {inserts_df.count()}")
        logger.info(f"Número de updates para {topic}: {updates_df.count()}")
        logger.info(f"Número de deletes para {topic}: {deletes_df.count()}")

        # Salva os dados particionados
        if inserts_df.count() > 0:
            logger.info(f"Salvando inserts para {topic}...")
            inserts_df.write \
                .format("parquet") \
                .mode("append") \
                .save(f"{GCS_BUCKET}/{topic}/inserts/")

        if updates_df.count() > 0:
            logger.info(f"Salvando updates para {topic}...")
            updates_df.write \
                .format("parquet") \
                .mode("append") \
                .save(f"{GCS_BUCKET}/{topic}/updates/")

        if deletes_df.count() > 0:
            logger.info(f"Salvando deletes para {topic}...")
            deletes_df.write \
                .format("parquet") \
                .mode("append") \
                .save(f"{GCS_BUCKET}/{topic}/deletes/")


        # Salva o último offset processado
        max_offset = df.selectExpr("max(offset)").collect()[0][0]
        if max_offset is not None:
            logger.info(f"Salvando último offset para {topic}: {max_offset}")
            save_last_offset(topic, partition, max_offset)
    else:
        print(f"Nenhuma nova mensagem para processar no tópico {topic}.")