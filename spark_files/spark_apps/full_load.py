from pyspark.sql import SparkSession
import logging
from writer import save_to_parquet
from config import DB_URL, DB_USER, DB_PASSWORD, GCS_BUCKET


# Configura o logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ingest_full_table(table):
    """
    Função para ingerir uma tabela do PostgreSQL e salvar em formato Parquet no GCS.

    Args:
        db_url (str): URL do banco de dados PostgreSQL.
        db_user (str): Usuário do banco de dados.
        db_password (str): Senha do banco de dados.
        table (str): Nome da tabela a ser ingerida.
        gcs_output_path (str): Caminho no GCS para salvar o Parquet.
    """
    try:
        # Inicializa a sessão do Spark
        spark = SparkSession.builder \
            .config("spark.jars", "/opt/spark/jars/postgresql-42.6.2.jar") \
            .appName(f"Ingestão da Tabela {table}") \
            .getOrCreate()

        # Define as propriedades de conexão
        connection_properties = {
            "user": DB_USER,
            "password": DB_PASSWORD,
            "driver": "org.postgresql.Driver"
        }

        # Lê a tabela do PostgreSQL
        logger.info(f"Lendo a tabela {table} do PostgreSQL...")
        df = spark.read.jdbc(url=DB_URL, table=table, properties=connection_properties)
        logger.info(f"Schema da tabela {table}:")
        df.printSchema()
        logger.info(f"N rows: {df.count()}")

        gcs_output_path = f"{GCS_BUCKET}/full_load/{table}"
        save_to_parquet(df, gcs_output_path, write_mode="overwrite")

    except Exception as e:
        logger.error(f"Erro ao ingerir a tabela {table}: {e}")
    finally:
        # Fecha a sessão do Spark
        if 'spark' in locals():
            spark.stop()
            logger.info("Sessão do Spark finalizada.")

# Exemplo de uso
if __name__ == "__main__":
    # Lista de tabelas para ingerir
    tables_to_ingest = [
        "albums",
        "albums_genres",
        "artists",
        "genres",
        "tracks"
        ]

    # Itera sobre as tabelas e realiza a ingestão
    for table_name in tables_to_ingest:
        ingest_full_table(table_name)