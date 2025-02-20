import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def save_to_parquet(df, output_path, write_mode):
    """
    Salva um DataFrame Spark em formato Parquet no caminho especificado.

    Args:
        df (DataFrame): DataFrame do Spark.
        output_path (str): Caminho de destino no GCS.
    """
    try:
        logger.info(f"Salvando dados no caminho: {output_path}")
        df.coalesce(1).write.mode(write_mode).parquet(output_path)
        logger.info("Dados salvos com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao salvar Parquet: {e}")
