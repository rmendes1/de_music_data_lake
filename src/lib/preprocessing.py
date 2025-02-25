from pyspark.sql.functions import expr


def transform_generic(df):
    """Transformação genérica: converte ts_ms para timestamp, se existir."""
    if "ts_ms" in df.columns:
        df = df.withColumn("ts_ms", expr("from_unixtime(ts_ms / 1000)"))
    return df


def transform_types_music_data_schema(df):
    """Transformações específicas para a tabela albums_genres."""
    if "release_date" in df.columns:
        df = df.withColumn("release_date", expr("DATE_FROM_UNIX_DATE(release_date)"))
    if set(["created_at", "updated_at"]).issubset(df.columns):
        df = df.withColumn("created_at", expr("from_unixtime(created_at / 1e6)")) \
               .withColumn("updated_at", expr("from_unixtime(updated_at / 1e6)"))
    df = transform_generic(df)
    return df


# Mapeia tabelas para funções de transformação específicas
TRANSFORMATIONS = {
    "music_data": {
        "albums_genres": transform_albums_genres,
        "artists": transform_artists
    },
    "others": transform_generic  # Fallback para tabelas sem regra específica
}
