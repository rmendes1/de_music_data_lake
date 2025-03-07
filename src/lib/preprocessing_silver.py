from pyspark.sql.functions import col, when, trim, lower, year, month


def transform_artists(df):
    return (df.withColumn("artist_name", trim(col("artist_name")))
            .withColumn("is_active", when(col("followers") > 0, True).otherwise(False)))


def transform_albums(df):
    return (
        df.withColumn("release_date", when(col("release_date") >= "1900-01-01", col("release_date")).otherwise(None))
        .withColumn("year", year(col("release_date")))
        .withColumn("month", month(col("release_date"))))


def transform_tracks(df):
    return (df.withColumn("duration", when(col("duration") >= 5, col("duration")).otherwise(None))
            .withColumn("track_status", when(col("_change_type") == "insert", "new")
                        .when(col("_change_type") == "update_postimage", "updated")))


def transform_genres(df):
    return df.withColumn("genre_name", lower(trim(col("genre_name"))))


# Dicionário para mapear as transformações por tabela
TRANSFORMATIONS = {
    "artists": transform_artists,
    "albums": transform_albums,
    "tracks": transform_tracks,
    "genres": transform_genres
}


def transform_data(table_name, df):
    return TRANSFORMATIONS.get(table_name, lambda x: x)(df)
