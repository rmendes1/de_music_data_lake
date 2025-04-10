from pyspark.sql.functions import col, when, trim, lower, year, month
import pandas as pd

def transform_artists(df):
    return (df.withColumn("artistName", trim(col("artistName")))
            .withColumn("isActive", when(col("followers") > 0, True).otherwise(False)))


def transform_albums(df):
    return (
        df.withColumn("releaseDate", when(col("releaseDate") >= "1900-01-01", col("releaseDate")).otherwise(None))
        .withColumn("year", year(col("releaseDate")))
        .withColumn("month", month(col("releaseDate"))))


def transform_tracks(df):
    return (df.withColumn("duration", when(col("duration") >= 5, col("duration")).otherwise(None))
            .withColumn("track_status", when(col("_change_type") == "insert", "new")
                        .when(col("_change_type") == "update_postimage", "updated")))


def transform_genres(df):
    return df.withColumn("genreName", lower(trim(col("genreName"))))


# Dicionário para mapear as transformações por tabela
TRANSFORMATIONS = {
    "artists": transform_artists,
    "albums": transform_albums,
    "tracks": transform_tracks,
    "genres": transform_genres
}


def transform_data(table_name, df):
    return TRANSFORMATIONS.get(table_name, lambda x: x)(df)
