from pyspark.sql.functions import (
    col, when, trim, lower, upper, year, month,
    length, lit, expr, row_number
)
from pyspark.sql.window import Window


def transform_artists(df):
    return (
        df
        .dropDuplicates(["artistId"])
        .withColumn("artistName", trim(col("artistName")))
        .withColumn("artistName", when(length(col("artistName")) > 0, col("artistName")).otherwise(None))
        .withColumn("artistName", lower(col("artistName")))
        .withColumn("isActive", when(col("followers") > 0, lit(True)).otherwise(lit(False)))
        .filter(col("artistName").isNotNull())
    )


def transform_albums(df):
    return (
        df
        .dropDuplicates(["albumId"])
        .withColumn("albumTitle", trim(col("albumTitle")))
        .withColumn("albumTitle", lower(col("albumTitle")))
        .withColumn("releaseDate", when(col("releaseDate") >= "1900-01-01", col("releaseDate")).otherwise(None))
        .withColumn("year", year(col("releaseDate")))
        .withColumn("month", month(col("releaseDate")))
        .filter(col("albumTitle").isNotNull())
    )


def transform_tracks(df):
    window_spec = Window.partitionBy("trackId").orderBy(col("updatedAt").desc())

    return (
        df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)  # dedupe by latest
        .drop("row_num")
        .withColumn("trackTitle", trim(col("trackTitle")))
        .withColumn("trackTitle", lower(col("trackTitle")))
        .withColumn("duration", when(col("duration") >= 5, col("duration")).otherwise(None))
        .withColumn("hasPreview", col("preview").isNotNull())
        .filter(col("trackTitle").isNotNull())
    )


def transform_genres(df):
    return (
        df
        .dropDuplicates(["genreId"])
        .withColumn("genreName", lower(trim(col("genreName"))))
        .filter(col("genreName").isNotNull())
    )


# Dicionário para mapear as transformações por tabela
TRANSFORMATIONS = {
    "artists": transform_artists,
    "albums": transform_albums,
    "tracks": transform_tracks,
    "genres": transform_genres
}


def transform_data(table_name, df):
    return TRANSFORMATIONS.get(table_name, lambda x: x)(df)
