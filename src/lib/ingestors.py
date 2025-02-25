from delta.tables import DeltaTable
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc, col

class GenericIngestor:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.catalog = config["catalog"]
        self.schema = config["schema"]
        self.tablename = config["tablename"]
        self.primary_key = config["primary_key"]
        self.timestamp_field = config["timestamp_field"]
        self.table_path = f"{self.catalog}.{self.schema}.{self.tablename}"

    def execute_full_load(self):
        if not utils.table_exists(self.spark, catalog=self.catalog, schema=self.schema, table=self.tablename):
            df_full = self.spark.read.format("parquet").load(f'/Volumes/raw/{self.schema}/full_load/{self.tablename}/')
            df_full.coalesce(1).write.format("delta").mode("overwrite").saveAsTable(self.table_path)

    def upsert(self, df, batch_id):
        deltatable = DeltaTable.forName(self.spark, self.table_path)

        df_filtered = df.filter(f"{self.primary_key} IS NOT NULL")
        windowSpec = Window.partitionBy(self.primary_key).orderBy(col(self.timestamp_field).desc())

        df_cdc = (df_filtered.withColumn("row_number", row_number().over(windowSpec))
                            .filter(col("row_number") == 1)
                            .drop("row_number"))

        (deltatable.alias("target")
                    .merge(df_cdc.alias("source"), f"target.{self.primary_key} = source.{self.primary_key}")
                    .whenMatchedDelete(condition="source.operation = 'd'")
                    .whenMatchedUpdateAll(condition="source.operation = 'u'")
                    .whenNotMatchedInsertAll(condition="source.operation = 'c' OR source.operation = 'u'")
                    .execute())

    def process_stream(self, df_stream):
        """Agora a transformação ocorre antes de chamar esta função."""
        stream = (df_stream.writeStream
                  .option("checkpointLocation", f"/Volumes/raw/{self.schema}/cdc/postgres.public.{self.tablename}/{self.tablename}_checkpoint/")
                  .foreachBatch(lambda df, batch_id: self.upsert(df, batch_id))
                  .trigger(availableNow=True))
        return stream