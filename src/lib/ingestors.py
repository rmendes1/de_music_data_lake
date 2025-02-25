import sys
sys.path.append(f'/Workspace/Users/{dbutils.widgets.get("account")}/music_data_lake/src/lib')

import utils
from preprocessing import TRANSFORMATIONS, transform_generic
from ingestors import GenericIngestor

# Pega o JSON passado como argumento do job
table_config = {
  "catalog": dbutils.widgets.get("catalog"),
  "schema": dbutils.widgets.get("schema"),
  "tablename": dbutils.widgets.get("tablename"),
  "primary_key": dbutils.widgets.get("primary_key"),
  "timestamp_field": dbutils.widgets.get("timestamp_field")
}

ingestor = GenericIngestor(spark, table_config)

# Full Load, se necessário
ingestor.execute_full_load()

# Lê o stream
table_schema = utils.import_schema(ingestor.tablename)
df_stream = (spark.readStream
                  .format("parquet")
                  .option("cloudFiles.format", "parquet")
                  .schema(table_schema)
                  .load(f"/Volumes/raw/{ingestor.schema}/cdc/postgres.public.{ingestor.tablename}/"))


# Aplica transformação específica se existir, caso contrario aplica transformação genérica
transform_function = TRANSFORMATIONS.get(ingestor.schema, "others")
df_stream = transform_function(df_stream)

# Processa o CDC
cdc_stream = ingestor.process_stream(df_stream)
cdc_stream.start()
