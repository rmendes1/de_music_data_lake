from pyspark.sql import types
import json


def table_exists(spark, catalog, schema, table):
    count = (
        spark.sql(f"SHOW TABLES FROM {catalog}.{schema}").filter(f"database='{schema}' AND tableName='{table}'").count()
    )
    return count == 1


def import_schema(tablename, spark):
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
    with open(
        f"/Workspace/Users/{dbutils.widgets.get('account')}/music_data_lake/src/lib/{tablename}.json", "r"
    ) as open_file:
        schema_json = json.load(open_file)

    schema_df = types.StructType.fromJson(schema_json)
    return schema_df
