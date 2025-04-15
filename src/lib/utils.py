from pyspark.sql import types
import datetime
import json

def extract_from(query:str):
    tablename = (query.lower()
                      .split("from")[-1]
                      .strip(" ")
                      .split(" ")[0]
                      .split("\n")[0]
                      .strip(" "))
    return tablename

def format_query_cdf(query:str, from_table:str):
    fields = ["_change_type", "_commit_version", "_commit_timestamp"]
    query = add_fields(query=query, fields=fields)
    query = add_generic_from(query=query, generic_from=from_table)
    return query

def date_range(start, stop):
    dt_start = datetime.datetime.strptime(start, "%Y-%m-%d")
    dt_stop = datetime.datetime.strptime(stop, "%Y-%m-%d")
    dates = []
    while dt_start < dt_stop:
        dates.append(dt_start.strftime("%Y-%m-%d"))
        dt_start += datetime.timedelta(days=1)
    return dates

def add_fields(query:str, fields:list):
    select = query.split("FROM")[0].strip(" \n")
    fields = ",\n".join(fields)
    from_query = f"\n\nFROM{query.split('FROM')[-1]}"
    query_new = f"{select},\n{fields}{from_query}"
    return query_new

def add_generic_from(query:str, generic_from="df"):
    tablename = extract_from(query)
    query = query.replace(tablename, generic_from)
    return query

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
