import json
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType, TimestampType, DateType

# Load schemas from JSON file
with open("/opt/spark/apps/topics_schemas.json", "r") as f:
    schemas = json.load(f)

# Function to convert schema definition to Spark StructType
def get_schema_for_topic(topic):
    schema_def = schemas.get(topic)
    if not schema_def:
        raise ValueError(f"Schema not found for topic: {topic}")

    fields = []
    for field in schema_def["fields"]:
        field_name = field["name"]
        field_type = field["type"]
        field_nullable = field["nullable"]

        # Map field type to Spark data type
        if field_type == "long":
            spark_type = LongType()
        elif field_type == "string":
            spark_type = StringType()
        elif field_type == "integer":
            spark_type = IntegerType()
        elif field_type == "timestamp":
            spark_type = TimestampType()
        elif field_type == "date":
            spark_type = DateType()
        else:
            raise ValueError(f"Unsupported field type: {field_type}")

        fields.append(StructField(field_name, spark_type, field_nullable))

    return StructType(fields)
