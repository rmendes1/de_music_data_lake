from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType


def convert_debezium_schema_to_spark(debezium_schema):
    """
    Converte o schema do Debezium para o formato esperado pelo PySpark.
    """

    def parse_field(field):
        # Mapeia os tipos do Debezium para os tipos do Spark
        type_mapping = {
            "int64": LongType(),
            "string": StringType(),
            "int32": IntegerType(),
            "io.debezium.time.Date": IntegerType(),  # Datas são tratadas como inteiros
            "io.debezium.time.MicroTimestamp": LongType(),  # Timestamps são tratados como longos
        }

        field_type = field["type"]
        if field_type in type_mapping:
            return StructField(
                name=field["field"], dataType=type_mapping[field_type], nullable=field.get("optional", True)
            )
        elif field_type == "struct":
            # Campos aninhados (struct)
            return StructField(
                name=field["field"],
                dataType=StructType([parse_field(f) for f in field["fields"]]),
                nullable=field.get("optional", True),
            )
        else:
            raise ValueError(f"Unsupported field type: {field_type}")

    # Extrai os campos do schema do Debezium
    fields = []
    for field in debezium_schema["fields"]:
        fields.append(parse_field(field))

    return StructType(fields)
