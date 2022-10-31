from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)

INPUT_SCHEMA = StructType([
    StructField("business_id", StringType(), True),
    StructField("compliment_count", LongType(), True),
    StructField("date", StringType(), True),
    StructField("text", StringType(), True),
    StructField("user_id", StringType(), True)
])

OUTPUT_FIELDS = {
    # no renaming the columns is needed.
}
