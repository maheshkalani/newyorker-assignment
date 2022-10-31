from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)

INPUT_SCHEMA = StructType([
    StructField("business_id", StringType(), True),
    StructField("date", StringType(), True)
])

OUTPUT_FIELDS = {
    # no renaming the columns is needed.
}
