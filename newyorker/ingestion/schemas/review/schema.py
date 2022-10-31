from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType
)

INPUT_SCHEMA = StructType([
    StructField("business_id", StringType(), True),
    StructField("cool", LongType(), True),
    StructField("date", StringType(), True),
    StructField("funny", LongType(), True),
    StructField("review_id", StringType(), True),
    StructField("stars", DoubleType(), True),
    StructField("text", StringType(), True),
    StructField("useful", LongType(), True),
    StructField("user_id", StringType(), True)
])

OUTPUT_FIELDS = {
    "date": "datetime",
    "date_format": "date"
}

FORMATTING_COLUMNS = [
    "to_date(date) as date_format",
    "year(date)||'-W'||lpad(weekofyear(date),2,'0') as week"
]
