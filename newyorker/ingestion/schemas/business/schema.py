from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    MapType
)

INPUT_SCHEMA = StructType([
    StructField("address", StringType(), True),
    StructField("attributes", MapType(StringType(), StringType()), True),
    StructField("business_id", StringType(), True),
    StructField("categories", StringType(), True),
    StructField("city", StringType(), True),
    StructField("hours", StructType(
        [
            StructField("Friday", StringType(), True),
            StructField("Monday", StringType(), True),
            StructField("Saturday", StringType(), True),
            StructField("Sunday", StringType(), True),
            StructField("Thursday", StringType(), True),
            StructField("Tuesday", StringType(), True),
            StructField("Wednesday", StringType(), True),
        ]
    ), True),
    StructField("is_open", LongType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("name", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("review_count", LongType(), True),
    StructField("stars", DoubleType(), True),
    StructField("state", StringType(), True),
])

OUTPUT_FIELDS = {
    # no renaming the columns is needed.
}

MAP_COLUMNS_TO_EXPLODE = {
    "attributes": ["attribute", "attribute_value"]
}

COLUMNS_TO_FLATTEN = {
    "hours.Monday": "monday_hours",
    "hours.Tuesday": "tuesday_hours",
    "hours.Wednesday": "wednesday_hours",
    "hours.Thursday": "thursday_hours",
    "hours.Friday": "friday_hours",
    "hours.Saturday": "saturday_hours",
    "hours.Sunday": "sunday_hours"

}

COLUMNS_TO_DROP = ["hours"]
