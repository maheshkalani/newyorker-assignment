from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType
)

INPUT_SCHEMA = StructType([
    StructField("average_stars", DoubleType(), True),
    StructField("compliment_cool", LongType(), True),
    StructField("compliment_cute", LongType(), True),
    StructField("compliment_funny", LongType(), True),
    StructField("compliment_hot", LongType(), True),
    StructField("compliment_list", LongType(), True),
    StructField("compliment_more", LongType(), True),
    StructField("compliment_note", LongType(), True),
    StructField("compliment_photos", LongType(), True),
    StructField("compliment_plain", LongType(), True),
    StructField("compliment_profile", LongType(), True),
    StructField("compliment_writer", LongType(), True),
    StructField("cool", LongType(), True),
    StructField("elite", StringType(), True),
    StructField("fans", LongType(), True),
    StructField("friends", StringType(), True),
    StructField("funny", LongType(), True),
    StructField("name", StringType(), True),
    StructField("review_count", LongType(), True),
    StructField("useful", LongType(), True),
    StructField("user_id", StringType(), True),
    StructField("yelping_since", StringType(), True)
])

OUTPUT_FIELDS = {
    # no renaming the columns is needed.
}
