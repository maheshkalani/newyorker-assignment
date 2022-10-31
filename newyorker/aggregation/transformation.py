import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, size, split, round
from aggregation import variables as V
from common.utils import get_date_partition, add_imported_at


def main(df_reviews: DataFrame,
         df_business: DataFrame,
         df_checkins: DataFrame,
         execution_date: datetime.datetime,
         spark: SparkSession):
    """
    :param df_reviews:
    :param df_business:
    :param df_checkins:
    :param execution_date:
    :param spark:
    :return:
    """
    df_business_unique = _get_unique_business(df_business, spark)
    df_business_unique.persist()
    df_review_agg = _aggregate_avg_stars(df_reviews)
    df_star_business = _join_business(df_review_agg, df_business_unique)
    df_star_business = add_imported_at(df_star_business, get_date_partition(execution_date))
    df_checkin = _checkin_business(df_checkins)
    df_checkin_business = _join_business(df_checkin, df_business_unique)

    return df_star_business.select(V.COLUMNS_STAR_BUSINESS), df_checkin_business.select(V.COLUMNS_CHECKIN_BUSINESS)


def _aggregate_avg_stars(df_reviews: DataFrame):
    """

    :param df_reviews:
    :return:
    """
    return df_reviews.groupBy("business_id", "week") \
        .agg(round(avg("stars"), 2).alias("avg_stars"))


def _get_unique_business(df_business: DataFrame, spark: SparkSession):
    """

    :param df_business:
    :param spark:
    :return:
    """
    df_business.createOrReplaceTempView("business")
    return spark.sql("""
        select business_id, 
               name,
               address,
               stars,
               city,
               state,
               categories,
               postal_code,
               is_open
        from business group by 1,2,3,4,5,6,7,8,9
    """)


def _checkin_business(df_checkins: DataFrame):
    """
    :param df_checkins:
    :return:
    """
    return df_checkins.withColumn("checkin_count", size(split(col("date"), ",")))


def _join_business(df: DataFrame, df_business: DataFrame):
    """
    :param df:
    :param df_business:
    :return:
    """
    return df.join(df_business, on=["business_id"], how="inner")
