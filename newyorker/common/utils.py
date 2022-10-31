import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as f


def get_date_partition(execution_date: datetime.datetime):
    return "{year}{month}{date}".format(
        year=execution_date.year,
        month=f"{execution_date.month:02d}",
        date=f"{execution_date.day:02d}"
    )


def add_imported_at(df: DataFrame, date_partition: str):
    """
    Add a column to the dataframe with the execution date `imported_at`
    :param df: input dataframe
    :param date_partition: string in format YYYYMMDD
    :return: df
    """
    return df.withColumn("imported_at", f.lit(int(date_partition)))
