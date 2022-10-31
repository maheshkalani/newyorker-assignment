import argparse
import logging
import sys
from datetime import datetime
from pyspark.sql import SparkSession

sys.path.insert(0, '/app')
from aggregation import transformation as T

parser = argparse.ArgumentParser(
    description="Aggregation app for business data"
)

parser.add_argument(
    "--input-path",
    type=str,
    required=True,
    help="Input path of entities data"
)

parser.add_argument(
    "--output-path",
    type=str,
    required=True
)
parser.add_argument(
    "--execution-date",
    type=str,
    required=True
)


def run(
        input_path: str,
        output_path: str,
        execution_date: str,
) -> None:
    app_name = f"agg_business_{execution_date}"
    execution_date = datetime.strptime(execution_date, "%Y-%m-%d")
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    try:
        dfs = []
        for entity in ["review", "checkin", "business"]:
            df = spark.read.parquet(f"{input_path}/{entity}")
            dfs.append(df)
        df_reviews, df_checkins, df_business = dfs
        results_dfs = T.main(df_reviews=df_reviews,
                             df_business=df_business,
                             df_checkins=df_checkins,
                             execution_date=execution_date,
                             spark=spark)
        df_stars_business, df_checkin_business = results_dfs

        df_stars_business.coalesce(1).write \
            .format("parquet") \
            .partitionBy("week") \
            .mode("overwrite") \
            .save(f"{output_path}/stars_business")

        df_checkin_business.coalesce(1).write \
            .format("parquet") \
            .mode("overwrite") \
            .save(f"{output_path}/checkin_business")
    except Exception:
        logging.exception("Got exception")
        sys.exit(-1)


if __name__ == "__main__":
    args = parser.parse_args()
    run(
        input_path=args.input_path,
        output_path=args.output_path,
        execution_date=args.execution_date,
    )
