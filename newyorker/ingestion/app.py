import argparse
import logging
import sys
from datetime import datetime

# to avoid import error in docker
sys.path.insert(0, '/app')

from ingestion.utils import main_transform
from ingestion.schemas.schema_inputs import SCHEMA_INPUTS
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser(
    description="Ingestion service"
)
parser.add_argument(
    "--entity",
    type=str,
    required=True,
    help="Entity type like business, user, review etc."
)
parser.add_argument(
    "--input-path",
    type=str,
    required=True
)
parser.add_argument(
    "--output-path",
    type=str,
    required=False,
    default=None
)
parser.add_argument(
    "--execution-date",
    type=str,
    required=True
)


def run(entity: str,
        input_path: str,
        output_path: str,
        execution_date: str) -> None:
    app_name = f"ingest_{entity}_{execution_date}"
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    try:
        execution_date = datetime.strptime(execution_date, "%Y-%m-%d")
        logging.info(f"Reading input from {input_path}")
        schema_settings = SCHEMA_INPUTS[entity]

        full_input_path = f"{input_path}/yelp_academic_dataset_{entity}.json"

        df = spark.read.json(path=full_input_path,
                             schema=schema_settings["input_schema"])

        df = main_transform(df=df,
                            schema_settings=schema_settings,
                            execution_date=execution_date
                            )

        write_mode = schema_settings.get("write_mode") \
            if "write_mode" in schema_settings else "overwrite"

        if write_mode == "overwrite":
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        df = df.coalesce(1)
        full_output_path = f"{output_path}/{entity}"
        partition_columns = schema_settings.get("partition_columns")
        logging.info(f"Writing to the path {full_output_path}  in the {write_mode} mode")

        write_df = df.write \
            .format("parquet") \
            .mode(write_mode)

        if partition_columns:
            logging.info(f"Writing data with {partition_columns} Partitions.")
            write_df.partitionBy(partition_columns)
        else:
            logging.info("Writing data without Partition.")

        write_df.save(full_output_path)
    except Exception:
        logging.exception("Got exception")
        sys.exit(-1)

    logging.info(f"{app_name} has completed!")


if __name__ == "__main__":
    args = parser.parse_args()
    run(entity=args.entity,
        input_path=args.input_path,
        output_path=args.output_path,
        execution_date=args.execution_date
        )
