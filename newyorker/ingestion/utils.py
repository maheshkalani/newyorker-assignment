import datetime
from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from common.utils import get_date_partition, add_imported_at


def explode_map(df: DataFrame, column_list: Dict):
    """
    This function will explode the map column and rename the key-value pair.
    :param df: Input dataframe
    :param column_list: dictionary contains key as column name and value as aliases for key-value.
    :return: exploded dataframe
    """
    if column_list:
        for column_name, aliases in column_list.items():
            df = df.select("*", f.explode_outer(f.col(column_name))).drop(column_name)
            df = df.withColumnRenamed("key", aliases[0]).withColumnRenamed("value", aliases[1])
    return df


def flattening(df: DataFrame, columns):
    """
    This function will flatten struct columns and rename them based on type of structure.
    :return: output DataFrame
    """
    if columns is not None:
        if isinstance(columns, dict):
            df = df.selectExpr('*', *[f"{column} as {alias}" for column, alias in columns.items()])
        elif isinstance(columns, list):
            struct_fields_cols = []
            for field in columns:
                fields = df.schema[field].dataType.fieldNames()
                struct_fields_cols.extend([f"{field}.{field_name} as {field_name}" for field_name in fields])
            df = df.selectExpr('*', *struct_fields_cols)
        else:
            raise ValueError("Please pass either list or dictionary as columns param")
    return df


def formatting(df, expression: List):
    """
    This function is used for low level formatting like type conversion
    :param df: input dataframe
    :param expression: list of formatting expression
    :return: formatted dataframe.
    """
    if expression is not None:
        df = df.selectExpr('*', *[value for value in expression])
    return df


def drop_columns(df: DataFrame, column_list: List[str]):
    """
    :param df: Input DataFrame
    :param column_list: List of columns to be dropped from dataFrame
    :return: Output dataframe
    """
    if column_list and len(column_list) > 0:
        df = df.drop(*column_list)
    return df


def return_renamed_columns(df: DataFrame, column_dict: Dict[str, str]):
    """
    :param df: Input DataFrame
    :param column_dict: List of columns to be renamed in output df
    :return: Output dataframe
    """
    if column_dict:
        for column, alias in column_dict.items():
            df = df.withColumnRenamed(column, alias)
    return df


def main_transform(df: DataFrame,
                   schema_settings: Dict,
                   execution_date: datetime.datetime):
    """
    :param df: Input dataframe
    :param schema_settings: Schema settings from schema input file
    :param execution_date: date of execution
    :return: Output dataframe
    """
    df = explode_map(df, column_list=schema_settings.get("map_columns_to_explode"))
    df = flattening(df, columns=schema_settings.get("columns_to_flatten"))
    df = formatting(df, expression=schema_settings.get("formatting_columns"))
    df = drop_columns(df, column_list=schema_settings.get("columns_to_drop"))
    df = add_imported_at(df, date_partition=get_date_partition(execution_date))
    return return_renamed_columns(df, column_dict=schema_settings.get("output_mapping"))
