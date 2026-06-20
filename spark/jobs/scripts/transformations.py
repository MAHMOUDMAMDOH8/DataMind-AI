
from typing import Optional

from pyspark.sql.functions import col, concat, concat_ws
from pyspark.sql import DataFrame
from pyspark.sql.functions import when, lit
from pyspark.sql.types import StructType


def normalize_columns(df: DataFrame, columns: str, prefix: str) -> DataFrame:
    if columns not in df.columns:
        return df

    data_type = df.schema[columns].dataType
    if not isinstance(data_type, StructType):
        return df

    fields = data_type.names
    for field in fields:
        new_col_name = f"{prefix}_{field}" if prefix else field
        df = df.withColumn(new_col_name, col(columns)[field])
    return df.drop(columns)

def add_rejection_reason(
    df: DataFrame,
    required_columns: list,
    numeric_columns: Optional[list] = None,
    positive_columns: Optional[list] = None,
    is_between_columns: Optional[dict] = None,
    ) -> DataFrame:

    reasons = []

    for col_name in required_columns:
        if col_name in df.columns:
            reasons.append(
                when(col(col_name).isNull(), lit(f"{col_name} come with null value, "))
            )

    if numeric_columns:
        for col_name in numeric_columns:
            if col_name in df.columns:
                reasons.append(
                    when(~col(col_name).cast("double").isNotNull(), lit(f"{col_name} must be numeric, "))
                )

    if positive_columns:
        for col_name in positive_columns:
            if col_name in df.columns:
                reasons.append(
                    when(col(col_name).cast("double") <= 0, lit(f"{col_name} must be positive, "))
                )

    if is_between_columns:
        for col_name, (min_val, max_val) in is_between_columns.items():
            if col_name in df.columns:
                reasons.append(
                    when(
                        (col(col_name).cast("double") < min_val)
                        | (col(col_name).cast("double") > max_val),
                        lit(f"{col_name} must be between {min_val} and {max_val}, "),
                    )
                )

    combined = concat_ws("", *reasons) if reasons else lit("")

    df = df.withColumn("rejection_reason",
        when(combined == "", None).otherwise(combined)
    )

    df = df.withColumn("is_rejected", col("rejection_reason").isNotNull())

    return df