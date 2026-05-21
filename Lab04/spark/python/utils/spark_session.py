import os

from pyspark.sql import SparkSession


def create_spark_session(app_name="FecomAnalysis"):
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )


def read_csv(spark, filepath, delimiter=";"):
    df = (
        spark.read.option("header", "true")
        .option("delimiter", delimiter)
        .option("inferSchema", "true")
        .option("encoding", "UTF-8")
        .csv(filepath)
    )
    first_col = df.columns[0]
    if first_col.startswith("\ufeff"):
        df = df.withColumnRenamed(first_col, first_col.lstrip("\ufeff"))
    return df


def write_output(lines, filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")
