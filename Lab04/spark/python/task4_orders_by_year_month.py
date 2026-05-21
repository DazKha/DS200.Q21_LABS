import os

from utils.spark_session import create_spark_session, read_csv, write_output
from pyspark.sql.functions import col, count, year, month, to_timestamp


def run(spark, data_dir, results_dir):
    df_orders = read_csv(spark, os.path.join(data_dir, "Orders.csv"))

    df_orders = df_orders.withColumn(
        "PurchaseDate", to_timestamp(col("Order_Purchase_Timestamp"), "yyyy-MM-dd HH:mm")
    )

    result = (
        df_orders.groupBy(
            year("PurchaseDate").alias("Year"),
            month("PurchaseDate").alias("Month"),
        )
        .agg(count("Order_ID").alias("OrderCount"))
        .orderBy(col("Year").asc(), col("Month").desc())
    )

    lines = []
    lines.append("=" * 65)
    lines.append("  TASK 4 - ORDERS BY YEAR / MONTH")
    lines.append("=" * 65)
    lines.append("")
    lines.append(f"  {'Year':>6}  {'Month':>6}  {'OrderCount':>12}")
    lines.append("  " + "-" * 30)

    for row in result.collect():
        lines.append(f"  {row['Year']:>6}  {row['Month']:>6}  {row['OrderCount']:>12}")

    lines.append("")

    write_output(lines, os.path.join(results_dir, "task4_orders_by_year_month.txt"))
    print(f"[Task 4] Done -> {os.path.join(results_dir, 'task4_orders_by_year_month.txt')}")


if __name__ == "__main__":
    spark = create_spark_session("Task4_OrdersByYearMonth")
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "data")
    results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "results")
    run(spark, data_dir, results_dir)
    spark.stop()
