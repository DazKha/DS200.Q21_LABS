import os

from utils.spark_session import create_spark_session, read_csv, write_output
from pyspark.sql.functions import col, count as spark_count


def run(spark, data_dir, results_dir):
    df_orders = read_csv(spark, os.path.join(data_dir, "Orders.csv"))
    df_customers = read_csv(spark, os.path.join(data_dir, "Customer_List.csv"))

    joined = df_orders.join(df_customers, on="Customer_Trx_ID", how="inner")

    result = (
        joined.groupBy("Customer_Country")
        .agg(spark_count("Order_ID").alias("OrderCount"))
        .orderBy(col("OrderCount").desc())
    )

    lines = []
    lines.append("=" * 65)
    lines.append("  TASK 3 - ORDERS BY COUNTRY (DESCENDING)")
    lines.append("=" * 65)
    lines.append("")
    lines.append(f"  {'Country':<30} {'Orders':>10}")
    lines.append("  " + "-" * 42)

    for row in result.collect():
        lines.append(f"  {row['Customer_Country']:<30} {row['OrderCount']:>10}")

    lines.append("")

    write_output(lines, os.path.join(results_dir, "task3_orders_by_country.txt"))
    print(f"[Task 3] Done -> {os.path.join(results_dir, 'task3_orders_by_country.txt')}")


if __name__ == "__main__":
    spark = create_spark_session("Task3_OrdersByCountry")
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "data")
    results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "results")
    run(spark, data_dir, results_dir)
    spark.stop()
