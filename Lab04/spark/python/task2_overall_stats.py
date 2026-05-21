import os

from utils.spark_session import create_spark_session, read_csv, write_output
from pyspark.sql.functions import countDistinct


def run(spark, data_dir, results_dir):
    df_orders = read_csv(spark, os.path.join(data_dir, "Orders.csv"))
    df_customers = read_csv(spark, os.path.join(data_dir, "Customer_List.csv"))
    df_items = read_csv(spark, os.path.join(data_dir, "Order_Items.csv"))

    total_orders = df_orders.count()
    unique_customers = df_customers.select(countDistinct("Customer_Trx_ID")).collect()[0][0]
    unique_sellers = df_items.select(countDistinct("Seller_ID")).collect()[0][0]

    lines = []
    lines.append("=" * 65)
    lines.append("  TASK 2 - OVERALL STATISTICS")
    lines.append("=" * 65)
    lines.append("")
    lines.append(f"  Total orders       : {total_orders}")
    lines.append(f"  Unique customers   : {unique_customers}")
    lines.append(f"  Unique sellers     : {unique_sellers}")
    lines.append("")

    write_output(lines, os.path.join(results_dir, "task2_overall_stats.txt"))
    print(f"[Task 2] Done -> {os.path.join(results_dir, 'task2_overall_stats.txt')}")


if __name__ == "__main__":
    spark = create_spark_session("Task2_OverallStats")
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "data")
    results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "results")
    run(spark, data_dir, results_dir)
    spark.stop()
