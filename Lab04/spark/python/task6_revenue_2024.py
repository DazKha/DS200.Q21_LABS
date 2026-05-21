import os

from utils.spark_session import create_spark_session, read_csv, write_output
from pyspark.sql.functions import col, sum, year, to_timestamp


def run(spark, data_dir, results_dir):
    df_orders = read_csv(spark, os.path.join(data_dir, "Orders.csv"))
    df_items = read_csv(spark, os.path.join(data_dir, "Order_Items.csv"))
    df_products = read_csv(spark, os.path.join(data_dir, "Products.csv"))

    df_orders = df_orders.withColumn(
        "PurchaseYear",
        year(to_timestamp(col("Order_Purchase_Timestamp"), "yyyy-MM-dd HH:mm")),
    )

    df_2024 = df_orders.filter(col("PurchaseYear") == 2024)

    joined = (
        df_2024.join(df_items, on="Order_ID", how="inner")
        .join(df_products, on="Product_ID", how="inner")
    )

    result = (
        joined.withColumn("ItemRevenue", col("Price") + col("Freight_Value"))
        .groupBy("Product_Category_Name")
        .agg(sum("ItemRevenue").alias("TotalRevenue"))
        .orderBy(col("TotalRevenue").desc())
    )

    lines = []
    lines.append("=" * 65)
    lines.append("  TASK 6 - REVENUE IN 2024 BY PRODUCT CATEGORY")
    lines.append("=" * 65)
    lines.append("")
    lines.append(f"  {'Category':<40} {'TotalRevenue':>15}")
    lines.append("  " + "-" * 57)

    for row in result.collect():
        lines.append(f"  {row['Product_Category_Name']:<40} {row['TotalRevenue']:>15.2f}")

    lines.append("")

    write_output(lines, os.path.join(results_dir, "task6_revenue_2024_by_category.txt"))
    print(f"[Task 6] Done -> {os.path.join(results_dir, 'task6_revenue_2024_by_category.txt')}")


if __name__ == "__main__":
    spark = create_spark_session("Task6_Revenue2024")
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "data")
    results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "results")
    run(spark, data_dir, results_dir)
    spark.stop()
