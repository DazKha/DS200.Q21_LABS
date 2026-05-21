import os

from utils.spark_session import create_spark_session, read_csv, write_output
from pyspark.sql.functions import col, sum, countDistinct
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank


def run(spark, data_dir, results_dir):
    df_items = read_csv(spark, os.path.join(data_dir, "Order_Items.csv"))

    seller_stats = df_items.groupBy("Seller_ID").agg(
        sum(col("Price") + col("Freight_Value")).alias("TotalRevenue"),
        countDistinct("Order_ID").alias("OrderCount"),
    )

    window_spec = Window.orderBy(col("TotalRevenue").desc())
    result = seller_stats.withColumn("Rank", dense_rank().over(window_spec)).orderBy("Rank")

    lines = []
    lines.append("=" * 65)
    lines.append("  TASK 10 - SELLER RANKING BY REVENUE & ORDER COUNT")
    lines.append("=" * 65)
    lines.append("")
    lines.append(f"  {'Rank':>5}  {'Seller_ID':<36} {'TotalRevenue':>15} {'OrderCount':>12}")
    lines.append("  " + "-" * 75)

    for row in result.collect():
        lines.append(
            f"  {row['Rank']:>5}  {row['Seller_ID']:<36} "
            f"{row['TotalRevenue']:>15.2f} {row['OrderCount']:>12}"
        )

    lines.append("")

    write_output(lines, os.path.join(results_dir, "task10_seller_ranking.txt"))
    print(f"[Task 10] Done -> {os.path.join(results_dir, 'task10_seller_ranking.txt')}")


if __name__ == "__main__":
    spark = create_spark_session("Task10_SellerRanking")
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "data")
    results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "results")
    run(spark, data_dir, results_dir)
    spark.stop()
