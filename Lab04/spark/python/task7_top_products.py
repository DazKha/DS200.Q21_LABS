import os

from utils.spark_session import create_spark_session, read_csv, write_output
from pyspark.sql.functions import col, count, avg, round as spark_round
from pyspark.sql.types import IntegerType


def run(spark, data_dir, results_dir):
    df_items = read_csv(spark, os.path.join(data_dir, "Order_Items.csv"))
    df_reviews = read_csv(spark, os.path.join(data_dir, "Order_Reviews.csv"))

    sales_count = (
        df_items.groupBy("Product_ID")
        .agg(count("Order_Item_ID").alias("SalesCount"))
    )

    df_reviews_clean = (
        df_reviews.withColumn("Score", col("Review_Score").cast(IntegerType()))
        .filter(
            col("Score").isNotNull() & (col("Score") >= 1) & (col("Score") <= 5)
        )
    )

    avg_rating = (
        df_items.join(df_reviews_clean, on="Order_ID", how="inner")
        .groupBy("Product_ID")
        .agg(spark_round(avg("Score"), 2).alias("AvgReviewScore"))
    )

    result = (
        sales_count.join(avg_rating, on="Product_ID", how="left")
        .orderBy(col("SalesCount").desc())
    )

    lines = []
    lines.append("=" * 65)
    lines.append("  TASK 7 - TOP SELLING PRODUCTS + AVERAGE RATING")
    lines.append("=" * 65)
    lines.append("")
    lines.append(f"  {'Product_ID':<36} {'SalesCount':>12} {'AvgScore':>10}")
    lines.append("  " + "-" * 60)

    for row in result.collect():
        avg_score_display = f"{row['AvgReviewScore']:.2f}" if row["AvgReviewScore"] is not None else "N/A"
        lines.append(f"  {row['Product_ID']:<36} {row['SalesCount']:>12} {avg_score_display:>10}")

    lines.append("")

    write_output(lines, os.path.join(results_dir, "task7_top_products.txt"))
    print(f"[Task 7] Done -> {os.path.join(results_dir, 'task7_top_products.txt')}")


if __name__ == "__main__":
    spark = create_spark_session("Task7_TopProducts")
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "data")
    results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "results")
    run(spark, data_dir, results_dir)
    spark.stop()
