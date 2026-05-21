import os

from utils.spark_session import create_spark_session, read_csv, write_output
from pyspark.sql.functions import col, count, avg, round as spark_round
from pyspark.sql.types import IntegerType


def run(spark, data_dir, results_dir):
    df_reviews = read_csv(spark, os.path.join(data_dir, "Order_Reviews.csv"))

    df_clean = (
        df_reviews.withColumn("Score", col("Review_Score").cast(IntegerType()))
        .filter(
            col("Score").isNotNull() & (col("Score") >= 1) & (col("Score") <= 5)
        )
    )

    score_distribution = (
        df_clean.groupBy("Score")
        .agg(count("*").alias("ReviewCount"))
        .orderBy(col("Score").asc())
    )

    overall_avg = df_clean.select(spark_round(avg("Score"), 2).alias("AverageScore")).collect()[0][0]
    total_reviews = df_clean.count()

    lines = []
    lines.append("=" * 65)
    lines.append("  TASK 5 - REVIEW SCORE STATISTICS")
    lines.append("=" * 65)
    lines.append("")
    lines.append(f"  Total valid reviews  : {total_reviews}")
    lines.append(f"  Average review score : {overall_avg}")
    lines.append("")
    lines.append(f"  {'Score':>7}  {'ReviewCount':>13}")
    lines.append("  " + "-" * 24)

    for row in score_distribution.collect():
        lines.append(f"  {row['Score']:>7}  {row['ReviewCount']:>13}")

    lines.append("")

    write_output(lines, os.path.join(results_dir, "task5_review_stats.txt"))
    print(f"[Task 5] Done -> {os.path.join(results_dir, 'task5_review_stats.txt')}")


if __name__ == "__main__":
    spark = create_spark_session("Task5_ReviewStats")
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "data")
    results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "results")
    run(spark, data_dir, results_dir)
    spark.stop()
