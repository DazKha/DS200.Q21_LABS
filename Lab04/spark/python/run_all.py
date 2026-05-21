import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.spark_session import create_spark_session

from task1_load_datasets import run as run_task1
from task2_overall_stats import run as run_task2
from task3_orders_by_country import run as run_task3
from task4_orders_by_year_month import run as run_task4
from task5_review_stats import run as run_task5
from task6_revenue_2024 import run as run_task6
from task7_top_products import run as run_task7
from task10_seller_ranking import run as run_task10


def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    data_dir = os.path.join(base_dir, "data")
    results_dir = os.path.join(base_dir, "results")

    os.makedirs(results_dir, exist_ok=True)

    spark = create_spark_session("FecomAnalysis_RunAll")

    tasks = [
        ("Task 1", run_task1),
        ("Task 2", run_task2),
        ("Task 3", run_task3),
        ("Task 4", run_task4),
        ("Task 5", run_task5),
        ("Task 6", run_task6),
        ("Task 7", run_task7),
        ("Task 10", run_task10),
    ]

    print("\n" + "=" * 65)
    print("  FECOM E-COMMERCE ANALYSIS - RUN ALL TASKS")
    print("=" * 65 + "\n")

    for task_name, task_func in tasks:
        start = time.time()
        try:
            task_func(spark, data_dir, results_dir)
            elapsed = time.time() - start
            print(f"  [{task_name}] completed in {elapsed:.1f}s\n")
        except Exception as e:
            print(f"  [{task_name}] FAILED: {e}\n")

    spark.stop()
    print("=" * 65)
    print("  ALL TASKS FINISHED.")
    print(f"  Results saved to: {results_dir}")
    print("=" * 65)


if __name__ == "__main__":
    main()
