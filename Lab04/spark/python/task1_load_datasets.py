import os

from utils.spark_session import create_spark_session, read_csv, write_output


def run(spark, data_dir, results_dir):
    datasets = {
        "Orders": "Orders.csv",
        "Customer_List": "Customer_List.csv",
        "Order_Items": "Order_Items.csv",
        "Products": "Products.csv",
        "Order_Reviews": "Order_Reviews.csv",
    }

    lines = []
    lines.append("=" * 65)
    lines.append("  TASK 1 - LOAD DATASETS (INFERSCHEMA)")
    lines.append("=" * 65)

    for name, filename in datasets.items():
        filepath = os.path.join(data_dir, filename)
        df = read_csv(spark, filepath)
        row_count = df.count()

        lines.append("")
        lines.append(f"--- {name} ({filename}) ---")
        lines.append(f"Row count: {row_count}")
        lines.append(f"Column count: {len(df.columns)}")
        lines.append("Schema:")
        for field in df.schema.fields:
            lines.append(f"  {field.name}: {field.dataType.simpleString()}")
        lines.append("")

    write_output(lines, os.path.join(results_dir, "task1_load_datasets.txt"))
    print(f"[Task 1] Done -> {os.path.join(results_dir, 'task1_load_datasets.txt')}")


if __name__ == "__main__":
    spark = create_spark_session("Task1_LoadDatasets")
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "data")
    results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "results")
    run(spark, data_dir, results_dir)
    spark.stop()
