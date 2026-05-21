"""
VALIDATION SCRIPT - Kiểm tra tính đúng đắn của tất cả thuật toán.
Chạy các query độc lập để cross-check kết quả.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.spark_session import create_spark_session, read_csv
from pyspark.sql.functions import (
    col, count, countDistinct, sum, avg, year, month,
    to_timestamp, dense_rank, round as spark_round,
)
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window


def run_validation():
    base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    data_dir = os.path.join(base, "data")

    spark = create_spark_session("Validation")
    spark.sparkContext.setLogLevel("ERROR")

    df_orders = read_csv(spark, os.path.join(data_dir, "Orders.csv"))
    df_customers = read_csv(spark, os.path.join(data_dir, "Customer_List.csv"))
    df_items = read_csv(spark, os.path.join(data_dir, "Order_Items.csv"))
    df_reviews = read_csv(spark, os.path.join(data_dir, "Order_Reviews.csv"))
    df_products = read_csv(spark, os.path.join(data_dir, "Products.csv"))

    errors = []
    print("=" * 70)
    print("  VALIDATION REPORT")
    print("=" * 70)

    # ----------------------------------------------------------------
    # TASK 1: Row counts & schema
    # ----------------------------------------------------------------
    print("\n--- TASK 1: Load & Schema ---")
    checks = {
        "Orders.csv row count": (df_orders.count(), 99441),
        "Customer_List.csv row count": (df_customers.count(), 102727),
        "Order_Items.csv row count": (df_items.count(), 112650),
        "Products.csv row count": (df_products.count(), 32951),
        "Order_Reviews.csv row count": (df_reviews.count(), 99270),
        "Order_ID column exists (Orders)": ("Order_ID" in df_orders.columns, True),
        "Customer_Trx_ID column exists (Customer_List)": ("Customer_Trx_ID" in df_customers.columns, True),
        "Review_Score column exists (Reviews)": ("Review_Score" in df_reviews.columns, True),
        "Product_Category_Name exists (Products)": ("Product_Category_Name" in df_products.columns, True),
        "Seller_ID column exists (Items)": ("Seller_ID" in df_items.columns, True),
    }
    for name, (actual, expected) in checks.items():
        ok = actual == expected
        status = "OK" if ok else f"FAIL (got {actual}, expected {expected})"
        if not ok:
            errors.append(f"T1: {name}")
        print(f"  [{status}] {name}")

    # ----------------------------------------------------------------
    # TASK 2: Overall statistics
    # ----------------------------------------------------------------
    print("\n--- TASK 2: Overall Statistics ---")
    total_orders = df_orders.count()
    unique_customers = df_customers.select(countDistinct("Customer_Trx_ID")).collect()[0][0]
    unique_sellers = df_items.select(countDistinct("Seller_ID")).collect()[0][0]

    checks2 = {
        "Total orders == 99441": (total_orders, 99441),
        "Unique Customer_Trx_ID from Customer_List": (unique_customers, 99441),
        "Unique sellers from Order_Items": (unique_sellers, 3095),
    }
    for name, (actual, expected) in checks2.items():
        ok = actual == expected
        status = "OK" if ok else f"FAIL (got {actual}, expected {expected})"
        if not ok:
            errors.append(f"T2: {name}")
        print(f"  [{status}] {name}")

    # Cross-check: unique Customer_Trx_ID in Orders must match Customer_List
    orders_cust = df_orders.select(countDistinct("Customer_Trx_ID")).collect()[0][0]
    print(f"  [INFO] Distinct Customer_Trx_ID in Orders: {orders_cust}")
    print(f"  [INFO] Distinct Customer_Trx_ID in Customer_List: {unique_customers}")
    print(f"  [INFO] Distinct Subscriber_ID in Customer_List: {df_customers.select(countDistinct('Subscriber_ID')).collect()[0][0]}")

    # ----------------------------------------------------------------
    # TASK 3: Orders by Country (join integrity)
    # ----------------------------------------------------------------
    print("\n--- TASK 3: Orders by Country ---")
    joined = df_orders.join(df_customers, on="Customer_Trx_ID", how="inner")
    join_count = joined.count()

    # Check join doesn't lose or multiply orders
    checks3 = {
        "Join row count == 99441 (no loss/duplication)": (join_count, total_orders),
        "No NULL country after join": (joined.filter(col("Customer_Country").isNull()).count(), 0),
    }
    for name, (actual, expected) in checks3.items():
        ok = actual == expected
        status = "OK" if ok else f"FAIL (got {actual}, expected {expected})"
        if not ok:
            errors.append(f"T3: {name}")
        print(f"  [{status}] {name}")

    # Verify top country = Germany
    top_country = (
        joined.groupBy("Customer_Country")
        .agg(count("Order_ID").alias("cnt"))
        .orderBy(col("cnt").desc())
        .first()
    )
    print(f"  [INFO] Top country: {top_country['Customer_Country']} ({top_country['cnt']} orders)")
    print(f"  [INFO] Number of countries: {joined.select('Customer_Country').distinct().count()}")

    # ----------------------------------------------------------------
    # TASK 4: Orders by Year/Month
    # ----------------------------------------------------------------
    print("\n--- TASK 4: Orders by Year/Month ---")
    df_ts = df_orders.withColumn(
        "PurchaseDate", to_timestamp(col("Order_Purchase_Timestamp"), "yyyy-MM-dd HH:mm")
    )
    null_ts = df_ts.filter(col("PurchaseDate").isNull()).count()
    print(f"  [{'OK' if null_ts == 0 else 'FAIL'}] NULL timestamps: {null_ts}")
    if null_ts > 0:
        errors.append("T4: NULL timestamps")

    # Check year/month totals sum to 99441
    ym_agg = df_ts.groupBy(year("PurchaseDate"), month("PurchaseDate")).agg(count("Order_ID").alias("cnt"))
    ym_total = ym_agg.agg(sum("cnt")).collect()[0][0]
    print(f"  [{'OK' if ym_total == 99441 else 'FAIL'}] Sum of year/month counts = {ym_total} (expected 99441)")
    if ym_total != 99441:
        errors.append(f"T4: Sum mismatch ({ym_total} != 99441)")

    # ----------------------------------------------------------------
    # TASK 5: Review Score Stats
    # ----------------------------------------------------------------
    print("\n--- TASK 5: Review Score Statistics ---")
    # Check raw data quality
    raw_count = df_reviews.count()
    null_scores = df_reviews.filter(col("Review_Score").isNull()).count()
    empty_scores = df_reviews.filter(col("Review_Score") == "").count()

    df_clean = df_reviews.withColumn("Score", col("Review_Score").cast(IntegerType())) \
        .filter(col("Score").isNotNull() & (col("Score") >= 1) & (col("Score") <= 5))
    valid_count = df_clean.count()
    invalid_count = raw_count - valid_count

    print(f"  [INFO] Total reviews: {raw_count}")
    print(f"  [INFO] NULL Review_Score: {null_scores}")
    print(f"  [INFO] Empty string Review_Score: {empty_scores}")
    print(f"  [INFO] Valid reviews (1-5): {valid_count}")
    print(f"  [INFO] Invalid/outlier: {invalid_count}")

    # Check score distribution sums to valid_count
    dist_sum = df_clean.groupBy("Score").agg(count("*").alias("cnt")).agg(sum("cnt")).collect()[0][0]
    print(f"  [{'OK' if dist_sum == valid_count else 'FAIL'}] Score distribution sum = {dist_sum} (expected {valid_count})")
    if dist_sum != valid_count:
        errors.append("T5: Distribution sum mismatch")

    overall_avg = df_clean.select(spark_round(avg("Score"), 2)).collect()[0][0]
    print(f"  [INFO] Average score: {overall_avg}")

    # Manual verify: avg should be between min(1) and max(5)
    min_s = df_clean.agg({"Score": "min"}).collect()[0][0]
    max_s = df_clean.agg({"Score": "max"}).collect()[0][0]
    print(f"  [INFO] Min score: {min_s}, Max score: {max_s}")
    print(f"  [{'OK' if 1 <= overall_avg <= 5 else 'FAIL'}] Average in range [1,5]")

    # ----------------------------------------------------------------
    # TASK 6: Revenue 2024 by Category
    # ----------------------------------------------------------------
    print("\n--- TASK 6: Revenue 2024 by Category ---")
    df_ts = df_orders.withColumn(
        "PurchaseYear", year(to_timestamp(col("Order_Purchase_Timestamp"), "yyyy-MM-dd HH:mm"))
    )
    df_2024 = df_ts.filter(col("PurchaseYear") == 2024)

    # Check 2024 order count
    count_2024 = df_2024.count()
    print(f"  [INFO] 2024 orders: {count_2024}")

    # Verify join integrity
    joined6 = df_2024.join(df_items, on="Order_ID", how="inner").join(df_products, on="Product_ID", how="inner")
    joined6_count = joined6.count()
    items_2024 = df_2024.join(df_items, on="Order_ID", how="inner").count()
    print(f"  [INFO] Items in 2024 orders: {items_2024}")
    print(f"  [INFO] After join with Products: {joined6_count}")
    print(f"  [{'OK' if joined6_count == items_2024 else 'FAIL'}] No items lost after Products join")
    if joined6_count != items_2024:
        errors.append("T6: Items lost after Products join")

    # Check revenue sum is plausible (positive)
    revenue_total = joined6.agg(sum(col("Price") + col("Freight_Value"))).collect()[0][0]
    print(f"  [INFO] Total revenue 2024: {revenue_total:,.2f}")
    print(f"  [{'OK' if revenue_total > 0 else 'FAIL'}] Revenue > 0")

    # ----------------------------------------------------------------
    # TASK 7: Top Products + Average Rating
    # ----------------------------------------------------------------
    print("\n--- TASK 7: Top Products + Average Rating ---")
    df_reviews_clean = df_reviews.withColumn("Score", col("Review_Score").cast(IntegerType())) \
        .filter(col("Score").isNotNull() & (col("Score") >= 1) & (col("Score") <= 5))

    sales_count = df_items.groupBy("Product_ID").agg(count("Order_Item_ID").alias("SalesCount"))
    total_sales = sales_count.agg(sum("SalesCount")).collect()[0][0]
    print(f"  [{'OK' if total_sales == df_items.count() else 'FAIL'}] Total sales count = {total_sales} (expected {df_items.count()})")
    if total_sales != df_items.count():
        errors.append("T7: Sales count sum mismatch")

    # Check top product matches
    top_product = sales_count.orderBy(col("SalesCount").desc()).first()
    print(f"  [INFO] Top product: {top_product['Product_ID']} ({top_product['SalesCount']} sales)")

    # Verify avg rating per product doesn't exceed [1,5]
    avg_rating = (
        df_items.join(df_reviews_clean, on="Order_ID", how="inner")
        .groupBy("Product_ID")
        .agg(avg("Score").alias("AvgReviewScore"))
    )
    bad_avg = avg_rating.filter((col("AvgReviewScore") < 1) | (col("AvgReviewScore") > 5)).count()
    print(f"  [{'OK' if bad_avg == 0 else 'FAIL'}] All avg ratings in [1,5]: {bad_avg} outliers")
    if bad_avg > 0:
        errors.append("T7: Avg rating out of range")

    # ----------------------------------------------------------------
    # TASK 10: Seller Ranking
    # ----------------------------------------------------------------
    print("\n--- TASK 10: Seller Ranking ---")
    seller_stats = df_items.groupBy("Seller_ID").agg(
        sum(col("Price") + col("Freight_Value")).alias("TotalRevenue"),
        countDistinct("Order_ID").alias("OrderCount"),
    )
    seller_count = seller_stats.count()
    print(f"  [{'OK' if seller_count == 3095 else 'FAIL'}] Unique sellers: {seller_count} (expected 3095)")
    if seller_count != 3095:
        errors.append("T10: Seller count mismatch")

    # Check dense_rank: rank 1 seller has highest revenue
    window_spec = Window.orderBy(col("TotalRevenue").desc())
    ranked = seller_stats.withColumn("Rank", dense_rank().over(window_spec))
    rank1 = ranked.filter(col("Rank") == 1).count()
    top_seller = ranked.orderBy("Rank").first()
    print(f"  [INFO] Rank 1 sellers: {rank1}")
    print(f"  [INFO] Top seller: {top_seller['Seller_ID']} (Revenue: {top_seller['TotalRevenue']:,.2f}, Orders: {top_seller['OrderCount']})")
    print(f"  [{'OK' if rank1 >= 1 else 'FAIL'}] At least 1 rank-1 seller")

    # Verify dense_rank continuity (no gaps)
    ranks = [r["Rank"] for r in ranked.select("Rank").distinct().orderBy("Rank").collect()]
    expected_ranks = list(range(1, len(ranks) + 1))
    print(f"  [{'OK' if ranks == expected_ranks else 'FAIL'}] dense_rank continuity: {ranks[:10]}...")
    if ranks != expected_ranks:
        errors.append("T10: dense_rank has gaps")

    # ----------------------------------------------------------------
    # EDGE CASES
    # ----------------------------------------------------------------
    print("\n--- EDGE CASES ---")

    # Check for duplicate Order_ID in Orders
    dup_orders = df_orders.groupBy("Order_ID").count().filter(col("count") > 1).count()
    print(f"  [{'OK' if dup_orders == 0 else 'FAIL'}] Duplicate Order_ID in Orders: {dup_orders}")
    if dup_orders > 0:
        errors.append("EDGE: Duplicate Order_ID in Orders")

    # Check for orphan orders (no matching customer)
    orphan_orders = df_orders.join(df_customers, on="Customer_Trx_ID", how="left_anti").count()
    print(f"  [{'OK' if orphan_orders == 0 else 'FAIL'}] Orphan orders (no customer): {orphan_orders}")
    if orphan_orders > 0:
        errors.append("EDGE: Orphan orders")

    # Check for orphan items (no matching order)
    orphan_items = df_items.join(df_orders, on="Order_ID", how="left_anti").count()
    print(f"  [{'OK' if orphan_items == 0 else 'FAIL'}] Orphan items (no order): {orphan_items}")
    if orphan_items > 0:
        errors.append("EDGE: Orphan items")

    # Check for orphan reviews (no matching order)
    orphan_reviews = df_reviews.join(df_orders, on="Order_ID", how="left_anti").count()
    print(f"  [{'OK' if orphan_reviews == 0 else 'FAIL'}] Orphan reviews (no order): {orphan_reviews}")
    if orphan_reviews > 0:
        errors.append("EDGE: Orphan reviews")

    # Check for products with no category
    null_cat = df_products.filter(col("Product_Category_Name").isNull()).count()
    print(f"  [{'OK' if null_cat == 0 else 'FAIL'}] Products with NULL category: {null_cat}")
    if null_cat > 0:
        errors.append("EDGE: NULL product category")

    # Check for negative prices
    neg_price = df_items.filter(col("Price") < 0).count()
    neg_freight = df_items.filter(col("Freight_Value") < 0).count()
    print(f"  [{'OK' if neg_price == 0 else 'ISSUE'}] Negative prices: {neg_price}")
    print(f"  [{'OK' if neg_freight == 0 else 'ISSUE'}] Negative freight: {neg_freight}")

    # ----------------------------------------------------------------
    # SUMMARY
    # ----------------------------------------------------------------
    print("\n" + "=" * 70)
    if errors:
        print(f"  VALIDATION FAILED - {len(errors)} issue(s) found:")
        for e in errors:
            print(f"    - {e}")
    else:
        print("  ALL VALIDATIONS PASSED")
    print("=" * 70)

    spark.stop()
    return len(errors) == 0


if __name__ == "__main__":
    ok = run_validation()
    sys.exit(0 if ok else 1)
