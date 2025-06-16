import requests
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, round as _round

# === 0. Завантаження CSV-файлів з Google Drive ===
def download_from_drive(file_id, output_path):
    url = f"https://drive.google.com/uc?export=download&id={file_id}"
    if not os.path.exists(output_path):
        print(f"Завантаження {output_path} ...")
        response = requests.get(url)
        with open(output_path, "wb") as f:
            f.write(response.content)
        print(f"{output_path} збережено.")
    else:
        print(f"{output_path} вже існує — пропущено.")

files = {
    "user.csv": "1GN8o8vYP8LLj3A0UWkvbgnf0YjcF3PkV",
    "product.csv": "1im5cGVl1z8ejypTyNJZSQ2lqMGh6tTFN",
    "purchases.csv": "1cDLTc0KYQfnsDWHLR0cJkF5puBpAo2Uh"
}

for filename, file_id in files.items():
    download_from_drive(file_id, filename)

# === 1. Створення Spark сесії ===
spark = SparkSession.builder \
    .appName("Homework1 - Product Purchase Analysis") \
    .getOrCreate()

# === 2. Завантаження CSV як DataFrame ===
users_df = spark.read.option("header", "true").option("inferSchema", "true").csv("user.csv")
products_df = spark.read.option("header", "true").option("inferSchema", "true").csv("product.csv")
purchases_df = spark.read.option("header", "true").option("inferSchema", "true").csv("purchases.csv")

# === 3. Видалення рядків з пропущеними значеннями ===
users_df = users_df.dropna()
products_df = products_df.dropna()
purchases_df = purchases_df.dropna()

# === 4. Загальна сума покупок за кожною категорією ===
merged = purchases_df.join(products_df, on="product_id", how="inner")
total_by_category = merged.groupBy("category").agg(_sum("price").alias("total_spent"))
print("Загальна сума покупок за категоріями:")
total_by_category.show()

# === 5. Сума покупок за категоріями для вікової групи 18–25 ===
full = purchases_df.join(users_df, on="user_id").join(products_df, on="product_id")
young = full.filter((col("age") >= 18) & (col("age") <= 25))
young_total_by_category = young.groupBy("category").agg(_sum("price").alias("total_spent_18_25"))
print("Сума покупок за категоріями (18-25):")
young_total_by_category.show()

# === 6. Частка витрат за категоріями у віковій групі 18–25 ===
total_young_spent = young_total_by_category.agg(_sum("total_spent_18_25").alias("total")).collect()[0]["total"]
young_percentage = young_total_by_category.withColumn(
    "percentage", _round(col("total_spent_18_25") / total_young_spent * 100, 2)
)
print("Частка витрат за категоріями (18-25):")
young_percentage.show()

# === 7. Топ-3 категорії з найбільшими витратами серед молоді ===
top3 = young_percentage.orderBy(col("percentage").desc()).limit(3)
print("Топ-3 категорії серед 18-25 років:")
top3.show()

# === Завершення сесії ===
spark.stop()
