from pyspark.sql import SparkSession
from my_project.layers.bronze import load_bronze
from my_project.layers.silver import transform_silver
from my_project.layers.gold import transform_gold

def run_pipeline(spark: SparkSession, input_path: str) -> dict:
    """
    Runs the full medallion pipeline.
    Returns a dict of DataFrames for each layer.
    """
    print("loading bronze layer...")
    bronze_df = load_bronze(spark,input_path)

    print("loading silver layer...")
    silver_df = transform_silver(bronze_df)

    print("loading gold layer...")
    gold_df = transform_gold(silver_df)

    return {
        "bronze": bronze_df,
        "silver": silver_df,
        "gold": gold_df
    }

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("customer_pipeline") \
        .getOrCreate()

    results = run_pipeline(spark, "data/raw/customers.csv")

    print("\n=== BRONZE (raw) ===")
    results["bronze"].show()

    print("\n=== SILVER (cleaned) ===")
    results["silver"].show()

    print("\n=== GOLD (aggregated) ===")
    results["gold"].show()

