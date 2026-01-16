import logging
import os
from datetime import datetime
from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def advanced_tmdb_kpis(df: DataFrame, top_n: int = 10, log_dir: str = "/tmdbmovies/app/logs") -> Dict[str, DataFrame]:
    # setup logging
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"tmdb_advanced_kpis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
    )
    logger = logging.getLogger(__name__)
    logger.info("Advanced TMDB KPI computation started")
    logger.info(f"Top-N threshold: {top_n}, Input rows: {df.count()}")

    # CAST COLUMNS FOR EFFICIENCY-
    logger.info("Casting numeric columns to correct types for calculations")
    numeric_columns = {
        "budget_musd": "double",
        "revenue_musd": "double",
        "vote_average": "double",
        "vote_count": "int",
        "popularity": "double",
        "runtime": "int"
    }
    for col_name, dtype in numeric_columns.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(dtype))
    logger.info("Casting complete")

    # add profit and ROI columns
    logger.info("Calculating profit and ROI")
    df = df.withColumn("profit", F.col("revenue_musd") - F.col("budget_musd"))
    df = df.withColumn("roi", F.when(F.col("budget_musd") >= 10, F.col("revenue_musd") / F.col("budget_musd")))

    results = {}

    # generic ranking function
    def rank(df_kpi: DataFrame, col: str, order: str = "desc", filter_expr=None):
        if filter_expr is not None:
            df_kpi = df_kpi.filter(filter_expr)
        window_spec = Window.orderBy(F.desc(col) if order == "desc" else F.asc(col))
        return df_kpi.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") <= top_n)

    # helper to log top rows
    def log_top(df_kpi: DataFrame, label: str, cols: list):
        rows = df_kpi.limit(3).collect()
        for r in rows:
            values = ", ".join([f"{c}={r[c]}" for c in cols])
            logger.info(f"{label} | {values}")

    # KPI rankings
    kpi_configs = [
        {"name": "highest_revenue", "col": "revenue_musd"},
        {"name": "highest_budget", "col": "budget_musd"},
        {"name": "highest_profit", "col": "profit"},
        {"name": "lowest_profit", "col": "profit", "order": "asc"},
        {"name": "highest_roi", "col": "roi", "filter": F.col("budget_musd") >= 10},
        {"name": "lowest_roi", "col": "roi", "order": "asc", "filter": F.col("budget_musd") >= 10},
        {"name": "most_voted", "col": "vote_count"},
        {"name": "highest_rated", "col": "vote_average", "filter": F.col("vote_count") >= 10},
        {"name": "lowest_rated", "col": "vote_average", "order": "asc", "filter": F.col("vote_count") >= 10},
        {"name": "most_popular", "col": "popularity"}
    ]

    for kpi in kpi_configs:
        df_kpi = rank(df, kpi["col"], kpi.get("order", "desc"), kpi.get("filter"))
        results[kpi["name"]] = df_kpi
        log_top(df_kpi, kpi["name"], ["rank", "title", kpi["col"]])

    # Advanced Searches
    logger.info("Running advanced search: Bruce Willis in Sci-Fi/Action movies")
    search1 = df.filter(
        F.array_contains(F.split(F.col("genres"), "\\|"), "Science Fiction") &
        F.array_contains(F.split(F.col("genres"), "\\|"), "Action") &
        F.array_contains(F.split(F.col("cast"), "\\|"), "Bruce Willis")
    ).orderBy(F.desc("vote_average"))
    results["search_bruce_willis_sci_fi_action"] = search1
    log_top(search1, "Search 1 - Bruce Willis Sci-Fi Action", ["title", "vote_average"])

    logger.info("Running advanced search: Uma Thurman + Quentin Tarantino")
    search2 = df.filter(
        F.array_contains(F.split(F.col("cast"), "\\|"), "Uma Thurman") &
        (F.col("director") == "Quentin Tarantino")
    ).orderBy(F.asc("runtime"))
    results["search_uma_thurman_tarentino"] = search2
    log_top(search2, "Search 2 - Uma Thurman + Tarantino", ["title", "runtime"])


    # Franchise vs Standalone
    logger.info("Analyzing franchise vs standalone movies")
    df_franchise = df.withColumn("is_franchise", F.when(F.col("belongs_to_collection").isNotNull(), True).otherwise(False))
    franchise_stats = df_franchise.groupBy("is_franchise").agg(
        F.mean("revenue_musd").alias("mean_revenue"),
        F.expr("percentile_approx(roi, 0.5)").alias("median_roi"),
        F.mean("budget_musd").alias("mean_budget"),
        F.mean("popularity").alias("mean_popularity"),
        F.mean("vote_average").alias("mean_rating")
    )
    results["franchise_vs_standalone"] = franchise_stats
    log_top(franchise_stats, "Franchise vs Standalone", ["is_franchise", "mean_revenue", "median_roi"])

    # Most Successful Franchises
    logger.info("Computing most successful franchises")
    franchises = df.filter(F.col("belongs_to_collection").isNotNull()).groupBy("belongs_to_collection").agg(
        F.count("*").alias("total_movies"),
        F.sum("budget_musd").alias("total_budget"),
        F.mean("budget_musd").alias("mean_budget"),
        F.sum("revenue_musd").alias("total_revenue"),
        F.mean("revenue_musd").alias("mean_revenue"),
        F.mean("vote_average").alias("mean_rating")
    ).orderBy(F.desc("total_revenue"))
    results["most_successful_franchises"] = franchises
    log_top(franchises, "Most Successful Franchises", ["belongs_to_collection", "total_revenue", "mean_rating"])

    # Most Successful Directors
    logger.info("Computing most successful directors")
    directors = df.groupBy("director").agg(
        F.count("*").alias("total_movies"),
        F.sum("revenue_musd").alias("total_revenue"),
        F.mean("vote_average").alias("mean_rating")
    ).orderBy(F.desc("total_revenue"))
    results["most_successful_directors"] = directors
    log_top(directors, "Most Successful Directors", ["director", "total_revenue", "mean_rating"])

    logger.info("Advanced TMDB KPI computation completed")
    return results



from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TMDB_advanced_analysis").getOrCreate()
df_tmdb = spark.read.option("header", True).option("inferSchema", True).csv("/tmdbmovies/app/data/clean/tmdb_movies_clean.csv")

results= advanced_tmdb_kpis(df_tmdb,top_n=10)
print("Top 5 Highest Revenue Movies:")
results["highest_revenue"].show(truncate=False)