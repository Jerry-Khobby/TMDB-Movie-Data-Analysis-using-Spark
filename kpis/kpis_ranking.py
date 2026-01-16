import logging
import os
from datetime import datetime
from typing import Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession 

 
def compute_tmdb_kpis(
    df: DataFrame,
    top_n: int = 10,
    log_dir: str = "/tmdbmovies/app/logs"
) -> Dict[str, DataFrame]:
    """
    Compute KPI rankings for TMDB movies dataset.

    This version is refactored to be configuration-driven:
    - KPI definitions are stored in a list to remove repeated code
    - Ranking and filtering are generic
    - Logging is consistent for all KPIs
    - Returns results in a dictionary keyed by KPI name

    Parameters
    ----------
    df : DataFrame
        Input Spark DataFrame containing TMDB movie data.
    top_n : int, optional
        Number of top/bottom records to return per KPI (default = 10)
    log_dir : str, optional
        Directory to save logs

    Returns
    -------
    Dict[str, DataFrame]
        Dictionary of KPI results
    """

    # Setup logging
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(
        log_dir, f"tmdb_kpis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)
    logger.info("TMDB KPI computation started")
    logger.info(f"Top-N threshold set to {top_n}")
    logger.info(f"Input row count: {df.count()}")

    # Validate required columns
    required_columns = {
        "title", "budget_musd", "revenue_musd",
        "vote_count", "vote_average", "popularity"
    }
    missing_cols = required_columns - set(df.columns)
    if missing_cols:
        logger.error(f"Missing required columns: {missing_cols}")
        raise ValueError(f"Dataset missing required columns: {missing_cols}")
    logger.info("Input schema validation passed")

    # Feature engineering: profit & ROI
    logger.info("Computing profit and ROI")
    df = df.withColumn("profit", F.col("revenue_musd") - F.col("budget_musd"))
    df = df.withColumn(
        "roi",
        F.when(F.col("budget_musd") >= 10, F.col("revenue_musd") / F.col("budget_musd"))
    )

    # KPI configuration
    kpis = [
        {"name": "highest_revenue", "col": "revenue_musd", "order": "desc", "filter": None, "output_cols": ["rank", "title", "revenue_musd"]},
        {"name": "highest_budget", "col": "budget_musd", "order": "desc", "filter": None, "output_cols": ["rank", "title", "budget_musd"]},
        {"name": "highest_profit", "col": "profit", "order": "desc", "filter": None, "output_cols": ["rank", "title", "profit"]},
        {"name": "lowest_profit", "col": "profit", "order": "asc", "filter": None, "output_cols": ["rank", "title", "profit"]},
        {"name": "highest_roi", "col": "roi", "order": "desc", "filter": F.col("budget_musd") >= 10, "output_cols": ["rank", "title", "roi"]},
        {"name": "lowest_roi", "col": "roi", "order": "asc", "filter": F.col("budget_musd") >= 10, "output_cols": ["rank", "title", "roi"]},
        {"name": "most_voted", "col": "vote_count", "order": "desc", "filter": None, "output_cols": ["rank", "title", "vote_count"]},
        {"name": "highest_rated", "col": "vote_average", "order": "desc", "filter": F.col("vote_count") >= 10, "output_cols": ["rank", "title", "vote_average", "vote_count"]},
        {"name": "lowest_rated", "col": "vote_average", "order": "asc", "filter": F.col("vote_count") >= 10, "output_cols": ["rank", "title", "vote_average", "vote_count"]},
        {"name": "most_popular", "col": "popularity", "order": "desc", "filter": None, "output_cols": ["rank", "title", "popularity"]}
    ]

    results: Dict[str, DataFrame] = {}

    # Generic function to rank movies by a KPI
    def rank_movies(df_kpi: DataFrame, column: str, order: str, filter_expr: Optional[F.Column] = None) -> DataFrame:
        if filter_expr is not None:
            df_kpi = df_kpi.filter(filter_expr)
        window_spec = Window.orderBy(F.desc(column) if order == "desc" else F.asc(column))
        return df_kpi.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") <= top_n)

    # Helper to log top rows
    def log_top(df_kpi: DataFrame, label: str, cols: List[str]):
        rows = df_kpi.limit(3).collect()
        for r in rows:
            values = ", ".join([f"{c}={r[c]}" for c in cols])
            logger.info(f"{label} | {values}")

    # Compute KPIs
    for kpi in kpis:
        logger.info(f"Computing KPI: {kpi['name']}")
        df_kpi = rank_movies(df, kpi["col"], kpi["order"], kpi["filter"])
        results[kpi["name"]] = df_kpi.select(*kpi["output_cols"])
        log_top(results[kpi["name"]], kpi["name"], kpi["output_cols"])

    logger.info("TMDB KPI computation completed successfully")
    logger.info(f"Total KPI outputs generated: {len(results)}")

    return results


spark = SparkSession.builder .appName("TMDB_KPI_Test").getOrCreate()





csv_path = "/tmdbmovies/app/data/clean/tmdb_movies_clean.csv"

df = spark.read.csv(csv_path, header=True, inferSchema=True)
df.show(5)  # Quick check
df.printSchema()
    
kpi_results = compute_tmdb_kpis(df, top_n=5)  # Use 5 to make it easier to inspect

    # Verify results: show top 3 rows for each KPI
for kpi_name, kpi_df in kpi_results.items():
    print(f"\n=== {kpi_name.upper()} ===")
    kpi_df.show(3, truncate=False)