import logging 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, FloatType, ArrayType, StructType, StructField, DateType,IntegerType,DoubleType 
from pyspark.sql.window import Window 
import os 
from datetime import datetime 
import shutil


LOG_DIR="/tmdbmovies/app/logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(
  LOG_DIR, f"tmdb_transform_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)





logging.info(f"Logging initialized. Log file: {log_file}")
""" spark = SparkSession.builder.appName("TMDB_Raw_Transform").getOrCreate() """



def clean_data(df: DataFrame, validate: bool = True) -> DataFrame:
    logger = logging.getLogger(__name__)

    logger.info("Starting TMDB data cleaning pipeline")

    # Drop irrelevant columns
    logger.info("Dropping irrelevant columns")
    df = df.drop("adult", "imdb_id", "original_title", "video", "homepage")

    # Extract belongs_to_collection name
    logger.info("Extracting belongs_to_collection name")
    df = df.withColumn(
        "belongs_to_collection",
        F.col("belongs_to_collection.name")
    )

    # Extract genres
    logger.info("Flattening genres")
    df = df.withColumn(
        "genres",
        F.expr("concat_ws('|', transform(genres, x -> x.name))")
    )

    # Extract spoken languages
    logger.info("Flattening spoken_languages")
    df = df.withColumn(
        "spoken_languages",
        F.expr("concat_ws('|', transform(spoken_languages, x -> x.english_name))")
    )

    # Extract production countries
    logger.info("Flattening production_countries")
    df = df.withColumn(
        "production_countries",
        F.expr("concat_ws('|', transform(production_countries, x -> x.name))")
    )

    # Extract production companies
    logger.info("Flattening production_companies")
    df = df.withColumn(
        "production_companies",
        F.expr("concat_ws('|', transform(production_companies, x -> x.name))")
    )

    # Convert numeric columns
    logger.info("Casting numeric columns")
    df = (
        df
        .withColumn("budget", F.col("budget").cast(DoubleType()))
        .withColumn("id", F.col("id").cast(IntegerType()))
        .withColumn("popularity", F.col("popularity").cast(DoubleType()))
        .withColumn("revenue", F.col("revenue").cast(DoubleType()))
        .withColumn("runtime", F.col("runtime").cast(DoubleType()))
        .withColumn("vote_count", F.col("vote_count").cast(IntegerType()))
        .withColumn("vote_average", F.col("vote_average").cast(DoubleType()))
    )

    # Convert release_date to date
    logger.info("Converting release_date to date")
    df = df.withColumn(
        "release_date",
        F.to_date("release_date", "yyyy-MM-dd")
    )

    # Replace zero values with null
    logger.info("Replacing zero values with null")
    for col in ["budget", "revenue", "runtime"]:
        df = df.withColumn(
            col,
            F.when(F.col(col) == 0, None).otherwise(F.col(col))
        )

    # Convert budget and revenue to million USD
    logger.info("Converting budget and revenue to million USD")
    df = (
        df
        .withColumn("budget_musd", F.col("budget") / 1_000_000)
        .withColumn("revenue_musd", F.col("revenue") / 1_000_000)
    )

    # Handle vote_count = 0
    logger.info("Handling vote_count = 0")
    df = df.withColumn(
        "vote_average",
        F.when(F.col("vote_count") == 0, None)
         .otherwise(F.col("vote_average"))
    )

    # Replace placeholder text
    logger.info("Replacing placeholder text with null")
    placeholders = ["No Data", "N/A", "", "null"]
    df = (
        df
        .withColumn(
            "overview",
            F.when(F.col("overview").isin(placeholders), None)
             .otherwise(F.col("overview"))
        )
        .withColumn(
            "tagline",
            F.when(F.col("tagline").isin(placeholders), None)
             .otherwise(F.col("tagline"))
        )
    )

    # Remove duplicates
    logger.info("Dropping duplicate movies")
    df = df.dropDuplicates(["id"])

    # Drop rows with missing id or title
    logger.info("Dropping rows with missing id or title")
    df = df.dropna(subset=["id", "title"])

    # Keep rows with at least 10 non-null values
    logger.info("Filtering rows with insufficient data")
    df = df.na.drop(thresh=10)

    # Keep only released movies
    logger.info("Filtering released movies")
    df = df.filter(F.col("status") == "Released").drop("status")

    # Extract cast
    logger.info("Extracting cast information")
    df = df.withColumn(
        "cast",
        F.expr("""
            concat_ws(
                '|',
                transform(
                    slice(credits.cast, 1, 5),
                    x -> x.name
                )
            )
        """)
    )

    # Cast size
    df = df.withColumn("cast_size", F.size("credits.cast"))

    # Director
    logger.info("Extracting director")
    df = df.withColumn(
        "director",
        F.expr("""
            element_at(
                transform(
                    filter(credits.crew, x -> x.job = 'Director'),
                    x -> x.name
                ),
                1
            )
        """)
    )

    # Crew size
    df = df.withColumn("crew_size", F.size("credits.crew"))

    # Drop credits struct
    df = df.drop("credits")

    # Reorder columns
    logger.info("Reordering final columns")
    final_columns = [
        "id", "title", "tagline", "release_date", "genres",
        "belongs_to_collection", "original_language",
        "budget_musd", "revenue_musd",
        "production_companies", "production_countries",
        "vote_count", "vote_average", "popularity", "runtime",
        "overview", "spoken_languages", "poster_path",
        "cast", "cast_size", "director", "crew_size"
    ]
    df = df.select(*final_columns)

    # Reset index using row_number
    logger.info("Resetting row index")
    df = df.withColumn(
        "row_id",
        F.row_number().over(Window.orderBy("id"))
    ).drop("row_id")

    if validate:
        logger.info("Validation summary")
        logger.info(f"Final row count: {df.count()}")
        logger.info(f"Final column count: {len(df.columns)}")

    logger.info("TMDB data cleaning pipeline completed successfully")

    return df
  

def save_clean_data_csv(df, output_path: str):
    logger = logging.getLogger("tmdb_cleaning")
    logger.info(f"Saving cleaned DataFrame to CSV at {output_path}")

    # Coalesce to 1 partition so we get a single CSV
    temp_folder = output_path + "_tmp"
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(temp_folder)

    # Move the part-xxxx file to final CSV
    for f in os.listdir(temp_folder):
        if f.startswith("part-") and f.endswith(".csv"):
            shutil.move(os.path.join(temp_folder, f), output_path)
            logger.info(f"CSV saved successfully as {output_path}")
            break

    # Remove temp folder
    shutil.rmtree(temp_folder)
    
    


def run_transformation(
    spark: SparkSession,
    raw_path: str,
    output_csv: str
) -> str:
    logger = logging.getLogger(__name__)
    log_file = f"/tmdbmovies/app/logs/transform_{datetime.now():%Y%m%d_%H%M%S}.log"
    handler = logging.FileHandler(log_file)
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(handler)
    df_raw = spark.read.json(raw_path)
    df_clean = clean_data(df_raw)
    save_clean_data_csv(df_clean, output_csv)
    return output_csv

