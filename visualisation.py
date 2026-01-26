import os
import logging
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_style("whitegrid")
plt.rcParams["figure.figsize"] = (10, 6)

def visualize_tmdb_from_url(df_url, output_dir="/tmdbmovies/app/data/diagrams", log_dir="/tmdbmovies/app/logs", file_type="csv"):
    # create directories if not exist
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)
    
    # setup logging
    logger = logging.getLogger(__name__)
    log_file = os.path.join(log_dir, f"visualization_{datetime.now():%Y%m%d_%H%M%S}.log")
    handler = logging.FileHandler(log_file)
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(handler)
    
    logger.info("Starting TMDB visualizations")
    
    # read dataframe from URL
    if file_type.lower() == "csv":
        pandas_df = pd.read_csv(df_url)
    elif file_type.lower() == "json":
        pandas_df = pd.read_json(df_url)
    else:
        logger.error("Unsupported file type. Use 'csv' or 'json'.")
        return
    
    logger.info(f"Loaded DataFrame from {df_url} with {len(pandas_df)} rows")
    
    # Revenue vs Budget Trend
    trend = pandas_df.sort_values('budget_musd')
    plt.figure()
    sns.lineplot(data=trend, x='budget_musd', y='revenue_musd')
    plt.xlabel("Budget (M USD)")
    plt.ylabel("Revenue (M USD)")
    plt.title("Revenue vs Budget (Trend Line)")
    plot_path = os.path.join(output_dir, "revenue_vs_budget.png")
    plt.savefig(plot_path)
    plt.close()
    logger.info(f"Saved plot: {plot_path}")
    
    # ROI by Genre
    pandas_df['roi'] = pandas_df['revenue_musd'] / pandas_df['budget_musd']
    df_genres = pandas_df.assign(genres=pandas_df['genres'].str.split('|')).explode('genres')
    plt.figure()
    sns.boxplot(data=df_genres, x='genres', y='roi')
    plt.xticks(rotation=45)
    plt.title("ROI Distribution by Genre")
    plot_path = os.path.join(output_dir, "roi_by_genre.png")
    plt.savefig(plot_path)
    plt.close()
    logger.info(f"Saved plot: {plot_path}")
    
    # Top 10 Popular Movies by Rating
    top_pop = pandas_df.sort_values('popularity', ascending=False).head(10)
    plt.figure()
    sns.barplot(data=top_pop, x='vote_average', y='title', palette='magma')
    plt.xlabel("Average Rating")
    plt.ylabel("Movie")
    plt.title("Top 10 Popular Movies by Rating")
    plot_path = os.path.join(output_dir, "top10_popular_movies.png")
    plt.savefig(plot_path)
    plt.close()
    logger.info(f"Saved plot: {plot_path}")
    
    # Yearly Box Office Performance
    pandas_df['release_year'] = pd.to_datetime(pandas_df['release_date'], errors='coerce').dt.year
    yearly_revenue = pandas_df.groupby('release_year')['revenue_musd'].sum().reset_index()
    plt.figure()
    sns.lineplot(data=yearly_revenue, x='release_year', y='revenue_musd', marker='o')
    plt.title("Yearly Box Office Performance")
    plot_path = os.path.join(output_dir, "yearly_box_office.png")
    plt.savefig(plot_path)
    plt.close()
    logger.info(f"Saved plot: {plot_path}")
    
    # Franchise vs Standalone
    pandas_df['is_franchise'] = pandas_df['belongs_to_collection'].notnull()
    franchise_stats = pandas_df.groupby('is_franchise')['revenue_musd'].mean().reset_index()
    plt.figure()
    sns.barplot(data=franchise_stats, x='is_franchise', y='revenue_musd')
    plt.xticks([0,1], ['Standalone', 'Franchise'])
    plt.title("Average Revenue: Franchise vs Standalone")
    plot_path = os.path.join(output_dir, "franchise_vs_standalone.png")
    plt.savefig(plot_path)
    plt.close()
    logger.info(f"Saved plot: {plot_path}")
    
    logger.info("All TMDB visualizations completed")

    
    
    
    
    



