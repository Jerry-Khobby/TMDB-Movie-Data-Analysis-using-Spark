from typing import Dict
from pyspark.sql import DataFrame
from datetime import datetime
import os
import shutil


def save_kpi_results(
    kpi_results: Dict[str, DataFrame],
    base_output_dir: str,
    kpi_group: str
) -> None:
    """
    Save each KPI as ONE clean CSV file (no Spark folders).
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    group_dir = os.path.join(base_output_dir, kpi_group)
    os.makedirs(group_dir, exist_ok=True)

    for kpi_name, df in kpi_results.items():
        tmp_dir = os.path.join(group_dir, f"_{kpi_name}_tmp")

        final_csv = os.path.join(
            group_dir,
            f"{kpi_name}_{timestamp}.csv"
        )

        (
            df
            .coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(tmp_dir)
        )

        # move part file to final CSV
        for file in os.listdir(tmp_dir):
            if file.startswith("part-") and file.endswith(".csv"):
                shutil.move(
                    os.path.join(tmp_dir, file),
                    final_csv
                )

        shutil.rmtree(tmp_dir)
