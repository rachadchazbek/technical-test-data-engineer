"""
Dagster definitions including jobs, assets, and resources.
"""
from dagster import Definitions, define_asset_job, load_assets_from_modules

from . import assets

# Load all assets from the assets module
all_assets = load_assets_from_modules([assets])

# Define a job that materializes all assets
data_ingestion_job = define_asset_job(
    name="data_ingestion_job",
    selection="*",  # Select all assets
    description="Job to fetch and save data from MooVitamix API",
)

# Create the Definitions object with assets and jobs
defs = Definitions(
    assets=all_assets,
    jobs=[data_ingestion_job],
)
