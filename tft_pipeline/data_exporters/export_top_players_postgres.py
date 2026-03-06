from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    """
    Export data to a PostgreSQL database.
    """
    # Use environment variables directly since Mage is running in a container
    # alongside postgres
    
    if df.empty:
        print("Dataframe is empty, nothing to export.")
        return
        
    schema_name = os.getenv('POSTGRES_SCHEMA', 'public')
    table_name = 'raw_top_players'

    # Build connection string from env vars
    host = os.getenv('POSTGRES_HOST', 'postgres')
    port = os.getenv('POSTGRES_PORT', '5432')
    database = os.getenv('POSTGRES_DBNAME', 'tft_data')
    user = os.getenv('POSTGRES_USER', 'postgres')
    password = os.getenv('POSTGRES_PASSWORD', 'postgres')
    
    print(f"Exporting exactly {len(df)} rows to {schema_name}.{table_name}...")

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df,
            schema_name,
            table_name,
            index=False,
            if_exists='replace', # For simplicity, replacing. In prod, 'append' or UPSERT
        )
