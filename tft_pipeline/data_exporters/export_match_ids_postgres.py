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
    if df.empty:
        print("Dataframe is empty, nothing to export.")
        return
        
    schema_name = os.getenv('POSTGRES_SCHEMA', 'public')
    table_name = 'raw_match_ids'
    
    print(f"Exporting {len(df)} match IDs to {schema_name}.{table_name}...")

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df,
            schema_name,
            table_name,
            index=False,
            if_exists='append', # Append new match IDs
        )
