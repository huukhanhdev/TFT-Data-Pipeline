from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path
from pandas import DataFrame

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def load_data_from_postgres(*args, **kwargs):
    # Incremental: Only fetch IDs that are in raw_match_ids but NOT in raw_matches
    query = '''
        SELECT match_id 
        FROM public.raw_match_ids 
        WHERE match_id NOT IN (SELECT match_id FROM public.raw_matches)
        LIMIT 500;
    '''
    
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        return loader.load(query)
