import pandas as pd
import requests
import os
import time

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def get_puuid_from_summoner(summoner_id, api_key, region='vn2'):
    url = f"https://{region}.api.riotgames.com/tft/summoner/v1/summoners/{summoner_id}"
    headers = {"X-Riot-Token": api_key}
    while True:
        response = requests.get(url, headers=headers)
        if response.status_code == 429:
            time.sleep(int(response.headers.get("Retry-After", 10)))
            continue
        response.raise_for_status()
        return response.json().get('puuid')

import random

def get_match_ids_by_puuid(puuid, api_key, routing='sea', count=20):
    url = f"https://{routing}.api.riotgames.com/tft/match/v1/matches/by-puuid/{puuid}/ids?start=0&count={count}"
    headers = {"X-Riot-Token": api_key}
    while True:
        response = requests.get(url, headers=headers)
        if response.status_code == 429:
            # Respect Riot Policy: Sleep then retry
            retry_after = int(response.headers.get("Retry-After", 20))
            print(f"Rate limited! Waiting {retry_after} seconds...")
            time.sleep(retry_after + random.uniform(1, 3)) # Add jitter
            continue
        response.raise_for_status()
        return response.json()

@transformer
def transform(data, *args, **kwargs):
    api_key = os.getenv('RIOT_API_KEY')
    routing = os.getenv('RIOT_ROUTING', 'sea')
    
    # args[0] will be the dataframe from load_existing_match_ids
    existing_match_ids = set()
    if len(args) > 0 and isinstance(args[0], pd.DataFrame):
        existing_match_ids = set(args[0]['match_id'].tolist())
    
    all_match_ids = set()
    rows = []
    
    print(f"Fetching matches for {len(data)} players...")
    print(f"Skipping {len(existing_match_ids)} already known matches.")
    
    for idx, row in data.iterrows():
        puuid = row.get('puuid')
        if not puuid:
            continue
        
        try:
            # Get Match IDs (Last 10 matches)
            # In a true streaming setup, we could pass startTime here
            match_ids = get_match_ids_by_puuid(puuid, api_key, routing, count=10)
            time.sleep(1.2)
            
            for match_id in match_ids:
                # Filter against BOTH in-memory and in-database IDs
                if match_id not in all_match_ids and match_id not in existing_match_ids:
                    all_match_ids.add(match_id)
                    rows.append({
                        "match_id": match_id,
                        "puuid": puuid,
                        "fetched_at": pd.Timestamp.utcnow()
                    })
        except Exception as e:
            print(f"Failed to fetch for puuid {puuid}: {e}")
            
    print(f"Found {len(rows)} NEW unique matches.")
    return pd.DataFrame(rows)

@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
    assert len(output) > 0, 'No match IDs fetched'
