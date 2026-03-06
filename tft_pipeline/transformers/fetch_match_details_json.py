import pandas as pd
import requests
import os
import time
import json

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

import random

def get_match_details(match_id, api_key, routing='sea'):
    url = f"https://{routing}.api.riotgames.com/tft/match/v1/matches/{match_id}"
    headers = {"X-Riot-Token": api_key}
    while True:
        response = requests.get(url, headers=headers)
        if response.status_code == 429:
            # Policy Compliance: Respect Retry-After
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
    
    rows = []
    
    print(f"Fetching details for {len(data)} match IDs...")
    for idx, row in data.iterrows():
        match_id = row['match_id']
        try:
            match_json = get_match_details(match_id, api_key, routing)
            time.sleep(1.2) # Rate limiting buffer
            
            rows.append({
                "match_id": match_id,
                "data_version": match_json.get("metadata", {}).get("data_version", "unknown"),
                "match_info": json.dumps(match_json), # Save as JSON string
                "fetched_at": pd.Timestamp.utcnow()
            })
        except Exception as e:
            print(f"Failed to fetch match {match_id}: {e}")
            
    print(f"Successfully fetched details for {len(rows)} matches.")
    return pd.DataFrame(rows)
