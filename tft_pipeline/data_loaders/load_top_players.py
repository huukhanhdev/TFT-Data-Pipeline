import io
import pandas as pd
import requests
import os
import time

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def get_league_data(api_key, tier, region='vn2', max_retries=5):
    """
    Fetch league data for Challenger, Grandmaster, or Master.
    For lower tiers (Diamond etc.), Riot uses a different endpoint.
    """
    if tier in ['CHALLENGER', 'GRANDMASTER', 'MASTER']:
        url = f"https://{region}.api.riotgames.com/tft/league/v1/{tier.lower()}"
    else:
        # For Diamond, we fetch Division I as a sample to avoid massive data
        url = f"https://{region}.api.riotgames.com/tft/league/v1/entries/{tier}/I?page=1"
        
    headers = {"X-Riot-Token": api_key}
    
    retries = 0
    while retries < max_retries:
        response = requests.get(url, headers=headers, timeout=20)
        if response.status_code == 429:
            retries += 1
            retry_after = int(response.headers.get("Retry-After", 10))
            print(f"Rate limited! Waiting {retry_after} seconds (Attempt {retries}/{max_retries})...")
            time.sleep(retry_after)
            continue
        response.raise_for_status()
        
        json_data = response.json()
        # The list endpoint returns a list directly, the top tier endpoints return a dict with 'entries'
        if isinstance(json_data, list):
            return {"entries": json_data, "tier": tier}
        return json_data
    
    raise Exception(f"Max retries ({max_retries}) exceeded for {tier}.")

@data_loader
def load_data_from_api(*args, **kwargs):
    api_key = os.getenv('RIOT_API_KEY')
    region = os.getenv('RIOT_REGION', 'vn2')
    
    if not api_key or api_key == "RGAPI-YOUR-KEY-HERE":
        raise ValueError("Please set a valid RIOT_API_KEY in the .env file")
        
    all_players = []
    # Tiers to fetch
    tiers = ['CHALLENGER', 'GRANDMASTER', 'MASTER', 'DIAMOND']
    
    for tier in tiers:
        print(f"Fetching {tier} league data...")
        try:
            data = get_league_data(api_key, tier, region)
            entries = data.get('entries', [])
            
            for entry in entries:
                entry['tier'] = tier
                all_players.append(entry)
            
            print(f"Loaded {len(entries)} players from {tier}.")
            time.sleep(1.2) # Small delay between tiers
        except Exception as e:
            print(f"Error fetching {tier}: {e}")
            
    df = pd.DataFrame(all_players)
    if df.empty:
        return df

    # Standardize columns:
    # Riot now returns PUUID directly in league endpoints!
    required_cols = ['puuid', 'leaguePoints', 'tier', 'rank']
    
    # Ensure rank column exists (Diamond/Master have it, others are implicitly 'I')
    if 'rank' not in df.columns:
        df['rank'] = 'I'
    
    df = df[required_cols]
    df['queue'] = 'RANKED_TFT'
    
    print(f"Total top players loaded: {len(df)}")
    return df

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert len(output) > 0, 'No players were fetched'
    assert 'puuid' in output.columns, 'puuid column missing'
