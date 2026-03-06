import requests
import time
import os

class RiotAPIClient:
    def __init__(self, api_key, region='vn2', routing='sea'):
        self.api_key = api_key
        self.region = region
        self.routing = routing
        self.headers = {
            "X-Riot-Token": self.api_key
        }
        
    def _get(self, url):
        """Helper method to execute GET requests with basic rate limit handling."""
        while True:
            response = requests.get(url, headers=self.headers)
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 10))
                print(f"Rate limited! Waiting {retry_after} seconds...")
                time.sleep(retry_after)
                continue
            response.raise_for_status()
            return response.json()

    def get_challenger_league(self):
        """Fetch the challenger league for TFT."""
        url = f"https://{self.region}.api.riotgames.com/tft/league/v1/challenger"
        return self._get(url)
        
    def get_summoner_by_id(self, summoner_id):
        """Fetch summoner details by summoner ID."""
        # Note: As of mid-2024, Riot encourages using Account API for puuid, 
        # but TFT summoner API still works for some region-specific queries.
        url = f"https://{self.region}.api.riotgames.com/tft/summoner/v1/summoners/{summoner_id}"
        return self._get(url)
        
    def get_matches_by_puuid(self, puuid, count=20):
        """Fetch recent match IDs for a player."""
        url = f"https://{self.routing}.api.riotgames.com/tft/match/v1/matches/by-puuid/{puuid}/ids?start=0&count={count}"
        return self._get(url)
        
    def get_match_details(self, match_id):
        """Fetch the full match details."""
        url = f"https://{self.routing}.api.riotgames.com/tft/match/v1/matches/{match_id}"
        return self._get(url)

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    
    API_KEY = os.getenv('RIOT_API_KEY')
    REGION = os.getenv('RIOT_REGION', 'vn2')
    ROUTING = os.getenv('RIOT_ROUTING', 'sea')
    
    if API_KEY and API_KEY != "RGAPI-YOUR-KEY-HERE":
        client = RiotAPIClient(API_KEY, REGION, ROUTING)
        print("API Client initialized. Testing with Challenger League...")
        try:
            challengers = client.get_challenger_league()
            print(f"Found {len(challengers.get('entries', []))} challengers.")
        except Exception as e:
            print(f"Error checking API: {e}")
    else:
        print("Please add a valid Riot API key to the .env file.")
