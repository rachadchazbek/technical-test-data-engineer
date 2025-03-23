"""
Dagster assets for fetching data from MooVitamix FastAPI.
"""
import os
import json
import requests
from datetime import datetime
from dagster import asset, AssetExecutionContext


def fetch_api_data(endpoint: str, base_url: str = "http://127.0.0.1:8000") -> list:
    """
    Fetch data from a MooVitamix API endpoint.
    
    Args:
        endpoint: API endpoint path (without leading slash)
        base_url: Base URL for the API
        
    Returns:
        List of records from the API
    """
    url = f"{base_url}/{endpoint}"
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for HTTP errors
    data = response.json()
    return data.get("items", [])


@asset
def tracks(context: AssetExecutionContext) -> list:
    """Fetch tracks data from the API."""
    context.log.info("Fetching tracks data from API")
    tracks_data = fetch_api_data("tracks")
    context.log.info(f"Retrieved {len(tracks_data)} tracks")
    return tracks_data


@asset
def users(context: AssetExecutionContext) -> list:
    """Fetch users data from the API."""
    context.log.info("Fetching users data from API")
    users_data = fetch_api_data("users")
    context.log.info(f"Retrieved {len(users_data)} users")
    return users_data


@asset
def listen_history(context: AssetExecutionContext) -> list:
    """Fetch listen history data from the API."""
    context.log.info("Fetching listen history data from API")
    history_data = fetch_api_data("listen_history")
    context.log.info(f"Retrieved {len(history_data)} listen history records")
    return history_data


@asset(
    deps=["tracks", "users", "listen_history"]
)
def save_data_locally(
    context: AssetExecutionContext, 
    tracks: list, 
    users: list, 
    listen_history: list
) -> None:
    """
    Save all fetched data to local JSON files.
    
    Args:
        context: Dagster execution context
        tracks: List of track records
        users: List of user records
        listen_history: List of listen history records
    """
    output_dir = os.path.join(os.getcwd(), "data")
    os.makedirs(output_dir, exist_ok=True)
    
    # Add timestamp to filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save tracks data
    tracks_file = os.path.join(output_dir, f"tracks_{timestamp}.json")
    with open(tracks_file, "w") as f:
        json.dump(tracks, f, indent=2, default=str)
    context.log.info(f"Saved tracks data to {tracks_file}")
    
    # Save users data
    users_file = os.path.join(output_dir, f"users_{timestamp}.json")
    with open(users_file, "w") as f:
        json.dump(users, f, indent=2, default=str)
    context.log.info(f"Saved users data to {users_file}")
    
    # Save listen history data
    history_file = os.path.join(output_dir, f"listen_history_{timestamp}.json")
    with open(history_file, "w") as f:
        json.dump(listen_history, f, indent=2, default=str)
    context.log.info(f"Saved listen history data to {history_file}")