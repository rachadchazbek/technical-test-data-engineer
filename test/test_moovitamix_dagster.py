"""
Unit tests for the MooVitamix Dagster pipeline.
"""
import os
import sys
import pytest
import responses
import json
from dagster import build_op_context

# Add the project root directory to Python path so imports work correctly
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.moovitamix_dagster.assets import tracks, users, listen_history, fetch_api_data


@pytest.fixture
def mock_tracks_response():
    """Fixture for mocked tracks API response."""
    return {
        "items": [
            {
                "id": 1,
                "name": "Song1",
                "artist": "Artist1",
                "songwriters": "Writer1",
                "duration": "3:45",
                "genres": "Rock",
                "album": "Album1",
                "created_at": "2023-01-01T00:00:00",
                "updated_at": "2023-01-02T00:00:00"
            },
            {
                "id": 2,
                "name": "Song2",
                "artist": "Artist2",
                "songwriters": "Writer2",
                "duration": "4:20",
                "genres": "Pop",
                "album": "Album2",
                "created_at": "2023-02-01T00:00:00",
                "updated_at": "2023-02-02T00:00:00"
            }
        ]
    }


@pytest.fixture
def mock_users_response():
    """Fixture for mocked users API response."""
    return {
        "items": [
            {
                "id": 1,
                "first_name": "John",
                "last_name": "Doe",
                "email": "john@example.com",
                "gender": "Male",
                "favorite_genres": "Rock",
                "created_at": "2023-01-01T00:00:00",
                "updated_at": "2023-01-02T00:00:00"
            },
            {
                "id": 2,
                "first_name": "Jane",
                "last_name": "Smith",
                "email": "jane@example.com",
                "gender": "Female",
                "favorite_genres": "Jazz",
                "created_at": "2023-02-01T00:00:00",
                "updated_at": "2023-02-02T00:00:00"
            }
        ]
    }


@pytest.fixture
def mock_listen_history_response():
    """Fixture for mocked listen history API response."""
    return {
        "items": [
            {
                "user_id": 1,
                "items": [101, 102, 103, 104, 105],
                "created_at": "2023-01-01T00:00:00",
                "updated_at": "2023-01-02T00:00:00"
            },
            {
                "user_id": 2,
                "items": [201, 202, 203, 204, 205],
                "created_at": "2023-02-01T00:00:00",
                "updated_at": "2023-02-02T00:00:00"
            }
        ]
    }


class TestFetchApiData:
    """Tests for the fetch_api_data function."""

    @responses.activate
    def test_fetch_api_data(self, mock_tracks_response):
        """Test that fetch_api_data correctly fetches and parses API data."""
        # Setup mock response
        responses.add(
            responses.GET,
            "http://127.0.0.1:8000/tracks",
            json=mock_tracks_response,
            status=200
        )
        
        # Call the function
        result = fetch_api_data("tracks")
        
        # Verify the result
        assert len(result) == 2
        assert result[0]["name"] == "Song1"
        assert result[1]["name"] == "Song2"


class TestAssetsExecution:
    """Tests for Dagster assets execution."""

    @responses.activate
    def test_tracks_asset(self, mock_tracks_response):
        """Test that the tracks asset correctly fetches track data."""
        # Setup mock response
        responses.add(
            responses.GET,
            "http://127.0.0.1:8000/tracks",
            json=mock_tracks_response,
            status=200
        )
        
        # Create a context for testing
        context = build_op_context()
        
        # Execute the asset
        result = tracks(context)
        
        # Verify the result
        assert len(result) == 2
        assert result[0]["name"] == "Song1"
        assert result[1]["name"] == "Song2"

    @responses.activate
    def test_users_asset(self, mock_users_response):
        """Test that the users asset correctly fetches user data."""
        # Setup mock response
        responses.add(
            responses.GET,
            "http://127.0.0.1:8000/users",
            json=mock_users_response,
            status=200
        )
        
        # Create a context for testing
        context = build_op_context()
        
        # Execute the asset
        result = users(context)
        
        # Verify the result
        assert len(result) == 2
        assert result[0]["first_name"] == "John"
        assert result[1]["first_name"] == "Jane"

    @responses.activate
    def test_listen_history_asset(self, mock_listen_history_response):
        """Test that the listen_history asset correctly fetches history data."""
        # Setup mock response
        responses.add(
            responses.GET,
            "http://127.0.0.1:8000/listen_history",
            json=mock_listen_history_response,
            status=200
        )
        
        # Create a context for testing
        context = build_op_context()
        
        # Execute the asset
        result = listen_history(context)
        
        # Verify the result
        assert len(result) == 2
        assert result[0]["user_id"] == 1
        assert result[1]["user_id"] == 2
