"""
Unit tests for the MooVitamix Dagster pipeline.
"""
import os
import sys
import pytest
import requests
import responses
import json
import tempfile
from unittest.mock import patch
from dagster import build_op_context


# Add the directory containing classes_out.py to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../src/moovitamix_fastapi'))

from src.moovitamix_fastapi.generate_fake_data import FakeDataGenerator
from src.moovitamix_dagster.assets import (
    tracks, users, listen_history, fetch_api_data,
    save_tracks, save_users, save_listen_history
)

@pytest.fixture
def fake_data():
    """
    Fixture that generates fake data using the FakeDataGenerator.
    Returns a tuple of (tracks, users, listen_history).
    """
    generator = FakeDataGenerator(data_range_observations=10)
    return generator.generate_fake_data()


@pytest.fixture
def mock_tracks_response(fake_data):
    """Fixture for mocked tracks API response using the FakeDataGenerator."""
    tracks_data = fake_data[0]
    serializable_data = []
    
    for item in tracks_data:
        # Convert datetime objects to ISO format strings
        serializable_item = {
            "id": item.id,
            "name": item.name,
            "artist": item.artist,
            "songwriters": item.songwriters,
            "duration": item.duration,
            "genres": item.genres,
            "album": item.album,
            "created_at": item.created_at.isoformat(),
            "updated_at": item.updated_at.isoformat()
        }
        serializable_data.append(serializable_item)
    
    return {
        "items": serializable_data
    }


@pytest.fixture
def mock_users_response(fake_data):
    """Fixture for mocked users API response using the FakeDataGenerator."""
    users_data = fake_data[1]
    serializable_data = []
    
    for item in users_data:
        # Convert datetime objects to ISO format strings
        serializable_item = {
            "id": item.id,
            "first_name": item.first_name,
            "last_name": item.last_name,
            "email": item.email,
            "gender": item.gender,
            "favorite_genres": item.favorite_genres,
            "created_at": item.created_at.isoformat(),
            "updated_at": item.updated_at.isoformat()
        }
        serializable_data.append(serializable_item)
    
    return {
        "items": serializable_data
    }


@pytest.fixture
def mock_listen_history_response(fake_data):
    """Fixture for mocked listen history API response using the FakeDataGenerator."""
    listen_history_data = fake_data[2]
    serializable_data = []
    
    for item in listen_history_data:
        # Convert datetime objects to ISO format strings
        serializable_item = {
            "user_id": item.user_id,
            "items": item.items,
            "created_at": item.created_at.isoformat(),
            "updated_at": item.updated_at.isoformat()
        }
        serializable_data.append(serializable_item)
    
    return {
        "items": serializable_data
    }


class TestFetchApiData:
    """Tests for the fetch_api_data function."""
    
    @responses.activate
    def test_fetch_api_data_success(self):
        """Test that fetch_api_data correctly retrieves data from an API endpoint."""
        # Mock API response
        test_data = {"items": [{"id": 1, "name": "Test Item"}]}
        responses.add(
            responses.GET,
            "http://127.0.0.1:8000/test-endpoint",
            json=test_data,
            status=200
        )
        
        # Call the function
        result = fetch_api_data("test-endpoint")
        
        # Verify results
        assert result == test_data["items"]
        assert len(responses.calls) == 1
        assert responses.calls[0].request.url == "http://127.0.0.1:8000/test-endpoint"
    
    @responses.activate
    def test_fetch_api_data_error_handling(self):
        """Test that fetch_api_data raises an exception when the API returns an error."""
        # Mock API error response
        responses.add(
            responses.GET,
            "http://127.0.0.1:8000/error-endpoint",
            json={"error": "Not found"},
            status=404
        )
        
        # Verify exception is raised
        with pytest.raises(requests.exceptions.HTTPError):
            fetch_api_data("error-endpoint")


class TestAssetsExecution:
    """Tests for Dagster assets execution."""
    
    @responses.activate
    def test_tracks_asset(self, mock_tracks_response):
        """Test the tracks asset function."""
        # Mock API response
        responses.add(
            responses.GET,
            "http://127.0.0.1:8000/tracks",
            json=mock_tracks_response,
            status=200
        )
        
        # Create a test context
        context = build_op_context()
        
        # Execute the asset
        result = tracks(context)
        
        # Verify the result
        assert result == mock_tracks_response["items"]
        assert len(responses.calls) == 1
    
    @responses.activate
    def test_users_asset(self, mock_users_response):
        """Test the users asset function."""
        # Mock API response
        responses.add(
            responses.GET,
            "http://127.0.0.1:8000/users",
            json=mock_users_response,
            status=200
        )
        
        # Create a test context
        context = build_op_context()
        
        # Execute the asset
        result = users(context)
        
        # Verify the result
        assert result == mock_users_response["items"]
        assert len(responses.calls) == 1
    
    @responses.activate
    def test_listen_history_asset(self, mock_listen_history_response):
        """Test the listen_history asset function."""
        # Mock API response
        responses.add(
            responses.GET,
            "http://127.0.0.1:8000/listen_history",
            json=mock_listen_history_response,
            status=200
        )
        
        # Create a test context
        context = build_op_context()
        
        # Execute the asset
        result = listen_history(context)
        
        # Verify the result
        assert result == mock_listen_history_response["items"]
        assert len(responses.calls) == 1


class TestSaveAssets:
    """Tests for the save assets."""
    
    @patch('src.moovitamix_dagster.assets.get_output_dir')
    @patch('src.moovitamix_dagster.assets.get_timestamp')
    def test_save_tracks(self, mock_timestamp, mock_output_dir, fake_data):
        """Test saving tracks data to a file."""
        # Setup mocks
        mock_timestamp.return_value = "20250323_105600"
        temp_dir = tempfile.mkdtemp()
        mock_output_dir.return_value = temp_dir
        
        # Create test context
        context = build_op_context()
        
        # Get fake tracks data
        tracks_data = fake_data[0]
        
        # Execute the asset
        file_path = save_tracks(context, tracks_data)
        
        # Verify the file was created and contains the correct data
        assert os.path.exists(file_path)
        with open(file_path, 'r') as f:
            saved_data = json.load(f)
        
        # Compare the length since we can't directly compare Pydantic objects with serialized data
        assert len(saved_data) == len(tracks_data)
    
    @patch('src.moovitamix_dagster.assets.get_output_dir')
    @patch('src.moovitamix_dagster.assets.get_timestamp')
    def test_save_users(self, mock_timestamp, mock_output_dir, fake_data):
        """Test saving users data to a file."""
        # Setup mocks
        mock_timestamp.return_value = "20250323_105600"
        temp_dir = tempfile.mkdtemp()
        mock_output_dir.return_value = temp_dir
        
        # Create test context
        context = build_op_context()
        
        # Get fake users data
        users_data = fake_data[1]
        
        # Execute the asset
        file_path = save_users(context, users_data)
        
        # Verify the file was created and contains the correct data
        assert os.path.exists(file_path)
        with open(file_path, 'r') as f:
            saved_data = json.load(f)
        
        # Compare the length since we can't directly compare Pydantic objects with serialized data
        assert len(saved_data) == len(users_data)
    
    @patch('src.moovitamix_dagster.assets.get_output_dir')
    @patch('src.moovitamix_dagster.assets.get_timestamp')
    def test_save_listen_history(self, mock_timestamp, mock_output_dir, fake_data):
        """Test saving listen history data to a file."""
        # Setup mocks
        mock_timestamp.return_value = "20250323_105600"
        temp_dir = tempfile.mkdtemp()
        mock_output_dir.return_value = temp_dir
        
        # Create test context
        context = build_op_context()
        
        # Get fake listen history data
        listen_history_data = fake_data[2]
        
        # Execute the asset
        file_path = save_listen_history(context, listen_history_data)
        
        # Verify the file was created and contains the correct data
        assert os.path.exists(file_path)
        with open(file_path, 'r') as f:
            saved_data = json.load(f)
        
        # Compare the length since we can't directly compare Pydantic objects with serialized data
        assert len(saved_data) == len(listen_history_data)
