"""
Unit tests for the MooVitamix Dagster pipeline.
"""
import os
import sys
import pytest
import responses
import json
import tempfile
from unittest.mock import patch
from dagster import build_op_context

# Add the project root directory to Python path so imports work correctly
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.moovitamix_dagster.assets import (
    tracks, users, listen_history, fetch_api_data,
    save_tracks, save_users, save_listen_history
)
from src.moovitamix_fastapi.generate_fake_data import FakeDataGenerator


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
    return {
        "items": tracks_data
    }


@pytest.fixture
def mock_users_response(fake_data):
    """Fixture for mocked users API response using the FakeDataGenerator."""
    users_data = fake_data[1]
    return {
        "items": users_data
    }


@pytest.fixture
def mock_listen_history_response(fake_data):
    """Fixture for mocked listen history API response using the FakeDataGenerator."""
    listen_history_data = fake_data[2]
    return {
        "items": listen_history_data
    }


class TestFetchApiData:
    """Tests for the fetch_api_data function."""

class TestAssetsExecution:
    """Tests for Dagster assets execution."""

class TestSaveAssets:
    """Tests for the save assets."""
    