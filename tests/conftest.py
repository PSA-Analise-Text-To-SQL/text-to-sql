import pytest
from unittest.mock import MagicMock
from src.services.database_service import DatabaseService
from src.models.database_parameters import DatabaseParameters

@pytest.fixture
def db_service():
    return DatabaseService()

@pytest.fixture
def mock_params():
    params = MagicMock(spec=DatabaseParameters)
    params.get_uri.return_value = "sqlite:///:memory:"
    return params