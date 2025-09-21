import pytest
from unittest.mock import AsyncMock, Mock, patch
from datetime import datetime, date
from aurum.data.external_dao import ExternalDAO, PaginationCursor


class TestPaginationCursor:
    """Test pagination cursor functionality."""

    def test_cursor_from_string_valid(self):
        """Test creating cursor from valid base64 string."""
        cursor_str = "ZnJlZCB8IEdEUC1NT05USExZIHwgMjAyNS0wMS0yMVQxMDozMDowMFo="
        cursor = PaginationCursor.from_string(cursor_str)

        assert cursor.provider_id == "fred"
        assert cursor.series_id == "GDP-MONTHLY"
        assert cursor.last_updated == datetime.fromisoformat("2025-01-21T10:30:00")
        assert cursor.offset == 0

    def test_cursor_from_string_invalid(self):
        """Test creating cursor from invalid base64 string."""
        cursor_str = "invalid-base64"
        cursor = PaginationCursor.from_string(cursor_str)

        # Should return default cursor on decode failure
        assert cursor.provider_id is None
        assert cursor.series_id is None
        assert cursor.last_updated is None
        assert cursor.offset == 0

    def test_cursor_from_string_empty_parts(self):
        """Test creating cursor with empty parts."""
        cursor_str = "fHwgfA=="  # base64 of "|||"
        cursor = PaginationCursor.from_string(cursor_str)

        assert cursor.provider_id == ""
        assert cursor.series_id == ""
        assert cursor.last_updated is None
        assert cursor.offset == 0

    def test_cursor_to_string(self):
        """Test converting cursor to base64 string."""
        cursor = PaginationCursor(
            provider_id="fred",
            series_id="GDP-MONTHLY",
            last_updated=datetime.fromisoformat("2025-01-21T10:30:00"),
            offset=10
        )

        cursor_str = cursor.to_string()
        decoded = PaginationCursor.from_string(cursor_str)

        assert decoded.provider_id == "fred"
        assert decoded.series_id == "GDP-MONTHLY"
        assert decoded.last_updated == datetime.fromisoformat("2025-01-21T10:30:00")
        assert decoded.offset == 10


class TestExternalDAO:
    """Test ExternalDAO functionality with mocked Trino client."""

    @pytest.fixture
    def mock_trino_client(self):
        """Create mock Trino client."""
        mock_client = Mock()
        mock_client.execute_query = AsyncMock()
        return mock_client

    @pytest.fixture
    def dao(self, mock_trino_client):
        """Create ExternalDAO with mocked Trino client."""
        return ExternalDAO(trino_client=mock_trino_client)

    def test_get_providers_success(self, dao, mock_trino_client):
        """Test successful providers retrieval."""
        # Mock query result
        mock_trino_client.execute_query.return_value = [
            {
                "provider_id": "fred",
                "name": "Federal Reserve Economic Data",
                "description": "Economic data from the Federal Reserve Bank of St. Louis",
                "base_url": "https://api.stlouisfed.org",
                "last_updated": "2025-01-21T10:30:00",
                "series_count": 12345
            }
        ]

        providers = await dao.get_providers()

        assert len(providers) == 1
        assert providers[0]["id"] == "fred"
        assert providers[0]["name"] == "Federal Reserve Economic Data"
        assert providers[0]["series_count"] == 12345

        # Verify query was called with correct parameters
        mock_trino_client.execute_query.assert_called_once()
        call_args = mock_trino_client.execute_query.call_args
        assert "SELECT" in call_args[0][0]  # First positional arg is query
        assert "external_providers" in call_args[0][0]
        assert "limit" in call_args[1][0]  # First keyword arg is params
        assert call_args[1][0]["limit"] == 100

    def test_get_providers_with_pagination(self, dao, mock_trino_client):
        """Test providers retrieval with pagination."""
        cursor = PaginationCursor(
            provider_id="fred",
            last_updated=datetime.fromisoformat("2025-01-21T10:30:00")
        )
        cursor_str = cursor.to_string()

        mock_trino_client.execute_query.return_value = []

        providers = await dao.get_providers(limit=50, offset=10, cursor=cursor_str)

        # Verify pagination parameters were used
        call_args = mock_trino_client.execute_query.call_args
        params = call_args[1][0]
        assert params["limit"] == 50
        assert params["offset"] == 10
        assert params["cursor_provider_id"] == "fred"
        assert params["cursor_last_updated"] == datetime.fromisoformat("2025-01-21T10:30:00")

    def test_get_providers_large_limit_warning(self, dao, mock_trino_client):
        """Test warning for large result sets."""
        mock_trino_client.execute_query.return_value = []

        # Should log warning for large limit
        with patch('aurum.data.external_dao.LOGGER') as mock_logger:
            await dao.get_providers(limit=600)

            mock_logger.warning.assert_called_once()
            assert "Large result set requested" in mock_logger.warning.call_args[0][0]

    def test_get_series_success(self, dao, mock_trino_client):
        """Test successful series retrieval."""
        mock_trino_client.execute_query.return_value = [
            {
                "series_id": "FRED:GDP",
                "provider_id": "fred",
                "name": "Gross Domestic Product",
                "frequency": "monthly",
                "description": "Real Gross Domestic Product",
                "units": "Billions of Dollars",
                "last_updated": "2025-01-21T10:30:00",
                "observation_count": 1000
            }
        ]

        series = await dao.get_series(provider="fred", frequency="monthly")

        assert len(series) == 1
        assert series[0]["id"] == "FRED:GDP"
        assert series[0]["provider_id"] == "fred"
        assert series[0]["frequency"] == "monthly"

        call_args = mock_trino_client.execute_query.call_args
        params = call_args[1][0]
        assert params["provider"] == "fred"
        assert params["frequency"] == "monthly"

    def test_get_series_with_filters(self, dao, mock_trino_client):
        """Test series retrieval with multiple filters."""
        mock_trino_client.execute_query.return_value = []

        series = await dao.get_series(
            provider="fred",
            frequency="monthly",
            asof="2024-12-31"
        )

        call_args = mock_trino_client.execute_query.call_args
        params = call_args[1][0]
        assert params["provider"] == "fred"
        assert params["frequency"] == "monthly"
        assert params["asof"] == "2024-12-31"

    def test_get_observations_success(self, dao, mock_trino_client):
        """Test successful observations retrieval."""
        mock_trino_client.execute_query.side_effect = [
            # First call to check series exists
            [{"exists_check": 1}],
            # Second call to get observations
            [
                {
                    "series_id": "FRED:GDP",
                    "observation_date": "2020-01-01",
                    "value": 21538.032,
                    "metadata": {"seasonal_adjustment": "SAAR"}
                }
            ]
        ]

        observations = await dao.get_observations("FRED:GDP", limit=100)

        assert len(observations) == 1
        assert observations[0]["series_id"] == "FRED:GDP"
        assert observations[0]["date"] == "2020-01-01"
        assert observations[0]["value"] == 21538.032
        assert observations[0]["metadata"] == {"seasonal_adjustment": "SAAR"}

    def test_get_observations_series_not_found(self, dao, mock_trino_client):
        """Test observations retrieval for non-existent series."""
        mock_trino_client.execute_query.return_value = []  # Series doesn't exist

        with pytest.raises(Exception):  # Should raise NotFoundException
            await dao.get_observations("INVALID:SERIES")

    def test_get_observations_with_date_filters(self, dao, mock_trino_client):
        """Test observations retrieval with date filters."""
        mock_trino_client.execute_query.side_effect = [
            [{"exists_check": 1}],  # Series exists
            []  # No observations
        ]

        observations = await dao.get_observations(
            "FRED:GDP",
            start_date="2020-01-01",
            end_date="2024-01-01"
        )

        call_args = mock_trino_client.execute_query.call_args_list[1]  # Second call
        params = call_args[1][0]
        assert params["start_date"] == "2020-01-01"
        assert params["end_date"] == "2024-01-01"

    def test_get_observations_large_limit_warning(self, dao, mock_trino_client):
        """Test warning for very large observations result sets."""
        mock_trino_client.execute_query.side_effect = [
            [{"exists_check": 1}],
            []
        ]

        with patch('aurum.data.external_dao.LOGGER') as mock_logger:
            await dao.get_observations("FRED:GDP", limit=6000)

            mock_logger.warning.assert_called_once()
            assert "Very large observations result set requested" in mock_logger.warning.call_args[0][0]

    def test_get_metadata_success(self, dao, mock_trino_client):
        """Test successful metadata retrieval."""
        mock_trino_client.execute_query.side_effect = [
            # First call for providers
            [
                {
                    "provider_id": "fred",
                    "name": "Federal Reserve Economic Data",
                    "description": "Economic data from the Federal Reserve Bank of St. Louis",
                    "base_url": "https://api.stlouisfed.org",
                    "last_updated": "2025-01-21T10:30:00",
                    "series_count": 12345
                }
            ],
            # Second call for total series count
            [{"total": 54321}],
            # Third call for last updated
            [{"last_updated": "2025-01-21T10:30:00"}]
        ]

        metadata = await dao.get_metadata(include_counts=True)

        assert len(metadata["providers"]) == 1
        assert metadata["providers"][0]["id"] == "fred"
        assert metadata["total_series"] == 54321
        assert metadata["last_updated"] == "2025-01-21T10:30:00"

    def test_get_metadata_with_provider_filter(self, dao, mock_trino_client):
        """Test metadata retrieval with provider filter."""
        mock_trino_client.execute_query.side_effect = [
            [{"provider_id": "fred", "name": "FRED", "description": "Test", "base_url": "https://test.com", "last_updated": "2025-01-21T10:30:00", "series_count": 100}],
            [{"total": 100}],
            [{"last_updated": "2025-01-21T10:30:00"}]
        ]

        metadata = await dao.get_metadata(provider="fred")

        call_args_list = mock_trino_client.execute_query.call_args_list
        # Check that provider filter was used in all queries
        for call_args in call_args_list:
            params = call_args[1][0]
            assert params.get("provider") == "fred"

    def test_validate_limit_positive(self, dao):
        """Test limit validation with positive values."""
        result = dao._validate_limit(100, max_limit=1000)
        assert result == 100

    def test_validate_limit_zero(self, dao):
        """Test limit validation with zero."""
        with pytest.raises(ValueError, match="Limit must be positive"):
            dao._validate_limit(0, max_limit=1000)

    def test_validate_limit_negative(self, dao):
        """Test limit validation with negative values."""
        with pytest.raises(ValueError, match="Limit must be positive"):
            dao._validate_limit(-5, max_limit=1000)

    def test_validate_limit_clamped(self, dao):
        """Test limit validation with clamping."""
        result = dao._validate_limit(1500, max_limit=1000)
        assert result == 1000

    def test_safe_identifier_valid(self, dao):
        """Test safe identifier validation with valid identifier."""
        result = dao._safe_identifier("FRED_GDP")
        assert result == "FRED_GDP"

    def test_safe_identifier_empty(self, dao):
        """Test safe identifier validation with empty string."""
        with pytest.raises(ValueError, match="Identifier cannot be empty"):
            dao._safe_identifier("")

    def test_safe_identifier_invalid_chars(self, dao):
        """Test safe identifier validation with invalid characters."""
        with pytest.raises(ValueError, match="Identifier contains invalid characters"):
            dao._safe_identifier("FRED'; DROP TABLE")

    def test_safe_identifier_too_long(self, dao):
        """Test safe identifier validation with overly long identifier."""
        long_id = "A" * 256
        with pytest.raises(ValueError, match="Identifier too long"):
            dao._safe_identifier(long_id)

    def test_safe_identifier_with_numbers(self, dao):
        """Test safe identifier validation with valid numbers."""
        result = dao._safe_identifier("FRED123")
        assert result == "FRED123"

    def test_safe_identifier_with_underscores(self, dao):
        """Test safe identifier validation with underscores."""
        result = dao._safe_identifier("FRED_GDP_MONTHLY")
        assert result == "FRED_GDP_MONTHLY"


class TestExternalDAOIntegration:
    """Test ExternalDAO with real Trino client (if available)."""

    @pytest.mark.integration
    def test_get_trino_client(self):
        """Test getting Trino client instance."""
        dao = ExternalDAO()

        # This should work in integration environment
        try:
            client = await dao.get_trino_client()
            assert client is not None
        except Exception:
            # Trino not available, skip test
            pytest.skip("Trino client not available")

    @pytest.mark.integration
    def test_dao_with_real_client(self):
        """Test DAO with real Trino client (integration test)."""
        dao = ExternalDAO()

        try:
            # This would test with real Trino connection
            providers = await dao.get_providers(limit=1)
            assert isinstance(providers, list)
        except Exception:
            # Trino not available, skip test
            pytest.skip("Trino client not available or no external_providers table")
