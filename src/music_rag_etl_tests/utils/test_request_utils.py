from unittest.mock import MagicMock, patch

import pytest
import requests

from music_rag_etl.utils.request_utils import make_request_with_retries


@pytest.fixture
def mock_context():
    """Fixture for a mock Dagster context."""
    context = MagicMock()
    context.log = MagicMock()
    return context


@patch("requests.request")
def test_make_request_with_retries_post_success(mock_request, mock_context):
    """Tests a successful POST request on the first try."""
    # 1. Setup
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_request.return_value = mock_response

    url = "http://test.com/api"
    params = {"key": "value"}
    headers = {"Content-Type": "application/json"}

    # 2. Action
    response = make_request_with_retries(
        context=mock_context, url=url, method="POST", params=params, headers=headers
    )

    # 3. Assertions
    assert response == mock_response
    mock_request.assert_called_once_with(
        "POST", url, data=params, headers=headers, timeout=60
    )
    mock_context.log.warning.assert_not_called()


@patch("requests.request")
def test_make_request_with_retries_get_success(mock_request, mock_context):
    """Tests a successful GET request on the first try."""
    # 1. Setup
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_request.return_value = mock_response

    url = "http://test.com/api"
    params = {"id": "123"}
    headers = {"Accept": "application/json"}

    # 2. Action
    response = make_request_with_retries(
        context=mock_context, url=url, method="GET", params=params, headers=headers
    )

    # 3. Assertions
    assert response == mock_response
    mock_request.assert_called_once_with(
        "GET", url, params=params, headers=headers, timeout=60
    )
    mock_context.log.warning.assert_not_called()


@patch("time.sleep")
@patch("requests.request")
def test_make_request_with_retries_with_failures(
    mock_request, mock_sleep, mock_context
):
    """Tests that the function retries on failure and succeeds eventually."""
    # 1. Setup
    # Fail twice, then succeed
    mock_request.side_effect = [
        requests.exceptions.Timeout("Timed out"),
        requests.exceptions.ConnectionError("Connection failed"),
        MagicMock(status_code=200),
    ]

    # 2. Action
    make_request_with_retries(
        mock_context, "http://test.com", method="GET", max_retries=3
    )

    # 3. Assertions
    assert mock_request.call_count == 3
    assert mock_context.log.warning.call_count == 2
    assert mock_sleep.call_count == 2
    # Check backoff timing
    mock_sleep.assert_any_call(2)  # initial_backoff
    mock_sleep.assert_any_call(4)  # initial_backoff * 2**1


@patch("time.sleep")
@patch("requests.request", side_effect=requests.exceptions.RequestException("API down"))
def test_make_request_with_retries_exhausted(mock_request, mock_sleep, mock_context):
    """Tests that an exception is raised when all retries are exhausted."""
    # 1. Setup
    max_retries = 3

    # 2. Action & Assertions
    with pytest.raises(
        requests.exceptions.RequestException, match="Failed to fetch data"
    ):
        make_request_with_retries(
            mock_context, "http://test.com", method="POST", max_retries=max_retries
        )

    assert mock_request.call_count == max_retries
    assert mock_context.log.warning.call_count == max_retries
    assert mock_sleep.call_count == max_retries


def test_make_request_with_invalid_method(mock_context):
    """Tests that an invalid HTTP method raises a ValueError."""
    with pytest.raises(ValueError, match="Method must be 'GET' or 'POST'"):
        make_request_with_retries(mock_context, "http://test.com", method="PUT")
