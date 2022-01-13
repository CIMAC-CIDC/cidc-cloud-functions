import functions.grant_permissions
from functions.grant_permissions import grant_download_permissions
import pytest
from unittest.mock import MagicMock


def test_grant_download_permissions(monkeypatch):
    mock_api_call = MagicMock()
    monkeypatch.setattr(
        functions.grant_permissions.Permissions,
        "grant_download_permissions",
        mock_api_call,
    )

    # no matching does nothing at all, just logging
    mock_extract_data = MagicMock()
    mock_extract_data.return_value = "{}"
    monkeypatch.setattr(
        functions.grant_permissions, "extract_pubsub_data", mock_extract_data
    )
    with pytest.raises(
        Exception, match="trial_id and upload_type must both be provided, you provided:"
    ):
        grant_download_permissions({}, None)

    # incomplete/incorrect matching does nothing at all, just logging
    mock_extract_data = MagicMock()
    mock_extract_data.return_value = str({"trial_id": "foo", "user": "baz"})
    monkeypatch.setattr(
        functions.grant_permissions, "extract_pubsub_data", mock_extract_data
    )
    with pytest.raises(
        Exception, match="trial_id and upload_type must both be provided, you provided:"
    ):
        grant_download_permissions({}, None)

    # with data response, calls
    mock_extract_data = MagicMock()
    mock_extract_data.return_value = str({"trial_id": "foo", "upload_type": "bar"})
    monkeypatch.setattr(
        functions.grant_permissions, "extract_pubsub_data", mock_extract_data
    )
    grant_download_permissions({}, None)
    mock_api_call.assert_called_once()
    _, kwargs = mock_api_call.call_args_list[0]
    assert kwargs.get("trial_id") == "foo"
    assert kwargs.get("upload_type") == "bar"
