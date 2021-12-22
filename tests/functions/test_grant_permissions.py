import functions.grant_permissions
from functions.grant_permissions import grant_all_download_permissions
from unittest.mock import MagicMock


def test_grant_all_download_permissions(monkeypatch):
    mock_api_call = MagicMock()
    monkeypatch.setattr(
        functions.grant_permissions.Permissions,
        "grant_all_download_permissions",
        mock_api_call,
    )

    grant_all_download_permissions({}, None)
    mock_api_call.assert_called_once()
