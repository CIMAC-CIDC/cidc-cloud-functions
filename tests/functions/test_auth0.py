from datetime import datetime
from unittest.mock import MagicMock

from functions import auth0


def test_store_auth0_logs(monkeypatch):
    """Test store_auth0_logs control flow"""

    def mock_function_ret_val(function_name, return_value):
        f = MagicMock()
        f.return_value = return_value
        monkeypatch.setattr(auth0, function_name, f)
        return f

    token = "test-token"
    get_token = mock_function_ret_val("_get_auth0_access_token", token)

    log_id = "test-logid"
    get_log_id = mock_function_ret_val("_get_last_auth0_log_id", log_id)

    # Empty log results
    logs = []
    get_logs = mock_function_ret_val("_get_new_auth0_logs", logs)

    save_logs = mock_function_ret_val("_save_new_auth0_logs", None)

    auth0.store_auth0_logs()
    get_token.assert_called_once()
    get_log_id.assert_called_once()
    get_logs.assert_called_once_with(token, log_id)
    save_logs.assert_not_called()

    # Populated log results
    logs = ["a", "b", "c"]
    get_logs = mock_function_ret_val("_get_new_auth0_logs", logs)

    auth0.store_auth0_logs()
    save_logs.assert_called_once_with(logs)


def test_get_logfile_name():
    """Check that generated filenames have the expected structure"""
    dt = datetime.now()

    fname = auth0._get_logfile_name(dt)
    assert [dt.year, dt.month, dt.day] == [int(s) for s in fname.split("/")[:3]]
