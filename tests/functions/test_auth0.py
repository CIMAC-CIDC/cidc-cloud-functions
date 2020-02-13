import json
from datetime import datetime
from unittest.mock import MagicMock, call

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

    send_logs = mock_function_ret_val("_send_new_auth0_logs_to_stackdriver", None)

    save_logs = mock_function_ret_val("_save_new_auth0_logs", None)

    auth0.store_auth0_logs()
    get_token.assert_called_once()
    get_log_id.assert_called_once()
    get_logs.assert_called_once_with(token, log_id)
    send_logs.assert_not_called()
    save_logs.assert_not_called()

    # Populated log results
    logs = ["a", "b", "c"]
    get_logs = mock_function_ret_val("_get_new_auth0_logs", logs)

    auth0.store_auth0_logs()
    send_logs.assert_called_once_with(logs)
    save_logs.assert_called_once_with(logs)


def test_save_new_auth0_logs(monkeypatch):
    """Test that saving logs to GCS works as expected"""
    logs_group_1 = [
        {"_id": 1, "date": "2020-02-13T00:00:01.0Z"},
        {"_id": 2, "date": "2020-02-13T00:00:02.0Z"},
        {"_id": 3, "date": "2020-02-13T00:00:03.0Z"},
    ]
    logs_group_2 = [
        {"_id": 4, "date": "2020-02-14T00:00:04.0Z"},
        {"_id": 5, "date": "2020-02-14T00:00:05.0Z"},
        {"_id": 6, "date": "2020-02-14T00:00:06.0Z"},
    ]
    logs = logs_group_1 + logs_group_2

    _get_log_bucket = MagicMock()
    bucket = MagicMock()
    blob = MagicMock()
    bucket.blob.return_value = blob
    _get_log_bucket.return_value = bucket
    monkeypatch.setattr(auth0, "_get_log_bucket", _get_log_bucket)

    auth0._save_new_auth0_logs(logs)

    assert call("auth0/2020/02/13/00:00:03.json") in bucket.blob.call_args_list
    assert call("auth0/2020/02/14/00:00:06.json") in bucket.blob.call_args_list
    assert call(json.dumps(logs_group_1)) in blob.upload_from_string.call_args_list
    assert call(json.dumps(logs_group_2)) in blob.upload_from_string.call_args_list


def test_get_logfile_name():
    """Check that generated filenames have the expected structure"""
    dt = datetime.now()

    fname = auth0._get_logfile_name(dt)
    assert [dt.year, dt.month, dt.day] == [int(s) for s in fname.split("/")[:3]]
