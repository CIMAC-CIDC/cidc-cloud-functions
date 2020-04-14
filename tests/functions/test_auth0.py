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


def test_get_last_auth0_log_id(monkeypatch):
    """Test getting last log id"""

    _get_log_bucket = MagicMock()
    bucket = MagicMock()
    get_blob = MagicMock()
    bucket.get_blob.return_value = get_blob
    list_blobs = MagicMock()
    bucket.list_blobs.return_value = list_blobs
    _get_log_bucket.return_value = bucket
    monkeypatch.setattr(auth0, "_get_log_bucket", _get_log_bucket)

    auth0._get_last_auth0_log_id()

    assert call(prefix="auth0/__last_log_id/") in bucket.list_blobs.call_args_list
    # falls back to old way
    assert call("auth0/__last_log_id.txt") in bucket.get_blob.call_args_list

    bucket.list_blobs.reset_mock()
    bucket.get_blob.reset_mock()

    # new setup - two old log id record
    last_logid = MagicMock()
    last_logid.name = "auth0/__last_log_id/2020/01/02__last_log_id.txt"
    last_logid.download_as_string.return_value = b"123"
    prev_logid = MagicMock()
    prev_logid.name = "auth0/__last_log_id/2020/01/01__last_log_id.txt"
    bucket.list_blobs.return_value = [last_logid]

    assert "123" == auth0._get_last_auth0_log_id()

    assert call(prefix="auth0/__last_log_id/") in bucket.list_blobs.call_args_list
    last_logid.download_as_string.assert_called_once()
    # doesn't fall back to old way
    assert not bucket.get_blob.call_args_list


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
