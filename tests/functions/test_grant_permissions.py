import functions.grant_permissions
from functions.grant_permissions import grant_download_permissions
from functions.settings import GOOGLE_WORKER_TOPIC
import pytest
from unittest.mock import MagicMock


def test_grant_download_permissions(monkeypatch):
    mock_user_list = MagicMock()
    mock_user_list.return_value = ["foo@bar.com", "user@test.com"]
    monkeypatch.setattr(
        functions.grant_permissions.Permissions,
        "get_user_list_for_trial_type",
        mock_user_list,
    )

    mock_blob_list = MagicMock()
    # need more than 100 to test chunking
    mock_blob_list.return_value = [f"blob{n}" for n in range(100 + 50)]
    monkeypatch.setattr(functions.grant_permissions, "get_blob_names", mock_blob_list)

    mock_encode_and_publish = MagicMock()
    monkeypatch.setattr(
        functions.grant_permissions, "_encode_and_publish", mock_encode_and_publish
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
    mock_user_list.assert_called_once()  # not once_with because of unbound session
    _, kwargs = mock_user_list.call_args_list[0]
    assert kwargs.get("trial_id") == "foo"
    assert kwargs.get("upload_type") == "bar"
    mock_blob_list.assert_called_once_with(trial_id="foo", upload_type="bar")

    assert mock_encode_and_publish.call_count == 2
    call1, call2 = mock_encode_and_publish.call_args_list
    assert call1.args[1] == GOOGLE_WORKER_TOPIC and call2.args[1] == GOOGLE_WORKER_TOPIC

    print(call1.args[0])
    assert eval(call1.args[0]) == {
        "_fn": "permissions_worker",
        "user_list": mock_user_list.return_value,
        "blob_list": mock_blob_list.return_value[:100],
    }
    assert eval(call2.args[0]) == {
        "_fn": "permissions_worker",
        "user_list": mock_user_list.return_value,
        "blob_list": mock_blob_list.return_value[100:],
    }
