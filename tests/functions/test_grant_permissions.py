import functions.grant_permissions
from functions.grant_permissions import grant_download_permissions
from functions.settings import GOOGLE_WORKER_TOPIC
import pytest
from unittest.mock import MagicMock


def test_grant_download_permissions(monkeypatch):
    # this doesn't actually matter as we get the Users right after
    mock_permissions_list = MagicMock()
    mock_permissions_list.return_value = [MagicMock(), MagicMock()]
    monkeypatch.setattr(
        functions.grant_permissions.Permissions,
        "get_for_trial_type",
        mock_permissions_list,
    )

    users = [MagicMock(), MagicMock()]
    user_emails = ["foo@bar.com", "user@test.com"]
    for user, email in zip(users, user_emails):
        user.email = email

    monkeypatch.setattr(
        functions.grant_permissions.Users, "find_by_id", lambda id, session: users.pop()
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
    mock_permissions_list.assert_called_once()  # not once_with because of unbound session
    _, kwargs = mock_permissions_list.call_args_list[0]
    assert kwargs.get("trial_id") == "foo"
    assert kwargs.get("upload_type") == "bar"
    mock_blob_list.assert_called_once_with(trial_id="foo", upload_type="bar")

    assert mock_encode_and_publish.call_count == 2
    call1, call2 = mock_encode_and_publish.call_args_list
    print(call1.args, "\n", call2.args)
    assert call1.args[1] == GOOGLE_WORKER_TOPIC and call2.args[1] == GOOGLE_WORKER_TOPIC

    print(call1.args[0])
    assert eval(call1.args[0]) == {
        "_fn": "permissions_worker",
        "user_list": user_emails[::-1],  # pop above inverts
        "blob_list": mock_blob_list.return_value[:100],
    }
    assert eval(call2.args[0]) == {
        "_fn": "permissions_worker",
        "user_list": user_emails[::-1],  # pop above inverts
        "blob_list": mock_blob_list.return_value[100:],
    }
