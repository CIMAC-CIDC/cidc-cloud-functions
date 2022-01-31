import functions.grant_permissions
from functions.grant_permissions import grant_download_permissions, permissions_worker
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

    user_list = [MagicMock(), MagicMock()]
    user_email_list = ["foo@bar.com", "user@test.com"]
    for user, email in zip(user_list, user_email_list):
        user.email = email

    monkeypatch.setattr(
        functions.grant_permissions.Users,
        "find_by_id",
        lambda id, session: user_list.pop(),
    )

    mock_blob_name_list = MagicMock()
    # need more than 100 to test chunking
    mock_blob_name_list.return_value = [f"blob{n}" for n in range(100 + 50)]
    monkeypatch.setattr(
        functions.grant_permissions, "get_blob_names", mock_blob_name_list
    )

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
    mock_extract_data.return_value = str({"trial_id": "foo", "user_email_list": "baz"})
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
    mock_blob_name_list.assert_called_once_with(trial_id="foo", upload_type="bar")

    assert mock_encode_and_publish.call_count == 2
    call1, call2 = mock_encode_and_publish.call_args_list
    assert call1[0][1] == GOOGLE_WORKER_TOPIC and call2[0][1] == GOOGLE_WORKER_TOPIC

    assert eval(call1[0][0]) == {
        "_fn": "permissions_worker",
        "user_email_list": user_email_list[::-1],  # pop above inverts
        "blob_name_list": mock_blob_name_list.return_value[:100],
        "revoke": False,
    }
    assert eval(call2[0][0]) == {
        "_fn": "permissions_worker",
        "user_email_list": user_email_list[::-1],  # pop above inverts
        "blob_name_list": mock_blob_name_list.return_value[100:],
        "revoke": False,
    }

    # with revoke: True, passing revoke: True
    # passing user_email_list doesn't get the Permissions or users
    mock_find_user = MagicMock()
    monkeypatch.setattr(functions.grant_permissions.Users, "find_by_id", mock_find_user)
    mock_encode_and_publish.reset_mock()  # we're checking this
    mock_permissions_list.reset_mock()  # shouldn't be called

    mock_extract_data.return_value = str(
        {
            "trial_id": "foo",
            "upload_type": "bar",
            "user_email_list": user_email_list,
            "revoke": True,
        }
    )
    grant_download_permissions({}, None)

    mock_permissions_list.assert_not_called()
    mock_find_user.assert_not_called()

    assert mock_encode_and_publish.call_count == 2
    call1, call2 = mock_encode_and_publish.call_args_list
    assert call1[0][1] == GOOGLE_WORKER_TOPIC and call2[0][1] == GOOGLE_WORKER_TOPIC

    assert eval(call1[0][0]) == {
        "_fn": "permissions_worker",
        "user_email_list": user_email_list,
        "blob_name_list": mock_blob_name_list.return_value[:100],
        "revoke": True,
    }
    assert eval(call2[0][0]) == {
        "_fn": "permissions_worker",
        "user_email_list": user_email_list,
        "blob_name_list": mock_blob_name_list.return_value[100:],
        "revoke": True,
    }


def test_permissions_worker(monkeypatch):
    user_email_list = ["foo@bar.com", "user@test.com"]
    blob_name_list = [f"blob{n}" for n in range(100)]

    with pytest.raises(
        ValueError, match="user_email_list and blob_name_list must both be provided"
    ):
        permissions_worker()

    mock_grant, mock_revoke = MagicMock(), MagicMock()
    monkeypatch.setattr(
        functions.grant_permissions, "grant_download_access_to_blob_names", mock_grant
    )
    monkeypatch.setattr(
        functions.grant_permissions,
        "revoke_download_access_from_blob_names",
        mock_revoke,
    )
    permissions_worker(
        user_email_list=user_email_list, blob_name_list=blob_name_list, revoke=False
    )
    mock_grant.assert_called_with(
        user_email_list=user_email_list, blob_name_list=blob_name_list
    )
    mock_revoke.assert_not_called()

    mock_grant.reset_mock()
    permissions_worker(
        user_email_list=user_email_list, blob_name_list=blob_name_list, revoke=True
    )
    mock_grant.assert_not_called()
    mock_revoke.assert_called_with(
        user_email_list=user_email_list, blob_name_list=blob_name_list
    )
