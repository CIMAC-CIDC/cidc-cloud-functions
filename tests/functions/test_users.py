from unittest.mock import MagicMock

from cidc_api.models.models import Users

from functions import users


def test_disable_inactive_users(monkeypatch):
    """Smoketest for the disable_inactive_users function"""
    monkeypatch.setattr("functions.util.sqlalchemy_session", MagicMock())
    UsersMock = MagicMock()
    UsersMock.disable_inactive_users = MagicMock()
    monkeypatch.setattr(users, "Users", UsersMock)

    users.disable_inactive_users()
    UsersMock.disable_inactive_users.assert_called()


def test_refresh_download_permissions(monkeypatch):
    """Smoketest for the refresh_download_permissions function"""
    user = Users(email="test@email.com")

    UsersMock = MagicMock()
    UsersMock.list.return_value = [user]
    monkeypatch.setattr(users, "Users", UsersMock)

    PermissionsMock = MagicMock()
    PermissionsMock.grant_iam_permissions = MagicMock()
    monkeypatch.setattr(users, "Permissions", PermissionsMock)

    users.refresh_download_permissions()
    assert PermissionsMock.grant_iam_permissions.call_args[0][0] == user
