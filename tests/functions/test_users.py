from unittest.mock import MagicMock

from functions import users


def test_disable_inactive_users(monkeypatch):
    """Smoketest for the disable_inactive users function"""
    monkeypatch.setattr("functions.util.sqlalchemy_session", MagicMock())
    Users = MagicMock()
    Users.disable_inactive_users = MagicMock()
    monkeypatch.setattr(users, "Users", Users)

    users.disable_inactive_users()
    Users.disable_inactive_users.assert_called()
