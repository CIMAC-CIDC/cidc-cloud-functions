from unittest.mock import MagicMock

import pytest

from tests.util import make_pubsub_event
from functions import util


def test_extract_pubsub_data():
    """Ensure that extract_pubsub_data can do what it claims"""
    data = "hello there"
    event = make_pubsub_event(data)
    assert util.extract_pubsub_data(event) == data


def test_sqlalchemy_session(monkeypatch):
    """Check that the sqlalchemy context manager creates/closes a session on enter/exit"""
    # Simulate function startup, when no SQLAlchemy _engine
    # has yet been initialized.
    util._engine = None

    engine = MagicMock()
    create_engine = MagicMock()
    create_engine.return_value = engine
    session = MagicMock()
    session_creator = MagicMock()
    session_creator.return_value = session
    sessionmaker = MagicMock()
    sessionmaker.return_value = session_creator

    monkeypatch.setattr(util, "create_engine", create_engine)
    monkeypatch.setattr(util, "sessionmaker", sessionmaker)

    # On first invocation, we expect a global engine to be created
    # and a session to be made.
    with util.sqlalchemy_session() as sesh:
        create_engine.assert_called_once()
        sessionmaker.assert_called_once_with(bind=engine)
        assert sesh == session

    session.commit.assert_called_once()
    session.rollback.assert_not_called()
    session.close.assert_called_once()

    create_engine.reset_mock()
    sessionmaker.reset_mock()

    with pytest.raises(Exception), util.sqlalchemy_session() as sesh:
        # On subsequent invocations, the already-created engine to be used.
        create_engine.assert_not_called()
        sessionmaker.assert_called_once_with(bind=engine)
        # Force a failure
        raise Exception

    session.commit.assert_not_called()
    session.rollback.assert_called_once()
    session.close.assert_called_once()
