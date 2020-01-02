"""Helpers for working with Cloud Functions."""
import base64
import datetime
from contextlib import contextmanager
from typing import NamedTuple
from collections import namedtuple

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .settings import SQLALCHEMY_DATABASE_URI

_engine = None

_pseudo_blob = namedtuple(
    "_pseudo_blob", ["name", "size", "md5_hash", "crc32c", "time_created"]
)


def make_pseudo_blob(object_name) -> _pseudo_blob:
    return _pseudo_blob(object_name, 0, "_pseudo_md5", "_pseudo_crc32c", datetime.now())


pseudo_blob = namedtuple(
    "pseudo_blob", ["name", "size", "md5_hash", "crc32c", "time_created"]
)


def make_pseudo_blob(object_name) -> pseudo_blob:
    return pseudo_blob(object_name, 0, "_pseudo_md5", "_pseudo_crc32c", datetime.now())


@contextmanager
def sqlalchemy_session():
    """Get a SQLAlchemy session from the connection pool"""
    global _engine
    if not _engine:
        _engine = create_engine(SQLALCHEMY_DATABASE_URI)
    session = sessionmaker(bind=_engine)()

    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def extract_pubsub_data(event: dict):
    """Pull out and decode data from a pub/sub event."""
    # Pub/sub event data is base64-encoded
    b64data = event["data"]
    data = base64.b64decode(b64data).decode("utf-8")
    return data


class BackgroundContext(NamedTuple):
    """
    Model of the context object passed to a background cloud function.
    
    Based on: https://cloud.google.com/functions/docs/writing/background#function_parameters
    """

    event_id: str
    timestamp: str  # ISO 8601
    event_type: str  # e.g., "google.pubsub.topic.publish"
    resource: str
