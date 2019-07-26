"""Helpers for working with Cloud Functions."""
import base64
from typing import NamedTuple

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .settings import SQLALCHEMY_DATABASE_URI

_session = None


def get_db_session():
    """Get the current SQLAlchemy session"""
    global _session

    if not _session:
        engine = create_engine(SQLALCHEMY_DATABASE_URI)
        _session = sessionmaker(bind=engine)()

    return _session


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
