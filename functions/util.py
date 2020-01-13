"""Helpers for working with Cloud Functions."""
import base64
import datetime
from contextlib import contextmanager
from io import BytesIO, StringIO
from typing import NamedTuple, Union
from collections import namedtuple

from google.cloud import storage
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .settings import SQLALCHEMY_DATABASE_URI, FLASK_ENV, GOOGLE_DATA_BUCKET

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


def get_blob_as_stream(
    object_name: str, as_string: bool = False
) -> Union[BytesIO, StringIO]:
    """Download data from the CIDC data bucket as a byte or string stream."""
    file_bytes = _download_blob_bytes(object_name)
    if as_string:
        return StringIO(file_bytes.decode("utf-8"))
    return BytesIO(file_bytes)


def _download_blob_bytes(object_name: str) -> bytes:
    """
    Download a blob as bytes from GCS. Throws a FileNotFound exception 
    if the object doesn't exist.
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(GOOGLE_DATA_BUCKET)
    blob = bucket.get_blob(object_name)
    if not blob:
        FileNotFoundError(f"Could not find file {object_name} in {GOOGLE_DATA_BUCKET}")
    return blob.download_as_string()


def upload_to_data_bucket(object_name: str, data: Union[str, bytes]) -> storage.Blob:
    """Upload data to blob called `object_name` in the CIDC data bucket."""
    if FLASK_ENV == "development":
        fname = object_name.replace("/", "_")
        print(f"writing {fname}")
        with open(fname, "w") as f:
            f.write(data)
        return make_pseudo_blob(fname)

    client = storage.Client()
    bucket = client.get_bucket(GOOGLE_DATA_BUCKET)
    blob = bucket.blob(object_name)
    blob.upload_from_string(data)

    return blob
