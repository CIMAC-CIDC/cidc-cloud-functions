"""Configuration for CIDC functions."""
import os

from cidc_api.config import get_sqlalchemy_database_uri

# Cloud Functions provide the current GCP project id
# in the environment variable GCP_PROJECT.
# See: https://cloud.google.com/functions/docs/env-var
GOOGLE_CLOUD_PROJECT = os.environ.get("GCP_PROJECT")

if not GOOGLE_CLOUD_PROJECT:
    from dotenv import load_dotenv

    # We're running locally, so load config from .env
    load_dotenv()
    GOOGLE_CLOUD_PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT")

TESTING = os.environ.get("TESTING")
SQLALCHEMY_DATABASE_URI = get_sqlalchemy_database_uri(TESTING)
GOOGLE_UPLOAD_BUCKET = os.environ.get("GOOGLE_UPLOAD_BUCKET")
GOOGLE_UPLOAD_TOPIC = os.environ.get("GOOGLE_UPLOAD_TOPIC")
