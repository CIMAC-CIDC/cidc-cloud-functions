"""Configuration for CIDC functions."""
import os
import json

from cidc_api.config import get_sqlalchemy_database_uri, get_secret_manager


# Cloud Functions provide the current GCP project id
# in the environment variable GCP_PROJECT. If this
# variable isn't set, then we're not running in GCP.
# See: https://cloud.google.com/functions/docs/env-var
GCP_PROJECT = os.environ.get("GCP_PROJECT")
if not GCP_PROJECT:
    from dotenv import load_dotenv

    # We're running locally, so load config from .env
    load_dotenv()

TESTING = os.environ.get("TESTING")
secrets = get_secret_manager(TESTING)

# GCP config
SQLALCHEMY_DATABASE_URI = get_sqlalchemy_database_uri(TESTING)
GOOGLE_UPLOAD_BUCKET = os.environ.get("GOOGLE_UPLOAD_BUCKET")
GOOGLE_DATA_BUCKET = os.environ.get("GOOGLE_DATA_BUCKET")
GOOGLE_LOGS_BUCKET = os.environ.get("GOOGLE_LOGS_BUCKET")
GOOGLE_ANALYSIS_GROUP_ROLE = "roles/storage.legacyObjectReader"
GOOGLE_ANALYSIS_PERMISSIONS_GROUPS_DICT = json.loads(
    os.environ.get("GOOGLE_ANALYSIS_PERMISSIONS_GROUPS_DICT", "{}")
)


# Auth0 config
AUTH0_DOMAIN = os.environ.get("AUTH0_DOMAIN")
AUTH0_CLIENT_ID = os.environ.get("AUTH0_CLIENT_ID")
AUTH0_CLIENT_SECRET = secrets.get("AUTH0_CLIENT_SECRET")

# SendGrid config
SENDGRID_API_KEY = secrets.get("SENDGRID_API_KEY")

# Check for configuration that must be defined if we're running in GCP
if GCP_PROJECT:
    assert GOOGLE_DATA_BUCKET is not None
    assert GOOGLE_UPLOAD_BUCKET is not None
