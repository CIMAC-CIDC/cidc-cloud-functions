import json
import requests
from datetime import datetime
from typing import List, Optional

from google.cloud import storage
from google.api_core.exceptions import NotFound

from .settings import (
    AUTH0_CLIENT_ID,
    AUTH0_CLIENT_SECRET,
    AUTH0_DOMAIN,
    GOOGLE_LOGS_BUCKET,
)

MANAGEMENT_API = f"{AUTH0_DOMAIN}/api/v2/"
LAST_LOG_ID = "auth0/__last_log_id.txt"


def store_auth0_logs(*args):
    """
    Get new access logs from Auth0 and store them in GCS.
    """
    # Get Auth0 management API access token
    print("Fetching Auth0 management API access token")
    token = _get_auth0_access_token()

    # Get last log ID from GCS
    print("Fetching ID of most recently saved Auth0 log")
    log_id = _get_last_auth0_log_id()

    # Get new access logs
    print(f"Fetching new Auth0 logs (since log with id {log_id})")
    logs = _get_new_auth0_logs(token, log_id)

    if len(logs) == 0:
        print(f"No new logs found (since log with id {log_id})")
        return

    # Save new access logs
    print(f"Saving new Auth0 logs")
    file_name = _save_new_auth0_logs(logs)

    print(f"New Auth0 logs saved to {file_name}")


def _get_auth0_access_token() -> str:
    """Fetches an access token for the Auth0 management API."""
    payload = {
        "grant_type": "client_credentials",
        "client_id": AUTH0_CLIENT_ID,
        "client_secret": AUTH0_CLIENT_SECRET,
        "audience": MANAGEMENT_API,
    }
    res = requests.post(f"{AUTH0_DOMAIN}/oauth/token", json=payload)
    return res.json()["access_token"]


__log_bucket = None


def _get_log_bucket():
    global __log_bucket
    if __log_bucket is None:
        client = storage.Client()
        __log_bucket = client.bucket(GOOGLE_LOGS_BUCKET)
    return __log_bucket


def _get_last_auth0_log_id() -> Optional[str]:
    """Fetch that ID of the last access log imported from Auth0"""
    blob = _get_log_bucket().get_blob(LAST_LOG_ID)
    if blob:
        return blob.download_as_string().decode("utf-8")
    return None


def _get_new_auth0_logs(token: str, log_id: Optional[str]) -> List[dict]:
    """Get all new access logs since `log_id`"""
    logs_endpoint = f"{MANAGEMENT_API}logs"
    headers = {"Authorization": f"Bearer {token}"}
    # see: https://auth0.com/docs/logs#get-logs-by-checkpoint
    params = {"from": log_id} if log_id else {}

    results = requests.get(logs_endpoint, headers=headers, params=params)
    gs_path = f"gs://{GOOGLE_LOGS_BUCKET}/auth0"

    if results.status_code != 200:
        raise Exception(
            f"Failed to fetch auth0 logs, Reason: {results.reason},"
            f" Status Code: {results.status_code}, Body:\n"
            f"{results.json()}"
        )

    return results.json()


def _save_new_auth0_logs(logs: List[dict]) -> str:
    """Save a list of access log objects from Auth0"""
    log_bucket = _get_log_bucket()

    # Save new logs to a blob in GCS.
    logs_blob_name = f"{_get_logfile_name()}.json"
    logs_blob = log_bucket.blob(f"auth0/{logs_blob_name}")
    logs_blob.upload_from_string(json.dumps(logs))

    # Save the id of the most recent log in the collection.
    log_id = logs[0]["_id"]
    id_blob = log_bucket.blob(LAST_LOG_ID)
    id_blob.upload_from_string(log_id)

    return f"gs://{logs_blob.bucket.name}/{logs_blob.name}"


def _get_logfile_name(dt: str = None):
    """Generate file name with structure [year]/[month]/[day]/[timestamp].json"""
    dt = str(dt or datetime.now())
    return dt.replace(" ", "/").replace("-", "/")
