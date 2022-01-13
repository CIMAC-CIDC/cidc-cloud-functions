"""Entrypoint for CIDC cloud functions."""

from functions import (
    ingest_upload,
    send_email,
    store_auth0_logs,
    vis_preprocessing,
    derive_files_from_manifest_upload,
    derive_files_from_assay_or_analysis_upload,
    disable_inactive_users,
    refresh_download_permissions,
    update_cidc_from_csms,
    grant_download_permissions,
)

from flask import Flask, request, jsonify

app = Flask(__name__)

topics_to_functions = {
    "uploads": ingest_upload,
    "artifact_upload": vis_preprocessing,
    "patient_sample_update": derive_files_from_manifest_upload,
    "assay_or_analysis_upload": derive_files_from_assay_or_analysis_upload,
    "csms_trigger": update_cidc_from_csms,
    "grant_download_perms": grant_download_permissions,
    "daily_cron": [
        store_auth0_logs,
        disable_inactive_users,
        refresh_download_permissions,
    ],
    "emails": send_email,
}


@app.route("/projects/cidc-dfci-staging/topics/<topic>", methods=["POST"])
def trigger_pubsub_function(topic):
    print(f"topic {topic} received message: {request}")
    data = request.form
    print(f"with data: {data}")
    func = topics_to_functions[topic]
    if isinstance(func, list):
        for f in func:
            f(data, {})
    else:
        func(data, {})
    return jsonify(success=True)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=3001)
