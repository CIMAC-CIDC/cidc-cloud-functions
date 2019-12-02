"""Entrypoint for CIDC cloud functions."""

from functions import (
    ingest_upload,
    send_email,
    generate_csvs,
    store_auth0_logs,
    vis_preprocessing,
)

from flask import Flask, request, jsonify

app = Flask(__name__)

topics_to_functions = {
    "uploads": ingest_upload,
    "artifact_upload": vis_preprocessing,
    "patient_sample_update": generate_csvs,
    "daily_cron": store_auth0_logs,
    "emails": send_email,
}


@app.route("/projects/cidc-dfci-staging/topics/<topic>", methods=["POST"])
def trigger_pubsub_function(topic):
    print(f"topic {topic} received message: {request}")
    data = request.form
    print(f"with data: {data}")
    topics_to_functions[topic](data, {})
    return jsonify(success=True)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=3001)
