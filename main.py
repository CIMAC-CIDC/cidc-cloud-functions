"""Entrypoint for CIDC cloud functions."""

from functions import ingest_upload, send_email, generate_csvs, store_auth0_logs

from flask import Flask, request

app = Flask(__name__)


@app.route("/projects/cidc-dfci-staging/topics/patient_sample_update", methods=["POST"])
def gen_csvs():
    print(f"Got {request}")
    data = request.form
    print(f"with {data}")
    generate_csvs(data, {})
    return "200 ok"
