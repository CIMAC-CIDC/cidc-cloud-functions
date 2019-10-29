"""Entrypoint for CIDC cloud functions."""

from functions import ingest_upload, send_email, generate_csvs, store_auth0_logs

from flask import Flask, request

app = Flask(__name__)


@app.route("/projects/cidc-dfci-staging/topics/patient_sample_update", methods=["POST"])
def gen_csvs():
    print(f"generate_csvs got {request}")
    data = request.form
    print(f"with {data}")
    generate_csvs(data, {})
    return "200 ok"

@app.route("/projects/cidc-dfci-staging/topics/uploads", methods=["POST"])
def upload():
    print(f"ingest_upload got {request}")
    data = request.form
    print(f"with {data}")
    ingest_upload(data, {})
    return "200 ok"

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=3001)
