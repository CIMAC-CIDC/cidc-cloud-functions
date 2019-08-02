# cidc-cloud-functions

| Environment | Branch                                                                           | Status                                                                                                                                                |
| ----------- | -------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| production  | [production](https://github.com/CIMAC-CIDC/cidc-cloud-functions/tree/production) | [![Build Status](https://travis-ci.org/CIMAC-CIDC/cidc-cloud-functions.svg?branch=production)](https://travis-ci.org/CIMAC-CIDC/cidc-cloud-functions) |
| staging     | [master](https://github.com/CIMAC-CIDC/cidc-cloud-functions)                     | [![Build Status](https://travis-ci.org/CIMAC-CIDC/cidc-cloud-functions.svg?branch=master)](https://travis-ci.org/CIMAC-CIDC/cidc-cloud-functions)     |


Google Cloud Functions for carrying out event-driven tasks in the CIDC.

## Functions in this repo

* Pub/Sub-triggered:
  * `ingest_upload`: when a successful upload job is published to the "uploads" topic, transfers data from the upload bucket to the data bucket in GCS.

## Development

### Setup
To install dependencies:
```bash
pip install -r requirements.txt -r requirements.dev.txt
```
GCP doesn't yet provide a Cloud Functions local emulator for Python, so there isn't a way to run these function as part of a local instance of the system. As such, writing/running tests for functions based on the [Cloud Functions docs](https://cloud.google.com/functions/docs/) is our best bet for experimenting with the functions locally. To run the tests:
```bash
pytest
```

### Deployment
While it's recommended that the functions in this repo be deployed using the Travis CI pipeline, you might find that you need to deploy a function by hand. The `deploy/` directtory contains a set of convenience scripts to help you do so.

To deploy a pub/sub-triggered function, run:

```bash
gcloud config set project $PROJECT
bash deploy/pubsub.sh $ENTRYPOINT $TOPIC $ENVFILE
```
where `$PROJECT` is the project you want to deploy to, `$ENTRYPOINT` is the name of the function in your code, `$TOPIC` is the pub/sub topic this function subscribes to, and `$ENVFILE` is the path to the local file containing environment variable configurations for this function.

As functions with other types of triggers are added (e.g., HTTP or Cloud SQL), other convenience scripts will be added too, but if you'd rather use `gcloud` directly, `gcloud functions deploy --help` is a great place to start.
```
