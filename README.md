# cidc-cloud-functions

| Environment | Branch                                                                           | Status                                                                                                                                                |
| ----------- | -------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| production  | [production](https://github.com/CIMAC-CIDC/cidc-cloud-functions/tree/production) | [![Build Status](https://travis-ci.org/CIMAC-CIDC/cidc-cloud-functions.svg?branch=production)](https://travis-ci.org/CIMAC-CIDC/cidc-cloud-functions) |
| staging     | [master](https://github.com/CIMAC-CIDC/cidc-cloud-functions)                     | [![Build Status](https://travis-ci.org/CIMAC-CIDC/cidc-cloud-functions.svg?branch=master)](https://travis-ci.org/CIMAC-CIDC/cidc-cloud-functions)     |

Google Cloud Functions for carrying out event-driven tasks in the CIDC.

## Functions in this repo

- Pub/Sub-triggered:
  - `ingest_upload`: when a successful upload job is published to the "uploads" topic, transfers data from the upload bucket to the data bucket in GCS.
  - `generate_csvs`: when a shipping/receiving manifest is ingested successfully, generate new participant/sample CSVs from the updated trial metadata.
  - `store_auth0_logs`: pull logs for the past day from Auth0 and store them in Google Cloud Storage.
  - `send_email`: when an email is published to the "emails" topic, sends the email using the SendGrid API.

## Development

### Setup

To install dependencies:

```bash
pip install -r requirements.dev.txt
```

To install and configure pre-commit hooks>

```bash
pre-commit install
```

### Testing

GCP doesn't yet provide a Cloud Functions local emulator for Python, so there isn't a way to run these function as part of a local instance of the system. As such, writing/running tests for functions based on the [Cloud Functions docs](https://cloud.google.com/functions/docs/) is our best bet for experimenting with the functions locally. To run the tests:

```bash
pytest
```

### Deployment

#### CI/CD

This project uses [Travis CI](https://travis-ci.org/) for continuous integration and deployment. To deploy an update to this application, follow these steps:

1. Create a new branch locally, commit updates to it, then push that branch to this repository.
2. Make a pull request from your branch into `master`. This will trigger Travis to run various tests and report back success or failure. You can't merge your PR until it passes the Travis build, so if the build fails, you'll probably need to fix your code.
3. Once the Travis build passes (and pending approval from collaborators reviewing the PR), merge your changes into `master`. This will trigger Travis to re-run tests on the code then deploy changes to the staging project.
4. Try out your deployed changes on the staging API once the Travis build completes.
5. If you're satisfied that staging should be deployed into production, make a PR from `master` into `production`.
6. Once the PR build passes, merge `master` into `production`. This will trigger Travis to deploy the changes on staging to the production project.

For more information or to update the Travis pipeline, check out the configuration in `.travis.yml`.

#### By Hand

While it's recommended that the functions in this repo be deployed using the Travis CI pipeline, you might find that you need to deploy a function by hand. The `deploy/` directtory contains a set of convenience scripts to help you do so.

To deploy a pub/sub-triggered function, run:

```bash
gcloud config set project $PROJECT
bash deploy/pubsub.sh $ENTRYPOINT $TOPIC $ENVFILE
```

where `$PROJECT` is the project you want to deploy to, `$ENTRYPOINT` is the name of the function in your code, `$TOPIC` is the pub/sub topic this function subscribes to, and `$ENVFILE` is the path to the local file containing environment variable configurations for this function.

As functions with other types of triggers are added (e.g., HTTP or Cloud SQL), other convenience scripts will be added too, but if you'd rather use `gcloud` directly, `gcloud functions deploy --help` is a great place to start.
