# cidc-cloud-functions

| Environment | Branch                                                                           | Status                                                                                                                                       |
| ----------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| production  | [production](https://github.com/CIMAC-CIDC/cidc-cloud-functions/tree/production) | ![continuous integration](https://github.com/CIMAC-CIDC/cidc-cloud-functions/workflows/Continuous%20Integration/badge.svg?branch=production) |
| staging     | [master](https://github.com/CIMAC-CIDC/cidc-cloud-functions)                     | ![continuous integration](https://github.com/CIMAC-CIDC/cidc-cloud-functions/workflows/Continuous%20Integration/badge.svg?branch=master)     |

Google Cloud Functions for carrying out event-driven tasks in the CIDC.

## Functions in this repo

- Pub/Sub-triggered:
  - `ingest_upload`: when a successful upload job is published to the "uploads" topic, transfers data from the upload bucket to the data bucket in GCS.
  - `vis_preprocessing`: perform and save precomputation on a given `downloadable_file` to facilitate visualization of that file's data in the CIDC Portal.
  - `derive_files_from_manifest_upload`: when a shipping/receiving manifest is ingested successfully, generate derivative files for the associated trial.
  - `derive_files_from_assay_or_analysis_upload`: when an assay or analysis upload completes, generate derivative files for the associated trial.
  - `store_auth0_logs`: pull logs for the past day from Auth0 and store them in Google Cloud Storage.
  - `send_email`: when an email is published to the "emails" topic, sends the email using the SendGrid API.
  - `disable_inactive_users`: find users who appear to have become inactive, and disable their accounts.
  - `refresh_download_permissions`: extend GCS IAM permission expiry dates for users who were active in the past day.

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

## JIRA Integration

To set-up the git hook for JIRA integration, run:

```bash
ln -s ../../.githooks/commit-msg .git/hooks/commit-msg
chmod +x .git/hooks/commit-msg
rm .git/hooks/commit-msg.sample
```

This symbolic link is necessary to correctly link files in `.githooks` to `.git/hooks`. Note that setting the `core.hooksPath` configuration variable would lead to [pre-commit failing](https://github.com/pre-commit/pre-commit/issues/1198). The `commit-msg` hook [runs after](https://git-scm.com/book/en/v2/Customizing-Git-Git-Hooks) the `pre-commit` hook, hence the two are de-coupled in this workflow.

To associate a commit with an issue, you will need to reference the JIRA Issue key (For eg 'CIDC-1111') in the corresponding commit message.

### Running locally

To start our hand-rolled local emulator:

```bash
python main.py
```

This starts up a Flask HTTP server that can simulate pubsub publish events and trigger cloud functions appropriately. E.g., to simulate publishing to the `uploads` pubsub topic:

```bash
curl http://localhost:3001/projects/cidc-dfci-staging/topics/uploads -d "data=< base64-encoded pubsub message>"
```

If you add a new cloud function, you'll need to add it to the local emulator by hand.

### Testing

To run the tests

```bash
pytest
```

### Deployment

#### CI/CD

This project uses [GitHub Actions](https://docs.github.com/en/free-pro-team@latest/actions) for continuous integration and deployment. To deploy an update to this application, follow these steps:

1. Create a new branch locally, commit updates to it, then push that branch to this repository.
2. Make a pull request from your branch into `master`. This will trigger GitHub Actions to run various tests and report back success or failure. You can't merge your PR until it passes the build, so if the build fails, you'll probably need to fix your code.
3. Once the build passes (and pending approval from collaborators reviewing the PR), merge your changes into `master`. This will trigger GitHub Actions to re-run tests on the code then deploy changes to the staging project.
4. Try out your deployed changes in the staging environment once the build completes.
5. If you're satisfied that staging should be deployed into production, make a PR from `master` into `production`.
6. Once the PR build passes, merge `master` into `production`. This will trigger GitHub Actions to deploy the changes on staging to the production project.

For more information or to update the CI workflow, check out the configuration in `.github/workflows/ci.yml`.

#### By Hand

While it's recommended that the functions in this repo be deployed automatically using GitHub Actions, you might find that you need to deploy a function by hand. To do so, checkout the [Google Cloud Functions docs](https://cloud.google.com/sdk/gcloud/reference/functions/deploy).
