# !/usr/bin/bash
# Deploy a cloud function to a topic

ENTRYPOINT=$1
TOPIC=$2
ENVFILE=$3
PROJECT=$(gcloud config get-value project)

gcloud functions deploy $ENTRYPOINT \
    --runtime python37 \
    --trigger-topic $TOPIC \
    --env-vars-file $ENVFILE \
    --project $PROJECT