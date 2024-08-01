#!/usr/bin/env bash

s3_bucket=$1

echo 'Removing old uuids'
sudo -u hadoop aws s3 rm s3://$s3_bucket/pipelines-data/ --recursive --exclude "*" --include "*/1/identifiers/*"

echo 'Uploading new uuids'
sudo -u hadoop java -jar /usr/share/aws/emr/command-runner/lib/command-runner.jar s3-dist-cp \
    --src=hdfs:///pipelines-data/ \
    --dest=s3://$s3_bucket/pipelines-data/ \
    --srcPattern='.*/1/identifiers/.*'