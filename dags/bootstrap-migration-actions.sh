#!/bin/bash
set -x
export S3_BUCKET=$1
echo 'S3 bucket to use: $S3_BUCKET'

# create directories
sudo mkdir -p /data/la-pipelines/config
sudo chown hadoop:hadoop -R /data/la-pipelines/*

# config files and JAR
sudo aws s3 cp s3://$S3_BUCKET/logback.xml  /data/la-pipelines/config
sudo aws s3 cp s3://$S3_BUCKET/log4j.properties  /data/la-pipelines/config
sudo aws s3 cp s3://$S3_BUCKET/la-pipelines-emr.yaml  /data/la-pipelines/config/la-pipelines-local.yaml
sudo aws s3 cp s3://$S3_BUCKET/la-pipelines.yaml  /data/la-pipelines/config
sudo aws s3 cp s3://$S3_BUCKET/la-pipelines.jar  /usr/share/la-pipelines/la-pipelines.jar

# set up migration script
sudo aws s3 cp s3://$S3_BUCKET/migration/migration.jar  /usr/share/la-pipelines/migration.jar
sudo aws s3 cp s3://$S3_BUCKET/migration/migrate-uuids-cluster.sh  /usr/bin/migrate-uuids-emr.sh
sudo aws s3 cp s3://$S3_BUCKET/airflow/dags/s3-upload-migrated-uuids.sh /usr/bin/s3-upload-migrated-uuids.sh
sudo chmod -R 777 /usr/bin/migrate-uuids-emr.sh
sudo chmod -R 777 /usr/bin/s3-upload-migrated-uuids.sh

# set up la-pipeline script
sudo wget https://github.com/mikefarah/yq/releases/download/v4.16.1/yq_linux_arm64.tar.gz -O - | tar xz
sudo mv yq_linux_arm64 /usr/bin/yq
sudo curl -o /usr/bin/docopts -LJO https://github.com/docopt/docopts/releases/download/v0.6.3-rc2/docopts_linux_arm
sudo chmod +x /usr/bin/docopts
sudo aws s3 cp s3://$S3_BUCKET/logging_lib.sh /usr/share/la-pipelines/logging_lib.sh
sudo aws s3 cp s3://$S3_BUCKET/la-pipelines /usr/bin/la-pipelines
sudo chmod -R 777 /usr/bin/la-pipelines
sudo chmod -R 777 /usr/share/la-pipelines/logging_lib.sh