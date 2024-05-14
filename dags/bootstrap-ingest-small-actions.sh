#!/bin/bash
set -x
export S3_BUCKET=$1
echo "S3 bucket to use: $S3_BUCKET"

sudo pip3 install -U FrictionlessDarwinCore

# set up name service
#sudo yum install -y docker
#sudo systemctl start docker
#sudo docker run --detach -p 9179:9179 -t djtfmartin/ala-namematching-service:v1.8-v20210811-3
#sudo docker run --detach -p 9189:9189 -t djtfmartin/ala-sensitive-data-service:v20200214-4-multiarch

# create directories
sudo mkdir /mnt/pipelines-data
sudo ln -s /mnt/pipelines-data /data

sudo mkdir -p /data/la-pipelines/config
sudo mkdir -p /data/biocache-load
sudo mkdir -p /data/pipelines-shp
sudo mkdir -p /data/pipelines-vocabularies

# Add output directories - non HDFS
sudo mkdir -p /data/pipelines-all-datasets/
sudo mkdir -p /data/pipelines-data
sudo mkdir -p /data/pipelines-species

sudo mkdir -p /data/dwca-tmp/
sudo chmod -R 777 /data/dwca-tmp/
sudo mkdir -p /data/spark-tmp

sudo chown hadoop:hadoop -R /data/*

# get SHP files
sudo aws s3 cp s3://$S3_BUCKET/pipelines-shp/ /data/pipelines-shp/ --recursive --include "*"
sudo aws s3 cp s3://$S3_BUCKET/pipelines-vocabularies/  /data/pipelines-vocabularies/ --recursive --include "*"

# config files and JAR
sudo aws s3 cp s3://$S3_BUCKET/logback.xml  /data/la-pipelines/config
sudo aws s3 cp s3://$S3_BUCKET/log4j.properties  /data/la-pipelines/config
sudo aws s3 cp s3://$S3_BUCKET/la-pipelines.yaml  /data/la-pipelines/config
sudo aws s3 cp s3://$S3_BUCKET/la-pipelines-emr-local.yaml  /data/la-pipelines/config/la-pipelines-local.yaml
sudo aws s3 cp s3://$S3_BUCKET/la-pipelines.jar  /usr/share/la-pipelines/la-pipelines.jar

# set up la-pipeline script
sudo wget https://github.com/mikefarah/yq/releases/download/v4.16.1/yq_linux_arm64.tar.gz -O - | tar xz
sudo mv yq_linux_arm64 /usr/bin/yq
sudo curl -o /usr/bin/docopts -LJO https://github.com/docopt/docopts/releases/download/v0.6.3-rc2/docopts_linux_arm
sudo chmod +x /usr/bin/docopts
sudo aws s3 cp s3://$S3_BUCKET/logging_lib.sh /usr/share/la-pipelines/logging_lib.sh
sudo aws s3 cp s3://$S3_BUCKET/la-pipelines /usr/bin/la-pipelines
sudo chmod -R 777 /usr/bin/la-pipelines
sudo chmod -R 777 /usr/share/la-pipelines/logging_lib.sh

# Download / Upload scripts
sudo aws s3 cp s3://$S3_BUCKET/airflow/dags/download-datasets.sh  /tmp/download-datasets.sh
sudo chmod -R 777 /tmp/download-datasets.sh
sudo aws s3 cp s3://$S3_BUCKET/airflow/dags/download-datasets-avro.sh  /tmp/download-datasets-avro.sh
sudo chmod -R 777 /tmp/download-datasets-avro.sh
sudo aws s3 cp s3://$S3_BUCKET/airflow/dags/upload-datasets.sh  /tmp/upload-datasets.sh
sudo chmod -R 777 /tmp/upload-datasets.sh
sudo aws s3 cp s3://$S3_BUCKET/airflow/dags/upload-export.sh  /tmp/upload-export.sh
sudo chmod -R 777 /tmp/upload-export.sh
sudo aws s3 cp s3://$S3_BUCKET/airflow/dags/frictionless.sh  /tmp/frictionless.sh
sudo chmod -R 777 /tmp/frictionless.sh