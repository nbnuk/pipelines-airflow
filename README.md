# pipelines-airflow

Airflow DAGs and supporting files for running pipelines on Apache Airflow
with Elastic Map Reduce.

## Installation

These scripts have been tested with Airflow (MWAA) and EMR.
To install, the following variables need to be set in Airflow > Admin > Variables.

The file [airflow-variables.json](airflow-variables.json) can be uploaded to a new Airflow instance to initialise, and then customised in airflow UI.

<img width="1029" alt="Screen Shot 2022-03-02 at 1 52 28 pm" src="https://user-images.githubusercontent.com/444897/156374747-9af8d19c-1150-4671-8204-c58ea67aa9dd.png">

## DAGS

This section describes some of the important DAGs in this project.

### [load_dataset_dag.py](dags/load_dataset_dag.py)
Steps:
* Look up the dataset in the collectory
* Retrieve the details of DwCA associated with the dataset
* Copy the DwCA to S3 for ingestion
* Determine the file size of the dataset, and either run pipelines on:
  * single node cluster for a small dataset
  * multi node cluster for a large dataset
* Run all pipelines to ingest the dataset, excluding SOLR indexing.

### [load_provider_dag.py](dags/load_provider_dag.py)
Steps:
* Look up the data provider in the collectory
* Retrieve the details of DwCA associated with the datasets
* Copy the DwCAs to S3 for all datasets for this provider ready for ingestion
* Run all pipelines to ingest the dataset, excluding SOLR indexing
This can be used to load all the datasets associated with an IPT

![load_provider](https://user-images.githubusercontent.com/444897/158418989-52229ae7-5c12-485d-b479-a26bc894d1f4.jpg)

### [ingest_small_datasets_dag.py](dags/ingest_small_datasets_dag.py)
A DAG used by the `Ingest_all_datasets` DAG to load large numbers of small datasets using a **single node cluster** in EMR.
This will not run SOLR indexing.
Includes the following options:
  * `load_images` - whether to load images for archives
  * `skip_dwca_to_verbatim` - skip the DWCA to Verbatim stage (which is expensive), and just reprocess

### [ingest_large_datasets_dag.py](dags/ingest_large_datasets_dag.py)
A DAG used by the `Ingest_all_datasets` DAG to load large numbers of large datasets using a **multi node cluster** in EMR.
This will not run SOLR indexing.
Includes the following options:
  * `load_images` - whether to load images for archives
  * `skip_dwca_to_verbatim` - skip the DWCA to Verbatim stage (which is expensive), and just reprocess

### [ingest_all_datasets_dag.py](dags/ingest_all_datasets_dag.py)
Steps:
* Retrieve a list of all available DwCAs in S3
* Run all pipelines to ingest each dataset. To do this it creates:
  * Several single node clusters for small datasets
  * Several multi-node clusters for large datasets
  * A single multi-node cluster for the largest dataset (eBird)
Includes the following options:
  * `load_images` - whether to load images for archives
  * `skip_dwca_to_verbatim` - skip the DWCA to Verbatim stage (which is expensive), and just reprocess
  * `run_index` - whether to run a complete reindex on completion of ingestion

<img width="1001" alt="Screen Shot 2022-03-16 at 12 52 42 pm" src="https://user-images.githubusercontent.com/444897/158594367-fa83b095-6a3f-4c66-95a3-7eba9e1bb270.png">


### [full_index_to_solr.py](dags/full_index_to_solr.py)
Steps:
* Run Sampling of environmental and contextual layers
* Run Jackknife environmental outlier detection
* Run Clustering 
* Run Expert Distribution outlier detection
* Run SOLR indexing for all datasets

### [solr_dataset_indexing](dags/solr_dataset_indexing.py)
Run SOLR indexing for single dataset into the live index.
This does not run the all dataset processes (Jackknife etc)

## Demo march 2022

https://user-images.githubusercontent.com/444897/158394390-33aec578-899d-4c45-9f09-5d3d81f52060.mp4




