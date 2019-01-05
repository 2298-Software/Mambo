# Turbine

Turbine is a configuration-driven framework for Apache Spark that makes it easy to develop Spark-based data processing pipelines.

Turbine is simply a pre-made Spark application that implements many of the tasks commonly found in ETL pipelines. In many cases, Turbine allows large pipelines to be developed on Spark with no coding required. When custom code is needed, there are pluggable points in Turbine for core functionality to be extended. Turbine works in batch and streaming modes.

Some examples of what you can easily do with Turbine:
- Run a graph of Spark SQL queries, all in the memory of a single Spark job
- Stream in event data from Apache Kafka, join to reference data, and write to Apache Kudu
- Read in from an RDBMS table and write to Apache Parquet files on HDFS
- Automatically merge into slowly changing dimensions (Type 1 and 2, and bi-temporal)
- Insert custom DataFrame transformation logic for executing complex business rules

## Available Components

### Generate
    - Dataset - generate a dataset that is useful testing
### Ingest
    - ImportFile - Import a text file into an in-memory dataset
    - ImportCsvFile - Import a csv file into an in-memory dataset
    - ImportXlsFile - Import a xls file into an in-memory dataset
### Process
    - ExecuteSql - Execute a sql command against an in-memory dataset
### Distribute
    - SaveFile - Save an in-memory dataset to file (csv, json, parquet, avro)
  
## Get started

### Requirements

Turbine can execute on any Spark cluster with:

### Compiling Turbine

You can build the Turbine application from the top-level directory of the source code by running the Maven command:

    mvn clean package

This will create `turbine-0.4.0.jar` in the target directory.

### Finding examples

Turbine provides three example pipelines that you can run for yourself:

TODO

### Running Turbine

You can run Turbine by submitting it to Spark with the configuration file for your pipeline:

    spark2-submit turbine-0.4.0.jar yourpipeline.conf

