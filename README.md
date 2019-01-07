# MamboFlow

MamboFlow is a configuration-driven framework for Apache Spark that makes it easy to develop Spark-based data processing pipelines.

MamboFlow is simply a pre-made Spark application that implements many of the tasks commonly found in ETL pipelines. In many cases, MamboFlow allows large pipelines to be developed on Spark with no coding required. When custom code is needed, there are pluggable points in MamboFlow for core functionality to be extended. MamboFlow works in batch and streaming modes.

Some examples of what you can easily do with MamboFlow:
- Run a graph of Spark SQL queries, all in the memory of a single Spark job
- Stream in event data from Apache Kafka, join to reference data, and write to Apache Kudu
- Read in from an RDBMS table and write to Apache Parquet files on HDFS
- Automatically merge into slowly changing dimensions (Type 1 and 2, and bi-temporal)
- Insert custom DataFrame transformation logic for executing complex business rules

## Available Components

### Generate
    - Dataset - generate a dataset that is useful testing
### Ingest
Supports local and remote files based on the specified fs (http://, file://, hdfs://)

    - ImportFile - Import a text file into an in-memory dataset
    - ImportCsvFile - Import a csv file into an in-memory dataset
    - ImportXlsFile - Import a xls file into an in-memory dataset
    - ImportRdbmsTable - Import a table/query from RDBMS into an in-memory dataset
### Process
    - ExecuteSql - Execute a sql command against an in-memory dataset
### Distribute
    - SaveFile - Save an in-memory dataset to file (csv, json, parquet, avro)
  
## Get started

### Compiling MamboFlow

You can build the MamboFlow application from the top-level directory of the source code by running the Maven command:

    mvn clean package

This will create `mamboflow-0.1.0.jar` in the target directory.

### Finding examples

TlMamboFlow provides example pipelines that you can run for yourself:

- [Ingest Local Excel File](examples/file-ingest-local-xls.conf): Example that reads a local XLS file, adds a timestamp column and saves as a json file.
- [Ingest Remote CSV File](examples/file-ingest-remote-csv.conf): Example that reads remote (HTTP) csv file, aggregates the data and then saves as a json file.
- [Generate Data](examples/generate-data.conf): Example that generates test data, adds a column and saves as a json file.
- [RDBMS Ingest](examples/rdbms-ingest.conf): Example that reads an rdbms table, adds a timestamp column and saves as a json file.

### Running MamboFlow

You can run MamboFlow by submitting it to Spark with the configuration file for your pipeline:

    spark-submit mamboflow-0.1.0.jar yourpipeline.conf

