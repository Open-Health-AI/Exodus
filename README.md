# Exodus
Exodus is a JSON-configurable, modular, data migration and transformation tool running on top of Apache Spark
## System Requirements
- Java 17
- Spark 3.5.x (Built against 3.5.1)
- Scala 2.12

## Configuration
### Overview
Exodus uses a JSON configuration to define its actions. The configuration has the following structure:
```
{
    "repositories": {  
        "repositoryID": {
            "@type": "REPOSITORYTYPE",
            "setting1": "value1",
            ...
            "settingN": "valueN"
        }
        ...
        "repositoryIDN": {
            "@type": "REPOSITORYTYPE",
            "setting1": "value1",
            ...
            "settingN": "valueN"
        }
    },
    "migrations": [
        // An array of migration definitions
    ],
    "checkpointPath": "/a/filesystem/or/HDFS/path/to/store/metadata/for/checkpointing"
}
```
### Repository Types
#### JDBC (SQL)
The JDBC Repository Type allows for reading/writing data to SQL-based RDBMS via JDBC. This configuration only defines connection to the server, while individual schema+tables are defined as source/target paths within individual migration definitions
| Parameter Name | Description |
| -------------- | ----------- |
| @type **(Required)** | `JDBC` |
| jdbcURL **(Required)** | The full JDBC URL to be used to connect to the database (excluding username/password) | 
| jdbcUsername | The username to use to authenticate with the database |
| jdbcPassword | The password to use to authenticate with the database |
| maxReadConnections (default: -1) | Maximum number of simultaneous connections to the database to be used for reading data. The lower value between this and the `maxReadConnections` parameter within the migration definition will be used. -1 for unlimited | 
| maxReadConnections (default: -1) | Maximum number of simultaneous connections to the database to be used for writing data. The lower value between this and the `maxWriteConnections` parameter within the migration definition will be used. -1 for unlimited | 

#### Filesystem
The Filesystem Repository Type allows for reading/writing data to local filesystem and/or HDFS via CSV or Parquet. As each repository will only contain one "table" of data, source and target paths can be left empty in migration definitions.

| Parameter Name | Description |
| -------------- | ----------- |
| @type **(Required)** | `FILESYSTEM` or `HDFS`. Note that if you are running Exodus in cluster mode, `FILESYSTEM` is not supported |
| path **(Required)** | File (or HDFS) path to the folder containing the data to read. All contents of said folder should be CSV/Parquet files of the same format. | 
| format **(Required)** | `CSV` or `Parquet` |
| schema **(Required, `CSV` only)** | A JSON object mapping column names within the CSV (in-order) to [types](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)|
| delimiter (`CSV` only, default: `,`) | Delimiter to be used to separate columns |
| firstRowIsHeader(`CSV` only, default: false) | Whether the first row contains column names |


### Migration Definitions
Migration definitions take a structure of a JSON Object that define an action to take between one or more [data] repositories, with the following  parameters:
| Parameter Name | Description |
| -------------- | ----------- |
| identifier **(Required)** | A unique name/identifier for this migration (used for checkpoint outputs and logging) | 
| sourceRepositoryID **(Required)**  | The name of the source repository (defined under the `repositories` section of the configuration) from which data should be retrieved |
| targetRepositoryID **(Required)**  | The name of the target repository (defined under the `repositories` section of the configuration) into which data should be deposited | 
| sourcePath **(Required)** | The path within the source repository to use for the data (definitions for this may differ depending on repository type, consult individual repository type definitions) |
| targetPath **(Required)** | The path within the target repository to which data should be stored (definitions for this may differ depending on repository type, consult individual repository type definitions) |
| checkpointed (Default: false) | Whether checkpointing (incremental loading) should be used. This will change migration behaviour to only append new data seen in the source since the last run |
| checkpointColumns (Default: empty) | A JSON array of strings containing column names to use for checkpointing (that will uniquely identify a particular data row from the source) |
| checkpointingStrategy (Default: null) | One of `KEY_VALUE_COMPARE`, `MIN_VALUE`, or `MAX_VALUE`. See checkpointing for details |
| writeBatchSizeRows (Default: 10000) | Defines batch size to use when writing (e.g., Row batch size for JDBC write). How this will impact writes will vary depending on the target repository type |
| partitionColumn (Default: null) | Defines columns to use for data partitioning and parallelization. Primarily used for JDBC Read |
| maxReadConnections (default: -1) | Maximum number of simultaneous connections to the data repository to be used for reading data. The lower value between this and the `maxReadConnections` parameter within the data repository definition will be used. -1 for unlimited. | 
| maxReadConnections (default: -1) | Maximum number of simultaneous connections to the data repository to be used for writing data. The lower value between this and the `maxWriteConnections` parameter within the data repository definition will be used. -1 for unlimited. | 

