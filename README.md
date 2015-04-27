#Hive Storage Handler for Kinesis#

The **Hive Storage Handler For Kinesis** helps users read from and write to Kinesis Streams using Hive, enabling them to run queries to analyze data that resides in Kinesis.

##Building from Source##
* Download the code from Github:
  ```
    $ git clone https://github.com/akhiluanandh/kinesis-storage-handler.git
    $ cd kinesis-storage-handler
  ```

* Build using Maven (add ```-DskipTests``` to build without running tests):

  ```
    $ mvn clean install -Phadoop-1
  ```

* The JARs for the storage handler can be found in the ```target/``` folder. Use ```qubole-hive-kinesis-0.0.4-jar-with-dependencies.jar``` in the hive session (see below).

##Usage##
* Add the JAR to the Hive session. ```<path-to-jar>``` is the path to the above mentioned JAR. For using this with Qubole hive, upload the JAR to an S3 bucket and provide its path.
  
  ``` ADD JAR <path-to-jar>;```

* Each record in the kinesis stream corresponds to a row in the Hive table. The data in kinesis can be stored in any format, and the corresponding SerDe needs to be specified while creating the table (the default SerDe is org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe).

* While creating the Hive table, use 
  
  ```STORED BY 'com.qubole.hive.kinesis.HiveKinesisStorageHandler'```.
  
  In TBLPROPERTIES, ```'kinesis.stream.name'``` should be set to the name of the kinesis stream.

* For example, the following query would create a Hive table called 'TransactionLogs' that reads from a kinesis stream called 'TransactionStream', with the fields transactionId (int), username (string), amount (int), with rows in JSON format.
  
  ```
    DROP TABLE TransactionLogs;

    CREATE TABLE TransactionLogs (
      transactionId INT,
      username STRING,
      amount INT
    )
    ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
    STORED BY 'com.qubole.hive.kinesis.HiveKinesisStorageHandler'
    TBLPROPERTIES ('kinesis.stream.name'='TransactionStream');
  ```
* Other options can be added to table properties, or set via the SET command. See [Configuration Settings](#conf) for details.

####Checkpoints####
* Checkpoints enable users to perform incremental queries, i.e, queries on the data that has been added to the stream after the last query.

* To enable checkpointing, run this command in the hive session:  
  ``` SET kinesis.checkpoint.enabled = true; ```

* Checkpoints are implemented using Amazon DynamoDB. Make sure that your AWS keys have access to DynamoDB. Specify the DynamoDB table name:  
  ``` SET kinesis.checkpoint.metastore.table.name = <table-name>;```  
  The DynamoDB table specified must have a hash key attribute named "leaseKey". If the table does not exist, it is created automatically with a read capacity of 50 and write capacity of 10. To alter the read/write capacity, use ```SET kinesis.checkpoint.dynamo.read.capacity``` and ```SET kinesis.checkpoint.dynamo.write.capacity```.

* Set a logical name and iteration number (default 0) for the query:  
  ``` 
    SET kinesis.checkpoint.logical.name = <logical-name>;  
    SET kinesis.checkpoint.iteration.no = <iteration-no>;
  ```  

* Checkpoints are maintained for each shard. The checkpoint value is the last sequence number read from that shard.

* When a query is performed with iteration number n, it looks for the checkpoint written by a query with the same logical name and checkpoint number n - 1 (if n is not 0). If this checkpoint is found, only the records that came after this checkpoint (sequence number) are read from the shard. Otherwise, the entire stream is read.

* If a query fails, re-run with the same iteration number.

## <a name="conf" />Configuration Settings ##
| Setting                      | Description                                       |  Default                           |
| ---------------------------- | ------------------------------------------------- | ---------------------------------- |
| kinesis.stream.name          | Name of the kinesis stream                        | None                               |
| kinesis.batch.size           | Number of records fetched at a time               | 1000                               |
| kinesis.iteration.timeout    | TIme in minutes after which iteration is stopped  | 15                                 |
| kinesis.accessKey            | AWS Access key to access kinesis                  | S3 credentials in the cluster      |
| kinesis.secretKey            | AWS Secret key to access kinesis                  | S3 credentials in the cluster      |
| kinesis.endpoint.region      | Endpoint region of Kinesis                        | us-east-1                          |
| kinesis.retry.maxattempts    | Maximum number of retries while reading from kinesis | 3                               |
| kinesis.retry.interval       | Interval (in milliseconds) between retries        | 1000                               |
| kinesis.checkpoint.enabled   | Enable checkpointing                              | false                              |
| kinesis.checkpoint.metastore.table.name | Name of the DynamoDB table where checkpoints are written | None             |
| kinesis.checkpoint.logical.name | Logical name for the query                     | None                               |
| kinesis.checkpoint.iteration.no | Checkpoint iteration number                    | 0                                  |
| kinesis.checkpoint.interval  | Interval (in milliseconds) after which checkpoint is written | 60000 (1 minute)        |
| kinesis.checkpoint.dynamo.read.capacity | Read capacity of DynamoDB table (used when the specified table does not exist and is created) | 50 |
| kinesis.checkpoint.dynamo.write.capacity | Write capacity of DynamoDB table (used when the specified table does not exist and is created) | 10 |

