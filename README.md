# AzureSynapseExperiments
Various fun projects with Azure Synapse Suite

## Multi Layer Data Lake  
* This experiment demonstrates an end-to-end use-case of a data lake / data warehouse. Various layers get data from sources, land it, and process it in both batch and real-time based pipelines and demonstrate the use of both dedicated and serverless Synapse pools to satisfy a variety of use-cases. Read more about this <a href="./dedicated_pool/README.md">here</a>

* This experiment will take a movielens dataset from Kaggle and shred the data to fit into a relational model using Spark. After that the data is imported into ADLS, and analyzed using Synapse serverless and dedicated pools. Read more about this <a href="./dedicated_pool/Movie_Lens_Processing.md">here</a>

* Once data is inside the dedicated pool, the columns in the table can be masked using various patterns. The users who do not have admin rights will see masked data, while the ones that have admin will see the regular data. We can also grant unmask on columns for various roles to allow for mask/unmask based entitilements. This experiment also shows how we can use Submit-AzSynapseSparkJob to allow us to submit the job from my local computer to Livy on the spark pool. Read more <a href="./dedicated_pool/Dynamic_Data_Masking.md">here</a>

## Analyze JSON data that shows temperatures at Raleigh airport.
* A simple use-case that shows how to take a parquet file from ADLS and expose it as an external table inside Synapse Serverless. Then we use Synapse spark pool to take this parquet file and convert it into a delta format. Please see steps <a href="./Analyze_Raleign_Airport_Temps.md">here</a>

## Weather data by Lat and Long
* For a more exhaustive use-case where we download weather data for multiple years and create tables in Synapse Serverless Pools, please refer to the readme under <a href="./SparkExamples/">SparkExamples</a>

## PII/PCI Fun test case. 

*  For a fun use-case where we can test how to scramble PII/PCI data and control access via entitlements, refer to <a href="./SparkExamples/PII_PCI_Data_Hash_Testing.md">this</a>

## Stream new temperature data to Kafka running locally in docker.  

*  We now switch focus to using the temperature dataset from London, and stream that to KAFKA. Once it is in KAFKA we can take different routes, refer to <a href="./kafka_docker/Readme.md">this</a>

## Configure Kafka on Ubuntu VM and test with various integration tools.  

*  We will setup the Ubuntu VM inside Azure. We will then setup Kafka and use it as a base for various experiments. Read more about <a href="./kafka_docker/Kafka_on_Azure_VM.md">this</a>


## Configure Synapse Spark pools and configure jobs for KAFKA structured streaming.  

*  We will setup the Ubuntu VM inside Azure. We will then setup Kafka and connect to it from within Synapse to stream data and download it to ADLS. Read more about <a href="./kafka_docker/Kafka_From_Synapse.md">this</a>

## Use Azure Event Hub to stream the data from local, and consume via Spark Structured Streaming.

* We will create an Azure Event Hub with STANDARD SKU. Then we will modify the code to produce the temperature messages to the event hub. Then we will run the JSON based downloader and the Streaming analytics from Event Hub. Read more about this <a href="./azure-eh/README.md">here</a>

## Synase Azure Data Explorer Pool and Event Hub based integration.

* This experiment will provision an event hub, and connect it to ingest into a data explorer pool. Once the data is streamed into the Azure Event Hub, it will be available for analysis via the Kusto Query Language inside ADX. Read more about this <a href="./azure-eh/Azure_Event_Hub_ADX.md">here</a>

## Synapse Azure Data Explorer Pool and ADLS continuous integration. 

* This experiment will provision an ADX cluster and create storage accounts to simulate a set of datalake that could be region bound and upload raw temperature files. We will do Synapse spark pool based ETL, and setup ADX to ingest the data. Read more about this <a href="./azure-eh/ADX_With_ADLS_Integration.md">here</a>

## Flink processing testing. 
* This experiment will show how to use Flink to process the weather data and reformat it. Flink can call the API and pull date based on the date ranges for all the cities. We explore the regular Flink Java API and code a regular map-reduce kind of application while comparing its similarties with a Spark program. Then we use the simplified Table API that can make various sources look like a regular SQL table, and interact with it with SQL queries. Kafka topics, regular file systems can be exposed as tables with this technique. Read more about this <a href="./flink/Flink-Experiment.md">here</a>

## Azure Streaming Analytics testing.
* Azure streaming analytics is a product that can make writing streaming analytics jobs a breeze. No need to know how to code with Flink/Spark etc. and a no-code editor does most of the code generation. Read more about testing it <a href="./ASA/README.md">here</a>

* ASA can stream the data from the event hub directly to a SQL Server or Postgres DB. This might be useful in some situations where we want to push domain based events via the event hub that need to be consumed in the destination database for other uses. Read more about this <a href="./ASA/ASA_SQL_Server_Postgres.md">here</a>

* ASA can benefit by having a timestamp column that can be used with the TIMESTAMP BY clause and applied to window functions. This section will take the base parquet files we had, add a date column to it, and push that data to Azure Event Hub using a Spark program. Once that data reaches the event hub, it is consumed by ASA and pushed to blob storage. Read more about this <a href="./ASA/ASA_Window_Functions.md">here</a>

* ASA can take the data from the Azure Event Hubs, and push that data directly over to a table INSIDE A Synapse DEDICATED POOL. Read more about the process <a href="./ASA/ASA_Synapse_Dedicated_Pool.md">here</a>

## Delta table experiments 
* The following section experiments with the merging of rows into the Delta table format. Read more <a href="./deltalake/README.md">here</a>

* This experiment deals with dealing with a delta lake format table and processing the merges coming from a Kafka topic. Spark reads the Kafka topic in batch mode (using read vs readStream) and adjusts offsets as reads progress creating in essence a micro-batching kind of application. Each batch is merged with the delta lake to keep the data current. Read more <a href="./deltalake/Delta_Lake_Streaming_Merge.md">here</a>

* The same technique of reading from KAFKA to create a dataframe, and merging to the data inside a SQL Server table might be good to use in cases where high performance is required, and a database layer is already being used. The merges on the SQLServer felt a little more snappy than the merges on the delta lake. Read more about this <a href="./SqlServerSpark/Readme.md">here</a>