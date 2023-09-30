# AzureSynapseExperiments
Various fun projects with Azure Synapse Suite

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

## Delta table experiments 
* The following section experiments with the merging of rows into the Delta table format. Read more <a href="./deltalake/README.md">here</a>
* This experiment deals with dealing with a delta lake format table and processing the merges coming from a Kafka topic. Spark reads the Kafka topic in batch mode (using read vs readStream) and adjusts offsets as reads progress creating in essence a micro-batching kind of application. Each batch is merged with the delta lake to keep the data current. Read more <a href="./deltalake/Delta_Lake_Streaming_Merge.md">here</a>