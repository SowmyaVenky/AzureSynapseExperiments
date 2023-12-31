## Message Streaming via KAFKA.
* This experiment will setup a single node KAFKA cluster for testing purposes via the docker-compose utility. 

<img src="../images/kafka_streaming_arch.png" />

* Make sure that docker is up and running inside the machine this is getting tested on. 
<pre>
    cd C:\Venky\DP-203\AzureSynapseExperiments\kafka_docker
    docker-compose up -d  
    docker ps 
</pre>

<img src="../images/docker-kafka.png" />

* Download the kafka tool from https://kafkatool.com/download.html
* After install, connect it to the kafka cluster running locally pointing to ports 22181 (ZK), and 29092 (bootstrap server)
* Let us start the producer to take new weather files we have downloaded and stream that to KAFKA for consumption.

<img src="../images/kafka_offset_explorer.png" />

<pre>
set JAVA_HOME=c:\Venky\jdk-11.0.15.10-hotspot
set PATH=%PATH%;c:\Venky\spark\bin;c:\Venky\apache-maven-3.8.6\bin
set SPARK_HOME=c:\Venky\spark
SET HADOOP_HOME=C:\Venky\AzureSynapseExperiments\SparkExamples

cd C:\Venky\AzureSynapseExperiments\SparkExamples
mvn clean package

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="51.508530 -0.076132 2019-01-01 2019-12-31 ../datafiles/streaming/input/2019_London_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="51.508530 -0.076132 2020-01-01 2020-12-31 ../datafiles/streaming/input/2020_London_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="51.508530 -0.076132 2021-01-01 2021-12-31 ../datafiles/streaming/input/2021_London_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="51.508530 -0.076132 2022-01-01 2022-12-31 ../datafiles/streaming/input/2022_London_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="51.508530 -0.076132 2023-01-01 2023-06-30 ../datafiles/streaming/input/2023_London_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="41.902782 12.496366 2019-01-01 2019-12-31 ../datafiles/streaming/input/2019_Rome_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="41.902782 12.496366 2020-01-01 2020-12-31 ../datafiles/streaming/input/2020_Rome_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="41.902782 12.496366 2021-01-01 2021-12-31 ../datafiles/streaming/input/2021_Rome_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="41.902782 12.496366 2022-01-01 2022-12-31 ../datafiles/streaming/input/2022_Rome_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="41.902782 12.496366 2023-01-01 2023-06-30 ../datafiles/streaming/input/2023_Rome_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="48.864716 2.349014 2019-01-01 2019-12-31 ../datafiles/streaming/input/2019_Paris_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="48.864716 2.349014 2020-01-01 2020-12-31 ../datafiles/streaming/input/2020_Paris_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="48.864716 2.349014 2021-01-01 2021-12-31 ../datafiles/streaming/input/2021_Paris_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="48.864716 2.349014 2022-01-01 2022-12-31 ../datafiles/streaming/input/2022_Paris_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="48.864716 2.349014 2023-01-01 2023-06-30 ../datafiles/streaming/input/2023_Paris_Temps.json"

spark-submit --master local[4] --class com.gssystems.spark.TemperaturesReformatterJSON target\SparkExamples-1.0-SNAPSHOT.jar file:///C:/Venky/DP-203/AzureSynapseExperiments/datafiles/streaming/input file:///C:/Venky/DP-203/AzureSynapseExperiments/datafiles/streaming/output/ file:///C:/Venky/DP-203/AzureSynapseExperiments/datafiles/streaming/location_master/


## Note - the JSON file names are created by spark and will change with each run. 

mvn exec:java -Dexec.mainClass="com.gssystems.kafka.WeatherDataStreamingProducer" -Dexec.args="C:\Venky\DP-203\AzureSynapseExperiments\datafiles\streaming\output\part-00000-dd3eed31-5521-456d-9fcd-3d66c266f6fc-c000.json C:\Venky\DP-203\AzureSynapseExperiments\datafiles\streaming\location_master\part-00000-a3a34469-0ef8-496f-be3f-826ef3d55233-c000.json"

## Read consumer
mvn exec:java -Dexec.mainClass="com.gssystems.kafka.WeatherDataStreamReceiver" 
</pre>

<img src="../images/kafka_consumer.png" />

* Now we can start a spark streaming job that can read from the KAFKA stream and do some queries on it. 

* Note the versions of the kafka clients and the streaming jars. They are sensitive and we need to update the pom xml to make them complaint.

* CHANGE PUBLIC IP OF machine if running from local to azure, else 127.0.0.1 
<pre>
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 --master local[4] --class com.gssystems.kafka.WeatherSparkStreaming target/SparkExamples-1.0-SNAPSHOT.jar 20.119.34.211 temperatures
</pre>

* As we can see the producer is slowly pushing messages to KAFKA, the structured streaming job is aggregating over the stream and computing the max and min tempertures for each latitude and longitude.

<img src="../images/kafka_stream_producer.png" />

<img src="../images/kafka_stream_aggregations.png" />

* After some time we can see the temperatures are changing (esp the max temp.)

<img src="../images/kafka_stream_aggregations_2.png" />

* Added a count field to show that the entire dataset is taken into consideration with the offset starting from the earliest as asked for in the code. 

<img src="../images/kafka_stream_aggregations_3.png" />

* Added more aggregations at the year and month level to show how the aggregates are calculated on the stream

<img src="../images/kafka_stream_aggregations_4.png" />

* After the experiment is done, bring down the kafka setup.
<img src="../images/shutting_down.png" />

* Now we will try to pull from the kafka stream and push that to a folder in local machine to write in delta format. 

* CHANGE IP ADDRESS TO THE RIGHT PUBLIC IP OR LOCAL ADDRESS 127.0.0 1
* Make sure we are running the Spark 3.3.3 and the delta core version 2.2.0 is used. Also make sure the scale 2.12 versions of spark and libraries are selected. Otherwise you will get weird class not found errors...

<pre>
## This will download in delta format.
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:2.2.0 --conf spark.sql.streaming.checkpointLocation=file:///C:\Venky\spark_checkpoints\ --master local[4] --class com.gssystems.kafka.KafkaStreamToDeltaLakeDownloader target/SparkExamples-1.0-SNAPSHOT.jar 127.0.0.1 temperatures file:///C:\Venky\DP-203\AzureSynapseExperiments\datafiles\temperatures_delta

## This will download as a parquet file.
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 --conf spark.sql.streaming.checkpointLocation=file:///C:\Venky\spark_checkpoints\ --master local[4] --class com.gssystems.kafka.KafkaStreamToParquetDownloader target/SparkExamples-1.0-SNAPSHOT.jar 127.0.0.1 temperatures file:///C:\Venky\DP-203\AzureSynapseExperiments\datafiles\temperatures_parquet

## This will download as a json file, for easier debugging.

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 --conf spark.sql.streaming.checkpointLocation=file:///C:\Venky\spark_checkpoints\ --master local[4] --class com.gssystems.kafka.KafkaStreamToJSONDownloader target/SparkExamples-1.0-SNAPSHOT.jar 127.0.0.1 temperatures file:///C:\Venky\DP-203\AzureSynapseExperiments\datafiles\temperatures_json
</pre>

* For some reason running this on spark 3.4.1 causes a weird failure. Needs further research.

<pre>
Exception in thread "main" java.lang.NoSuchMethodError: 'org.apache.spark.internal.config.ConfigEntry org.apache.spark.sql.internal.SQLConf$.PARQUET_FIELD_ID_READ_ENABLED()'
</pre>

* I changed the format to write as parquet format. Also set the trigger to write files every 2 minutes. Once I do this, I am able to unload the data that is in KAFKA into the file system. 

<img src="../images/kafka_to_parquet_format.png" />
