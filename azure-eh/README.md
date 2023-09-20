## Data streaming with Azure Event Hubs. 

* This series of experiments deal with creating an Azure event hub in Azure and integrating with it from both a consumer and producer perspective. Azure Event Hubs give us seamless integrations from other services that can be leveraged to do various types of streaming and batch based analytics

* First we create the required event hub 
<img src="../images/azeventhub_01.png" />

* The we setup our developer env and try to produce the same temperature JSON data to the Azure Event Hub. We did the same thing previously with KAFKA running locally as well as inside the Azure VM. 

<pre>
set JAVA_HOME=c:\Venky\jdk-11.0.15.10-hotspot
set PATH=%PATH%;c:\Venky\spark\bin;c:\Venky\apache-maven-3.8.6\bin
set SPARK_HOME=c:\Venky\spark
SET HADOOP_HOME=C:\Venky\AzureSynapseExperiments\SparkExamples

cd C:\Venky\AzureSynapseExperiments\SparkExamples
mvn clean package

## Producer to send messages 

mvn exec:java -Dexec.mainClass="com.gssystems.azeventhub.TemperaturesProducer" -Dexec.args="C:\Venky\DP-203\AzureSynapseExperiments\datafiles\streaming\output\part-00000-2fa6257f-a51c-41e6-9572-630bf2a22bfd-c000.json C:\Venky\DP-203\AzureSynapseExperiments\datafiles\streaming\location_master\part-00000-9ce98557-48be-4823-bfb3-a0764b296729-c000.json"

## Consumer to download in json format.
spark-submit --packages com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22 --conf spark.sql.streaming.checkpointLocation=file:///C:\Venky\spark_checkpoints\ --master local[4] --class com.gssystems.azeventhub.AEHStreamToJSONDownloader target/SparkExamples-1.0-SNAPSHOT.jar file:///C:/Venky/DP-203/AzureSynapseExperiments/datafiles/aeh_temps_json

## Streaming analytics 
spark-submit --packages com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22  --master local[4] --class com.gssystems.azeventhub.WeatherSparkStreaming target/SparkExamples-1.0-SNAPSHOT.jar

</pre>