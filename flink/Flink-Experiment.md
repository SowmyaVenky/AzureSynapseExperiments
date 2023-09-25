## Demonstrate Batch and Stream processing with Flink.
* Flink based streaming architecture
<img src="./images/flink_arch.png" />

* Testing the same ETLs we have done in SPARK before with Flink and integrate it with various services on Azure to prove out viability. For a first step, configure flink locally using docker compose. 

<pre>
cd C:\Venky\DP-203\AzureSynapseExperiments\flink
docker-compose up -d 
C:\Venky\DP-203\AzureSynapseExperiments\flink>docker ps

CONTAINER ID   IMAGE          COMMAND                  CREATED         STATUS              PORTS                              NAMES
cdf57d97c60a   flink:latest   "/docker-entrypoint.…"   2 minutes ago   Up About a minute   6123/tcp, 8081/tcp                 flink_taskmanager_2
788e66135e29   flink:latest   "/docker-entrypoint.…"   2 minutes ago   Up About a minute   6123/tcp, 8081/tcp                 flink_taskmanager_1
e92189041c0f   flink:latest   "/docker-entrypoint.…"   2 minutes ago   Up 2 minutes        6123/tcp, 0.0.0.0:8081->8081/tcp   flink_jobmanager_1
</pre>

* As we can see the simple flink environment is setup and ready to test with. In the real world however it might be setup with many VMs on Azure. 

<pre>
set JAVA_HOME=c:\Venky\jdk-11.0.15.10-hotspot
set PATH=%PATH%;c:\Venky\spark\bin;c:\Venky\apache-maven-3.8.4\bin
set SPARK_HOME=c:\Venky\spark
SET HADOOP_HOME=C:\Venky\DP-203\AzureSynapseExperiments\SparkExamples

cd C:\Venky\DP-203\AzureSynapseExperiments\flink

mvn archetype:generate -DarchetypeGroupId=org.apache.flink -DarchetypeArtifactId=flink-quickstart-java -DarchetypeVersion=1.17.1 -DgroupId=com.gssystems.flink -DartifactId=FlinkETLTesting -DinteractiveMode=false

cd FlinkETLTesting

mvn clean package 

mvn exec:java -Dexec.mainClass="com.gssystems.flink.FlinkTemperatureProcessor" -Dexec.args="--input file:///C:/Venky/DP-203/AzureSynapseExperiments/datafiles/flink_temperature_input/ --output file:///C:/Venky/DP-203/AzureSynapseExperiments/datafiles/downloaded_temps_flink/ --st 2023-01-01 --end 2023-08-30"

# Submit the job to the flink job manager.
docker exec -it flink_jobmanager_1 flink run /home/FlinkETLTesting/target/FlinkETLTesting-1.0-SNAPSHOT.jar --input /home/flink_temperature_input/ --output /home/flink_temperature_output/ --st 2023-01-01 --end 2023-08-30 

Job has been submitted with JobID babcb2a3b6b367fb0bbad2cd95fd1b00
Program execution finished
Job with JobID babcb2a3b6b367fb0bbad2cd95fd1b00 has finished.
Job Runtime: 81077 ms
</pre>

* The flink module takes a file input. The file is in json format and gives the city name and latitude and longitude. We also pass the start date and end date to download the data from the weather API and persist that to the file output stream.

* We can visit the URL http://localhost:8081/ and see the status of the job.
<img src="./images/flink_run.png" />

## Connecting the flink processor to Azure Event Hubs via the Flink Kafka Connector.

* Create the required Azure event hub using the powershell script and the ARM template.
<pre>
.\1002-Create-Azure-Event-Hub.ps1
</pre>

<img src="./images/event_hub_created.png" />

* We have to create a SAS signature for the event hub to enable clients to talk to the hub and produce/consume messages. Then we can copy the required endpoint connection string to enable Flink to talk to the event hub.

<img src="./images/sas_01.png" />

<img src="./images/sas_02.png" />

* Submitting the maven job will pull the data from the weather API, and downloads it into the directory.

<img src="./images/maven_run_01.png" />

<img src="./images/maven_run_02.png" />

* After adding the KAFKA sink and adjusting the parameters to talk to the event hub, we can see that the messages are getting pushed to the Azure Event Hub as shown below:

<img src="./images/flink_eh_sink.png" />

* We will now stream the data that we had sunk into the file system and stream it back into an aggregator. The aggregator will take the grouping parmeters we need, and create a map in the traditional map-reduce kind. Then we can calculate the max, min temperatures for each grouping condition and display it to the console.

<pre>
mvn exec:java -Dexec.mainClass="com.gssystems.flink.TemperatureStreamAggregatorFS" -Dexec.args="--input file:///C:/Venky/DP-203/AzureSynapseExperiments/datafiles/downloaded_temps_flink/ --output file:///C:/Venky/DP-203/AzureSynapseExperiments/datafiles/aggregated_temps_flink/"
</pre>

<img src="./images/flink_agg_from_fs.png" />

## Experimenting with the Table API to see whether that makes life easier.

* The idea of this experiment is to take the JSON data that was produced with regular flink map-reduce kind of code, and use the Table API construct to parse this JSON into a flink Table. Once that is in the table, we can do various analytics on the fly without having to worry about complex map and reduce functions we have used in the past to aggregate data. 

* Note that when we run the maven build, a shaded jar is generated. This shaded jar has a particular class that is designated as the main-class in the manifest. When we try to do this experiment make sure the pom.xml is modified to specify the correct main class. 

* Note also that we can't run the table API with a maven exec command like how we used to run the other programs containing flink java code. There are various classes that are needed to be in the classpath and it is just easier to run the program directly via docker that has a flink cluster running.

<pre>
docker exec -it flink_jobmanager_1 flink run /home/FlinkETLTesting/target/FlinkETLTesting-1.0-SNAPSHOT.jar --input /home/aggregated_temps_flink
</pre>

<img src="./images/flink_json_parsed1.png" />

<img src="./images/flink_json_parsed.png" />

* We can execute queries on top of the parsed data as shown below.

<pre>
// Now run aggregations on that table
System.out.println("Generating aggregations on top of parsed table...");

jsonParsedTable.groupBy(Expressions.$("year"), Expressions.$("month"))
        .select(Expressions.$("year"), Expressions.$("month"), Expressions.$("min_temp").min().as("minimum"),
                Expressions.$("max_temp").max().as("maximum"))
        .execute().print();
</pre>
<img src="./images/post_parse_agg.png" />