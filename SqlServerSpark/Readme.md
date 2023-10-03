## SQL Server Spark Testing

* This experiment will use Spark to push records to a SQL Server instance running in Docker.

<pre>
 docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=Ganesh20022002" -p 1433:1433 -d mcr.microsoft.com/mssql/server:2022-latest

Once the docker container starts, we can see that it is in running status.
C:\Venky\DP-203\AzureSynapseExperiments\SparkExamples>docker ps
CONTAINER ID   IMAGE                                        COMMAND                  CREATED          STATUS          PORTS                    NAMES
e582d76cbf87   mcr.microsoft.com/mssql/server:2022-latest   "/opt/mssql/bin/perm…"   12 seconds ago   Up 11 seconds   0.0.0.0:1433->1433/tcp   strange_swartz

</pre>

* We will connect to the database via Azure Data Studio and execute the commands needed to create a database and the tables under that database. 
<pre>
drop table temperatures;

create table temperatures ( 
    latitude float not null, 
    longitude float not null, 
    [time] varchar not null, 
    temperature_2m float,
    CONSTRAINT PK_temperatures PRIMARY KEY (latitude,longitude, [time])
)
</pre>

<img src="./images/sqls_001.png" />

<pre>
set JAVA_HOME=c:\Venky\jdk-11.0.15.10-hotspot
set PATH=%PATH%;c:\Venky\spark\bin;c:\Venky\apache-maven-3.8.4\bin
set SPARK_HOME=c:\Venky\spark
SET HADOOP_HOME=C:\Venky\DP-203\AzureSynapseExperiments\SparkExamples

cd C:\Venky\DP-203\AzureSynapseExperiments\SparkExamples
mvn clean package 

## Test SQL Server connection
mvn exec:java -Dexec.mainClass="com.gssystems.sqlserver.SQLServerJDBCTest" 

# Run the program to read from the delta format and push it to sql server.
#
spark-submit --master local[4] --jars file:///C:/Venky/DP-203/AzureSynapseExperiments/SqlServerSpark/mssql-jdbc-12.4.1.jre11.jar --driver-class-path file:///C:/Venky/DP-203/AzureSynapseExperiments/SqlServerSpark/mssql-jdbc-12.4.1.jre11.jar --packages io.delta:delta-core_2.12:2.2.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --class com.gssystems.sqlserver.TemperaturesLoader target\SparkExamples-1.0-SNAPSHOT.jar file:///C:/Venky/spring_tx_temps_delta/

</pre>

* After the code runs, we can see that the data is loaded into SQLServer. 
<img src="./images/sqls_002.png" />

* We will apply the same technique we used with the Delta Lake streaming merge. Events will come in via KAFKA and we will load that data into a temporary table in SQLServer and use the SQLServer's native merge functionality to merge the two tables and create the merged dataset on SQL server. This will be a good test to compare times between the SQLServer merge vs the Delta lake merge. 

* We need to first start KAFKA on docker to start streaming data. 
<pre>
cd C:\Venky\DP-203\AzureSynapseExperiments\kafka_docker
docker-compose up -d 

Once this starts up, we can start our producer to push the data to the KAFKA topic. 

cd C:\Venky\DP-203\AzureSynapseExperiments\SparkExamples
mvn clean package 

mvn exec:java -Dexec.mainClass="com.gssystems.kafka.WeatherDataStreamingProducer" -Dexec.args="C:\Venky\DP-203\AzureSynapseExperiments\datafiles\streaming\output\part-00000-dd3eed31-5521-456d-9fcd-3d66c266f6fc-c000.json C:\Venky\DP-203\AzureSynapseExperiments\datafiles\streaming\location_master\part-00000-a3a34469-0ef8-496f-be3f-826ef3d55233-c000.json"
</pre>

* CHANGE PUBLIC IP OF machine if running from local to azure, else 127.0.0.1 
<pre>
spark-submit --jars file:///C:/Venky/DP-203/AzureSynapseExperiments/SqlServerSpark/mssql-jdbc-12.4.1.jre11.jar --driver-class-path file:///C:/Venky/DP-203/AzureSynapseExperiments/SqlServerSpark/mssql-jdbc-12.4.1.jre11.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 --master local[4] --class com.gssystems.sqlserver.TemperaturesStreamingMerger target/SparkExamples-1.0-SNAPSHOT.jar 127.0.0.1 temperatures
</pre>

* Now fire the other dataset that causes a lot of updates, and 10 2009 row inserts

<pre>
mvn exec:java -Dexec.mainClass="com.gssystems.kafka.WeatherDataStreamingProducer" -Dexec.args="C:\Venky\DP-203\AzureSynapseExperiments\datafiles\delta_table_update_payload\part-00000-b7c18d2b-ddf5-4d7f-ad3f-eef9465d410c-c000.json C:\Venky\DP-203\AzureSynapseExperiments\datafiles\streaming\location_master\part-00000-a3a34469-0ef8-496f-be3f-826ef3d55233-c000.json"
</pre>

<img src="./images/sqls_003.png" />

* After the merges complete we can compare the data in SQLServer with the data in the Delta lake and we see that the totals match. The database merges felt a lot faster than the delta lake merges because of the presence of keys etc on the database. We can further tune this to see we can get a better performance with stored procedures. 

<img src="./images/sqls_004.png" />

<img src="./images/sqls_005.png" />