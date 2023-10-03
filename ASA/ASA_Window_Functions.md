## Explore Azure Streaming Analytics Window Functions

* The base dataset we got from the weather API does not have a proper datetime field. The issue with that is that we can't really use it as is for the ASA window queries. We are now going to convert the base dataset to add a datetime field. 

<pre>
set JAVA_HOME=c:\Venky\jdk-11.0.15.10-hotspot
set PATH=%PATH%;c:\Venky\spark\bin;c:\Venky\apache-maven-3.8.4\bin
set SPARK_HOME=c:\Venky\spark
SET HADOOP_HOME=C:\Venky\DP-203\AzureSynapseExperiments\SparkExamples

cd C:\Venky\DP-203\AzureSynapseExperiments\SparkExamples
mvn clean package 

# Run the program to read from the parquet format, add a datetime field and write out.

spark-submit --master local[4] --class com.gssystems.azeventhub.ASADateTimeAdjuster target\SparkExamples-1.0-SNAPSHOT.jar file:///C:/Venky\DP-203/AzureSynapseExperiments/datafiles/spring_tx_temps_formatted/ file:///C:/Venky\DP-203/AzureSynapseExperiments/datafiles/spring_tx_dtadded/

</pre>

* This will now create a dataset with year, month, day and hour split, and also a timestamp field added that can now be used to push to the event hub, and then consumed via Azure streaming analytics. Now we can push this to the event hub using spark.

<pre>
spark-submit --master local[4] --class com.gssystems.azeventhub.SparkEventHubProducer target\SparkExamples-1.0-SNAPSHOT.jar file:///C:/Venky\DP-203/AzureSynapseExperiments/datafiles/spring_tx_dtadded/

</pre>