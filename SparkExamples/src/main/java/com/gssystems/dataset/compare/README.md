## Commands to execute

<pre>
set JAVA_HOME=c:\Venky\jdk-11.0.15.10-hotspot
set PATH=%PATH%;c:\Venky\spark\bin;c:\Venky\apache-maven-3.8.6\bin
set SPARK_HOME=c:\Venky\spark
SET HADOOP_HOME=C:\Venky\AzureSynapseExperiments\SparkExamples

cd C:\Venky\AzureSynapseExperiments\SparkExamples
mvn clean package

Create the orc dataset. 

spark-submit --master local[4] --class com.gssystems.dataset.compare.ORCFileCreator target\SparkExamples-1.0-SNAPSHOT.jar file:///C:/Venky/AzureSynapseExperiments/datafiles/spring_tx_temps_formatted/ file:///C:/Venky/AzureSynapseExperiments/datafiles/spring_tx_temps_orc/


Create the parquet dataset.

spark-submit --master local[4] --class com.gssystems.dataset.compare.ParquetFileCreator target\SparkExamples-1.0-SNAPSHOT.jar file:///C:/Venky/AzureSynapseExperiments/datafiles/spring_tx_temps_formatted/ file:///C:/Venky/AzureSynapseExperiments/datafiles/spring_tx_temps_parquet/

# Run the compare program

spark-submit --master local[4] --class com.gssystems.dataset.compare.CompareUtil target\SparkExamples-1.0-SNAPSHOT.jar file:///C:/Venky/AzureSynapseExperiments/datafiles/spring_tx_temps_orc/ file:///C:/Venky/AzureSynapseExperiments/datafiles/spring_tx_temps_parquet/ "latitude,longitude,time" file:///C:/Venky/AzureSynapseExperiments/datafiles/spring_tx_temps_orcpqcompare/

</pre>
