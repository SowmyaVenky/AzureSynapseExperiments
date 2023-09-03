set JAVA_HOME=c:\Venky\jdk-11.0.15.10-hotspot
set PATH=%PATH%;c:\Venky\spark\bin;c:\Venky\apache-maven-3.8.6\bin
set SPARK_HOME=c:\Venky\spark
SET HADOOP_HOME=C:\Venky\AzureSynapseExperiments\SparkExamples

cd C:\Venky\AzureSynapseExperiments\SparkExamples
mvn clean package

# Spring TX 
mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="30.188530 -95.525810 2019-01-01 2019-12-31 2019_Spring_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="30.188530 -95.525810 2020-01-01 2020-12-31 2020_Spring_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="30.188530 -95.525810 2021-01-01 2021-12-31 2021_Spring_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="30.188530 -95.525810 2022-01-01 2022-12-31 2022_Spring_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="30.188530 -95.525810 2023-01-01 2023-06-30 2023_Spring_Temps.json"

# Anchorage AK
mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="61.217381 -149.863129 2019-01-01 2019-12-31 2019_Anchorage_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="61.217381 -149.863129 2020-01-01 2020-12-31 2020_Anchorage_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="61.217381 -149.863129 2021-01-01 2021-12-31 2021_Anchorage_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="61.217381 -149.863129 2022-01-01 2022-12-31 2022_Anchorage_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="61.217381 -149.863129 2023-01-01 2023-06-30 2023_Anchorage_Temps.json"

# Bangalore 
mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="12.971599 77.594566 2019-01-01 2019-12-31 2019_Bangalore_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="12.971599 77.594566 2020-01-01 2020-12-31 2020_Bangalore_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="12.971599 77.594566 2021-01-01 2021-12-31 2021_Bangalore_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="12.971599 77.594566 2022-01-01 2022-12-31 2022_Bangalore_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="12.971599 77.594566 2023-01-01 2023-06-30 2023_Bangalore_Temps.json"

Note that there is a problem when we are using windows 10 with spark. The hadoop.dll needs to be downloaded from 
https://github.com/steveloughran/winutils/tree/master/hadoop-3.0.0/bin and put into C:\Windows\System32 folder. Then we need to have the winutils.exe in a folder and set that as HADOOP_HOME. See setting above. 

spark-submit --master local[4] --class com.gssystems.spark.TemperaturesReformatter target\SparkExamples-1.0-SNAPSHOT.jar file:///C:/Venky/AzureSynapseExperiments/datafiles/spring_tx_temps/ file:///C:/Venky/AzureSynapseExperiments/datafiles/spring_tx_temps_formatted/ file:///C:/Venky/AzureSynapseExperiments/datafiles/location_master/


* Once we run all the steps above, we will have a parquet file that contains time and temperature readings. We need to open the container that is provisioned and create a new folder spring_tx_temps_formatted. Upload the parquet file to the ADLS directory and the folder can be queried by synapse via the linked service. 

* Testing with the storage account having public access enabled. When the ARM template created the storage account the public access is allowed. This means that the openrowset function can reach out to the storage account and get access to files to query the data. This is demonstrated in the screen shots below. 

<p align="center">
  <img src="../images/Storage_Acct_Pub_Access.png" title="Sample Architecure">
</p>
<p align="center">
  <img src="../images/SQL_Query_Success.png" title="Sample Architecure">
</p>

* Next we can disable public access on the storage account without changing anything, and try to query the data via the serverless pools. This should fail and it does.

<p align="center">
  <img src="../images/Storage_Acct_Pub_Access_Disabled.png" title="Sample Architecure">
</p>
<p align="center">
  <img src="../images/SQL_Query_Success.png" title="Sample Architecure">
</p>

./azcopy login 

./azcopy copy "/home/venkyuser/spring_tx_temps_formatted" "https://venkydatalake.dfs.core.windows.net/files/" --recursive=true

./azcopy list "https://venkydatalake.dfs.core.windows.net/files/" 

./azcopy remove "https://venkydatalake.dfs.core.windows.net/files/spring_tx_temps_formatted" --recursive=true