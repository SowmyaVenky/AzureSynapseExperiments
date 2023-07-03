set JAVA_HOME=c:\Venky\jdk-11.0.15.10-hotspot
set PATH=%PATH%;c:\Venky\spark\bin;c:\Venky\apache-maven-3.8.6\bin
set SPARK_HOME=c:\Venky\spark
SET HADOOP_HOME=C:\Venky\AzureSynapseExperiments\SparkExamples

cd C:\Venky\AzureSynapseExperiments\SparkExamples
mvn clean package

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="30.188530 -95.525810 2019-01-01 2019-12-31 2019_Spring_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="30.188530 -95.525810 2020-01-01 2020-12-31 2020_Spring_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="30.188530 -95.525810 2021-01-01 2021-12-31 2021_Spring_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="30.188530 -95.525810 2022-01-01 2022-12-31 2022_Spring_Temps.json"

mvn exec:java -Dexec.mainClass="com.gssystems.spark.DownloadWeatherDataHistorical" -Dexec.args="30.188530 -95.525810 2023-01-01 2023-06-30 2023_Spring_Temps.json"

Note that there is a problem when we are using windows 10 with spark. The hadoop.dll needs to be downloaded from 
https://github.com/steveloughran/winutils/tree/master/hadoop-3.0.0/bin and put into C:\Windows\System32 folder. Then we need to have the winutils.exe in a folder and set that as HADOOP_HOME. See setting above. 

spark-submit --master local[4] --class com.gssystems.spark.TemperaturesReformatter target\SparkExamples-1.0-SNAPSHOT.jar file:///C:/Venky/AzureSynapseExperiments/datafiles/spring_tx_temps/ file:///C:/Venky/AzureSynapseExperiments/datafiles/spring_tx_temps_formatted/
