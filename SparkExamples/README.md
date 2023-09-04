set JAVA_HOME=c:\Venky\jdk-11.0.15.10-hotspot
set PATH=%PATH%;c:\Venky\spark\bin;c:\Venky\apache-maven-3.8.6\bin
set SPARK_HOME=c:\Venky\spark
SET HADOOP_HOME=C:\Venky\AzureSynapseExperiments\SparkExamples

cd C:\Venky\AzureSynapseExperiments\SparkExamples
mvn clean package

* Run these steps on your local machine or run them on the DSVM that has spark installed and pre-configured. This will download the temperatures for a specific lat and long for a time period. Once the raw data is pulled down, we do some data massaging with Spark to clean the data up and make it into a format that can be used by Synapse easily.

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

* Once we run all the steps above, we will have a parquet file that contains time and temperature readings. We will also have another folder that has the location data. We need to open the container that is provisioned and create a new folders location_master and spring_tx_temps_formatted. Upload the parquet files to the ADLS directory and the folder can be queried by synapse via the linked service. 

* Testing with the storage account having public access enabled. When the ARM template created the storage account the public access is allowed. This means that the openrowset function can reach out to the storage account and get access to files to query the data. This is demonstrated in the screen shots below. 

<p align="center">
  <img src="../images/Storage_Acct_Pub_Access.png" title="Sample Architecure">
</p>
<p align="center">
  <img src="../images/SQL_Query_Success.png" title="Sample Architecure">
</p>

* This technique is not very secure, and we need to tighten things up a lot more. 
* We need to create a storage account with ADLS namespace enabled. To start with we can make the firewall settings in the network settings to be Disabled. This means that the storage account access will be blocked from all networks. Private links are absolutely needed to access the storage account. 
* I also created a virtual network and a Windows VM inside it. I configured the private endpoint to the storage account to be in the same subnet as the virtual machine. Once we do this setting we can do a nslookup on the storage account with the venkydatalake1001.dfs.core.windows.net and it will resolve to a PRIVATE IP, not a public IP. 

<p align="center">
  <img src="../images/nslookup_from_inside_vm.png" title="Sample Architecure">
</p>
<p align="center">
  <img src="../images/Storage_Acct_Pub_Access_Disabled.png" title="Sample Architecure">
</p>
<p align="center">
  <img src="../images/Storage_Acct_With_Private_Endpoints.png" title="Sample Architecure">
</p>

* I also downloaded the Azure Storage EXplorer tool and installed it on the VM. I can't hit the storage account anymore from my personal computer because the firewall blocks access to it. I opened the explorer tool, logged into the subscription and loaded the storage accounts. Once I try to load the blob containers inside the storage account, I get an authorization error. It clearly says in the error that the storage account firewall is not allowing the connection to happen for data related operations. The control operations are just fine. 
<p align="center">
  <img src="../images/explorer_error_with_disabled_setting.png" title="Sample Architecure">
</p>

* To make things work, we need to click on allowing the access to the storage account from specific IP addresses or VNET. Then click on the Add existing VNET and select the vnet we created before to host the virtual machine. Once this firewall rule is enabled, things work as normal.
<p align="center">
  <img src="../images/explorer_after_access_allowed_from_vnet.png" title="Sample Architecure">
</p>

* Also note that when we are creating the private link to connect the storage account to the vnet, it will ask for the type of resource we are creating the end-point for. In our case, since we are hosting an ADLS storage account, we need to make sure we select <b>DFS</b> as the target resource for the private link. If we were to pick blob instead, then the private link for ADLS would not work, and the nslookup test before would give a PUBLIC IP, not a private IP starting with a 10.XX.XX.XX from the subnet ranage. 

## Synapse testing

* Once we have got connectivity from the VM we can check out this repository from the VM, and upload the folders needed to the files container we had created before. Once we do this we need to ensure that the ACLs for the container and each of the folders are set to execute for the Other principal. Without this setting, I was never able to get the files uploaded since the user can't list the contents of the folders. 
<p align="center">
  <img src="../images/acl_settings_for_container.png" title="Sample Architecure">
</p>

* Now we can create the synapse workspace by selecting it from the +create menu. Once we present the existing storage account to it, Synapse understands that the storage account is not available publically and sets up both a private end-point and a vnet integration to make sure the connectivity is established. Once the synapse workspace gets created, we however need to MANUALLY APPROVE the private end-point that was created. Without this, Synapse will fail to access the storage account. 
<p align="center">
  <img src="../images/approve_synapse_private_endpoint.png" title="Sample Architecure">
</p>

* Also note that Synapse was smart enough to add the workspace we created to the allow list of the storage account, under the resources area. 
<p align="center">
  <img src="../images/approved_synapse_workspace_in_firewall.png" title="Sample Architecure">
</p>

* Once we get all this situated, we can open the Synapse workspace UI, and go to the linked storage accounts on there. Then opening the files container, we can right click on the folders and SQL to query the rows. Once we execute it, Synapse talks to the private endpoint of the storage account and executes the required queries. We can connect to synapse from both the Azure VM and the personal computer and execute queries because the Synapse workspace is not locked down and accessible publicly. We will change that and lock it down further. 
<p align="center">
  <img src="../images/query_success_from_synapse.png" title="Sample Architecure">
</p>

* These are the steps required to be followed on an Ubuntu machine to test things out. 

./azcopy login 

./azcopy copy "/home/venkyuser/spring_tx_temps_formatted" "https://venkydatalake.dfs.core.windows.net/files/" --recursive=true

./azcopy list "https://venkydatalake.dfs.core.windows.net/files/" 

./azcopy remove "https://venkydatalake.dfs.core.windows.net/files/spring_tx_temps_formatted" --recursive=true