## ADLS continuous ingest into ADX demonstration
<img src="../images/eh_arch_2.png" />


* Now we will create the Synapse workspace with all the required elements via the ARM template. Execute 1005-Create-Synapse-workspace.ps1. After this runs, it will create an ADX pool that we can use to analyze data present in our data lake via a continous ingest.

<img src="../images/synapse_azeventhub_00.png" />

* Note that this will leverage on the files that were generated and reformatted from the previous sessions. We will also download new files for other locations and date ranges, process them and put that into the spot where ADX expects to see the files. Once this happens, the ADX system automatically ingests these newly added files into its database and it is available for analytics. Let us see how much the latency plays in once the files are dropped, and the queries reflect the aggregations based on the newly added data. 

* Once the synapse workspace is spawned and ready to go, let us go to the ADLS linked to the workspace and put in some new directories, and files that we have downloaded from the weather API service. This will be the first set of files we load. 

* We have weather data for the following cities (Spring, Anchorage, Bangalore, London, Paris, Rome) from 2019 to 2023. It is daily hour by hour data. We can pretend that we have multiple orgs and each of these have a data lake stored in a separate storage account because of data localization requirements. Let us add these 3 storage accounts and link them to Synapse ( venkydlusa1001, venkydleu1001, venkydlasia1001). I am keeping all these in East US, but in theory they can be in multiple regions. 

<img src="../images/synapse_azeventhub_19.png" />

* We will now pretend these are data lakes and create folders to store data. Each storage account has one container called datalake and has 3 folders under it bronze, silver, and gold as shown below. Showing it for just one storage account, but it is created for all the 3 data lake accounts. 

<img src="../images/synapse_azeventhub_20.png" />

* Upload the relevant files that were downloaded from the API service to the correct folders. Like for instance London, Paris and Rome go into the EU data lake etc.

<img src="../images/synapse_azeventhub_21.png" />

<img src="../images/synapse_azeventhub_22.png" />

<img src="../images/synapse_azeventhub_23.png" />

* Next we need to prep the spark pool. For this we will open a notebook, print(spark) and execute it to warm the spark pool. Else, Livy will throw errors.It takes a while for the spark pool to start, but it will show a green circle once it starts. Next we need to stop the session to do other jobs since we have a small pool. 

<img src="../images/synapse_azeventhub_24.png" />

* Upload the jar file we have generated on the SparkExamples folder after running mvn clean package. We are trying to create a new spark definition from scratch here, but the entire definition can be imported from the JSON files provided. 

<img src="../images/synapse_azeventhub_25.png" />

* We need to add these parameters to the job definition on the screen. Here are the parameters. 
<pre>
Class
com.gssystems.spark.TemperaturesReformatter

Parameters
abfss://datalake@venkhdleu1001.dfs.core.windows.net/bronze/ abfss://datalake@venkhdleu1001.dfs.core.windows.net/silver/temperatures_formatted abfss://datalake@venkhdleu1001.dfs.core.windows.net/silver/location_master 
</pre>

<img src="../images/synapse_azeventhub_26.png" />

<img src="../images/synapse_azeventhub_27.png" />

* Hit the publish and let the spark job definition save. Then we can submit the job and switch to monitoring. 

<img src="../images/synapse_azeventhub_28.png" />

* After the jobs is finished, we can see the files created in ADLS as expected. 

<img src="../images/synapse_azeventhub_29.png" />

* Run the job two times more to get the usa and asia datalakes also populated.

<img src="../images/synapse_azeventhub_30.png" />

* Create the required database.
<img src="../images/synapse_azeventhub_31.png" />

* Ingest the data.
<img src="../images/synapse_azeventhub_32.png" />

* Follow screens
<img src="../images/synapse_azeventhub_33.png" />

<img src="../images/synapse_azeventhub_34.png" />

<img src="../images/synapse_azeventhub_35.png" />

<img src="../images/synapse_azeventhub_35.png" />

* Sanity check queries to see what is ingested 

<img src="../images/synapse_azeventhub_35.png" />

