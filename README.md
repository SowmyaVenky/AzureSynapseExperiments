# AzureSynapseExperiments
Various fun projects with Azure Synapse Suite

## Analyze JSON data that shows temperatures at Raleigh airport.
* Login to azure with your username and password.
* Open a command shell and execute Connect-AzAccount.
* Once logged in, run the powershell script to provision the synapse workspace and all the dependent objects (ADLS, dedicated pool, spark pool and data explorer pool.)
* Upload the json array file containing the temperature data to the attached ADLS storage of the synapse workspace. 
* We can now use the built in pool or serverless pool to do adhoc analytics directly on files present in the datalake.
* Now we can execute a notebook in the created spark pool to read the json array, parse it, and store it into a new directory with a parquet format. 
* Once the parquet files are created in the directory, we can now create an external table to refer to these in the serverless pool. This can act as a logical data warehouse and mininize costs since nothing is provisioned.
* Create a delta lake version of the same data set.
* Create an external table and query data from serverless pool.
* Try to update data in delta lake via Serverless pool (NOT ALLOWED WILL GIVE ERROR!)
* Try updating the data (delete) via the notebook. Remove rows where prcp = 1 and rows where date is in 2017. Vaccum and see.

## Weather data by Lat and Long
* For a more exhaustive use-case where we download weather data for multiple years and create tables in Synapse Serverless Pools, please refer to the readme under <a href="./SparkExamples/">SparkExamples</a>

## PII/PCI Fun test case. 

*  For a fun use-case where we can test how to scramble PII/PCI data and control access via entitlements, refer to <a href="./SparkExamples/PII_PCI_Data_Hash_Testing.md">this</a>