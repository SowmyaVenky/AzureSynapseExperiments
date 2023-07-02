# AzureSynapseExperiments
Various fun projects with Azure Synapse Suite

## Analyze JSON data that shows temperatures at Raleigh airport.
* Login to azure with your username and password.
* Open a command shell and execute Connect-AzAccount.
* Once logged in, run the powershell script to provision the synapse workspace and all the dependent objects (ADLS, dedicated pool, spark pool and data explorer pool.)
* Upload the json array file containing the temperature data to the attached ADLS storage of the synapse workspace. 
* We can now use the built in pool or serverless pool to do adhoc analytics directly on files present in the datalake.
* We can create a logical 