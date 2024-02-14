# Azure Synapse Spark pool testing

* This experiment will setup a Synapse workspace and create a set of different sized spark pools. Then we will test the concurrency characteristics to determine the limitations we might face in a multi-tenant scenario where a lot of concurrent spark jobs can run. 

* Log into the acloudguru account and spawn a sandbox azure environment. Then create an ADLS account and container.

* Now create a Synapse workspace and attach it to the created ADLS storage account and container. 

* The resource group should  look something like this.


