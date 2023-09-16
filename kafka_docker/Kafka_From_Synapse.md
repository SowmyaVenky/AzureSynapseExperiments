* Let us now experiment what we can do with Azure Synapse and Kafka integration. 
* Execute the Connect-AzAccount to login to Azure. Run the 1005-Create-Synapse-workspace.ps1
* This will provision a simple Synapse workspace without all the security needed in real life. Storage accounts are exposed to everyone, and a serverless, dedicated and spark pool. We will try various interactions with KAFKA from this Synapse workspace. 
* After following the instructions <a href="./Kafka_on_Azure_VM.md"> here </a>, we should have a docker based kafka setup with the temperatures topic. It will have quite a bit of temperature readings. The aggregated data via spark is also shown here.
<img src="../images/docker_with_kafka_messages.png" />