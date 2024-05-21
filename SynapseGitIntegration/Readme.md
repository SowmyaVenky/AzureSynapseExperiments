## Synapse Git Integration and Artifact Promotions

* First we need to setup Synapse, a spark pool and generate some artifacts that we can use as a base to export and import into another Synapse instance to prove out the capabilities of moving code from one Synapse region to another. To start with the base Synapse setup please follow the instructions <a href="Synapse_Env_Setup.md">here</a>

* Once the Synapse region is setup, we upload some sample data, run some SQL Scripts and Notebooks to validate the environment, and then export these assets using both powershell and azure CLI. To follow along please go <a href="./Synapse_Artifact_Export.md">here</a>