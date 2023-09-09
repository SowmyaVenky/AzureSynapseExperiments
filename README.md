# AzureSynapseExperiments
Various fun projects with Azure Synapse Suite

## Analyze JSON data that shows temperatures at Raleigh airport.
* A simple use-case that shows how to take a parquet file from ADLS and expose it as an external table inside Synapse Serverless. Then we use Synapse spark pool to take this parquet file and convert it into a delta format. Please see steps <a href="./Analyze_Raleign_Airport_Temps.md">here</a>

## Weather data by Lat and Long
* For a more exhaustive use-case where we download weather data for multiple years and create tables in Synapse Serverless Pools, please refer to the readme under <a href="./SparkExamples/">SparkExamples</a>

## PII/PCI Fun test case. 

*  For a fun use-case where we can test how to scramble PII/PCI data and control access via entitlements, refer to <a href="./SparkExamples/PII_PCI_Data_Hash_Testing.md">this</a>