######################################################################
##                PART 5: Create Synapse workspaces #
######################################################################

####Venky Notes
#1. Login in to cloud academy and create an azure sandbox. 
#2. Login to the portal with the username and password that is given
#3. Get the resource group name that has been provisioned. 
  
# DEFINE RESOURCE GROUP NAME AND LOCATION PARAMETERS
$resourceGroupName = (Get-AzResourceGroup).ResourceGroupName

#Login to Azure  - First pass uncomment to login to azure.
# Connect-AzAccount

#################################01 - Create ADF ########################
# Use ARM template to deploy resources
Write-Host "Creating Azure Event Grid. This may take some time..."

New-AzResourceGroupDeployment -ResourceGroupName $resourceGroupName `
  -TemplateFile "arm-templates/event_hub/azuredeploy.json" `
  -TemplateParameterFile "arm-templates/event_hub/azuredeploy.parameters.json" `
  -Mode Incremental `
  -Force

  $ehubName="temperatures"
  New-AzEventHub -ResourceGroupName $resourceGroupName `
   -NamespaceName venkyeh1001 `
   -EventHubName $ehubName  

  New-AzEventHubConsumerGroup -Name json-dw-cg `
   -NamespaceName venkyeh1001 `
   -ResourceGroupName $resourceGroupName `
   -EventHubName $ehubName `
   -UserMetadata "Consumer Group for JSON download streaming Util"

  New-AzEventHubConsumerGroup -Name stream-anal-cg `
   -NamespaceName venkyeh1001 `
   -ResourceGroupName $resourceGroupName `
   -EventHubName $ehubName `
   -UserMetadata "Consumer Group for streaming analytics Util"