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
Write-Host "Creating Secure ADLS Storage Account. This may take some time..."

New-AzResourceGroupDeployment -ResourceGroupName $resourceGroupName `
  -TemplateFile "arm-templates/secureADLS/azuredeploy.json" `
  -TemplateParameterFile "arm-templates/secureADLS/azuredeploy.parameters.json" `
  -Mode Incremental `
  -Force

## Create a new container called files to use
$storageCtx = New-AzStorageContext -StorageAccountName "venkydatalake1001" -UseConnectedAccount
## Gives auth error for some reason.
## New-AzStorageContainer  -Name "files101" -Context $storageCtx
Write-Host "Create a files container in the created storage account..."