Get-AzSynapseSqlScript -WorkspaceName venkysyn1001 | Select-Object -Property Name
Get-AzSynapseSqlScript -WorkspaceName venkysyn1001 -Name VenkySQLScript1

Export-AzSynapseNotebook -WorkspaceName venkysyn1001 -Name VenkySQLScript1 -OutputFolder "C:\Venky\AzureSynapseExperiments\SynapseGitIntegration\venkysyn1001-exported-sqls"
Export-AzSynapseNotebook -WorkspaceName venkysyn1001 -Name VenkySQLScript2 -OutputFolder "C:\Venky\AzureSynapseExperiments\SynapseGitIntegration\venkysyn1001-exported-sqls"
Export-AzSynapseNotebook -WorkspaceName venkysyn1001 -Name VenkySQLScript3 -OutputFolder "C:\Venky\AzureSynapseExperiments\SynapseGitIntegration\venkysyn1001-exported-sqls"
Export-AzSynapseNotebook -WorkspaceName venkysyn1001 -Name VenkySQLScript4 -OutputFolder "C:\Venky\AzureSynapseExperiments\SynapseGitIntegration\venkysyn1001-exported-sqls"
Export-AzSynapseNotebook -WorkspaceName venkysyn1001 -Name VenkySQLScript5 -OutputFolder "C:\Venky\AzureSynapseExperiments\SynapseGitIntegration\venkysyn1001-exported-sqls"
