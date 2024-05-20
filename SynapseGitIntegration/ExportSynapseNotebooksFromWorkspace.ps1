Get-AzSynapseNotebook -WorkspaceName venkysyn1001 | Select-Object -Property Name
Get-AzSynapseNotebook -WorkspaceName venkysyn1001 -Name VenkyTestNotebook6

Export-AzSynapseNotebook -WorkspaceName venkysyn1001 -Name VenkyTestNotebook1 -OutputFolder "C:\Venky\AzureSynapseExperiments\SynapseGitIntegration\venkysyn1001-exported-notebooks"
Export-AzSynapseNotebook -WorkspaceName venkysyn1001 -Name VenkyTestNotebook2 -OutputFolder "C:\Venky\AzureSynapseExperiments\SynapseGitIntegration\venkysyn1001-exported-notebooks"
Export-AzSynapseNotebook -WorkspaceName venkysyn1001 -Name VenkyTestNotebook3 -OutputFolder "C:\Venky\AzureSynapseExperiments\SynapseGitIntegration\venkysyn1001-exported-notebooks"
Export-AzSynapseNotebook -WorkspaceName venkysyn1001 -Name VenkyTestNotebook4 -OutputFolder "C:\Venky\AzureSynapseExperiments\SynapseGitIntegration\venkysyn1001-exported-notebooks"
Export-AzSynapseNotebook -WorkspaceName venkysyn1001 -Name VenkyTestNotebook5 -OutputFolder "C:\Venky\AzureSynapseExperiments\SynapseGitIntegration\venkysyn1001-exported-notebooks"
Export-AzSynapseNotebook -WorkspaceName venkysyn1001 -Name VenkyTestNotebook6 -OutputFolder "C:\Venky\AzureSynapseExperiments\SynapseGitIntegration\venkysyn1001-exported-notebooks"