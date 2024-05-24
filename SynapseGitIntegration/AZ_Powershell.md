
### Azure Powershell based process

* With Azure power shell we need to connect our powershell account to the subscription by logging in 
<code>
Connect-AzAccount
</code>

* Once this is executed, a browser will start up and authenticate the user. Once authenticated we can export the required notebooks and SQL Scripts as shown below:

<code>
Get-AzSynapseNotebook -WorkspaceName venkysyn1001 | Select-Object -Property Name
Get-AzSynapseNotebook -WorkspaceName venkysyn1001 -Name VenkyTestNotebook6

Export-AzSynapseNotebook -WorkspaceName venkysyn1001 -Name VenkyTestNotebook1 -OutputFolder "C:\Venky\AzureSynapseExperiments\SynapseGitIntegration\venkysyn1001-exported-notebooks"
Export-AzSynapseNotebook -WorkspaceName venkysyn1001 -Name VenkyTestNotebook2 -OutputFolder "C:\Venky\AzureSynapseExperiments\SynapseGitIntegration\venkysyn1001-exported-notebooks"
Export-AzSynapseNotebook -WorkspaceName venkysyn1001 -Name VenkyTestNotebook3 -OutputFolder "C:\Venky\AzureSynapseExperiments\SynapseGitIntegration\venkysyn1001-exported-notebooks"
Export-AzSynapseNotebook -WorkspaceName venkysyn1001 -Name VenkyTestNotebook4 -OutputFolder "C:\Venky\AzureSynapseExperiments\SynapseGitIntegration\venkysyn1001-exported-notebooks"
Export-AzSynapseNotebook -WorkspaceName venkysyn1001 -Name VenkyTestNotebook5 -OutputFolder "C:\Venky\AzureSynapseExperiments\SynapseGitIntegration\venkysyn1001-exported-notebooks"
Export-AzSynapseNotebook -WorkspaceName venkysyn1001 -Name VenkyTestNotebook6 -OutputFolder "C:\Venky\AzureSynapseExperiments\SynapseGitIntegration\venkysyn1001-exported-notebooks"
</code>

* These commands will export the notebooks in ipynb format and store it on the destination folder. 

* We can now create another script to pull down the SQL scripts present in the first Synapse workspace and store it in another folder. 

<code>
Get-AzSynapseSqlScript -WorkspaceName venkysyn1001 | Select-Object -Property Name
Get-AzSynapseSqlScript -WorkspaceName venkysyn1001 -Name VenkySQLScript1

Export-AzSynapseNotebook -WorkspaceName venkysyn1001 -Name VenkySQLScript1 -OutputFolder "C:\Venky\AzureSynapseExperiments\SynapseGitIntegration\venkysyn1001-exported-sqls"
Export-AzSynapseNotebook -WorkspaceName venkysyn1001 -Name VenkySQLScript2 -OutputFolder "C:\Venky\AzureSynapseExperiments\SynapseGitIntegration\venkysyn1001-exported-sqls"
Export-AzSynapseNotebook -WorkspaceName venkysyn1001 -Name VenkySQLScript3 -OutputFolder "C:\Venky\AzureSynapseExperiments\SynapseGitIntegration\venkysyn1001-exported-sqls"
Export-AzSynapseNotebook -WorkspaceName venkysyn1001 -Name VenkySQLScript4 -OutputFolder "C:\Venky\AzureSynapseExperiments\SynapseGitIntegration\venkysyn1001-exported-sqls"
Export-AzSynapseNotebook -WorkspaceName venkysyn1001 -Name VenkySQLScript5 -OutputFolder "C:\Venky\AzureSynapseExperiments\SynapseGitIntegration\venkysyn1001-exported-sqls"

</code>
