## CMEK setup with Synapse

* First we need to create a VNET and a subnet to enable us to create private endpoints to both ADLS storage and the Azure Key Vault that holds the keys to encrypt the workspace.

* Create a key-vault with the vault access policies enabled. Create it making sure that the public access is diabled and the private endpoints are enabled from the VNET to the key-vault. The process warns us that the service endpoints to the key-vault are not enabled inside the selected vnet, and it could take 15 mins to complete, but it usually finishes pretty fast.

* Note that the key-vault has been given access from our vnet, and an end-point connection to the vault has also been created and is in the approved state.

<img src="./images/cmek-001.png" />

<img src="./images/cmek-002.png" />

* Note resources created with the key-vault in place.

<img src="./images/cmek-003.png" />

* Create 2 keys to use for the CMEK with ADLS and Synapse. 

<img src="./images/cmek-004.png" />

* Create a UMI to use for talking to the KV and get the keys to encrypt/decrypt.

<img src="./images/cmek-005.png" />

* Create a vault access policy to connect to this KV and get keys. UMI will now have power to talk to the KV and get the keys.

<img src="./images/cmek-006.png" />

* Create a new storage account with the ADLS namespace enabled. Then make sure the network access is set to private and add a private link to the storage account to ensure nothing can talk to it other than the private connections we allow from the vnet. Note also that we have set the ADLS gen 2 to be encrypted with a CMEK and it is using the venky-umi to get to the KV.

<img src="./images/cmek-007.png" />

<img src="./images/cmek-008.png" />

<img src="./images/cmek-009.png" />

* I ran into an error here because Acloud guru does not allow us to create a KV with the purge protection enabled via a policy. Without that the KV will not be allowed to be used for CMEK. Will have to revert back to Microsoft managed keys for testing. 

<img src="./images/cmek-010.png" />

<img src="./images/cmek-011.png" />


<img src="./images/cmek-006.png" />




