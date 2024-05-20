from datetime import datetime
import json
import os
import requests
import base64
import msal

from utils.Utils import Utils

# To get the tenant id it is good to run Connect-AzAccount and once it logs in, it prints the tenant id.

azure_client_id = '29a1bdc6-772c-4101-95b7-980dcb02cfd1'
azure_tenant_id = '84f1e4ea-8554-43e1-8709-f0b8589ea118'
azure_client_secret = 'mRk8Q~gdSyVGYMPRMNkEJyGInF4YINYdlzX05c.d'
synapse_workspace_name = 'venkysyn1001'
output_folder = 'venkysyn1001_exported'

app = msal.PublicClientApplication(client_id=azure_client_id)
result = app.acquire_token_by_username_password(username="cloud_user_p_5ee46e86@realhandsonlabs.com", password="0BsWHsDhAg11x!0dVyS0")
print(result)

exit

print("Getting access token for synapse...")
a = Utils()

# synapse_dev_token = a.get_access_token(azure_client_id, azure_tenant_id, azure_client_secret)
# print( synapse_dev_token)

print('Exporting notebooks...')
a.export_notebooks(azure_client_id, azure_tenant_id, azure_client_secret, synapse_workspace_name, output_folder)
