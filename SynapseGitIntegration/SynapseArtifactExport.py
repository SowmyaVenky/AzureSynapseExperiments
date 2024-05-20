from datetime import datetime
import json
import os
import requests
import base64
import msal

from utils.Utils import Utils

# To get the tenant id it is good to run Connect-AzAccount and once it logs in, it prints the tenant id.

azure_client_id = 'REPLACE_WITH_VALUE'
azure_tenant_id = '84f1e4ea-8554-43e1-8709-f0b8589ea118'
azure_client_secret = 'REPLACE_WITH_VALUE'
synapse_workspace_name = 'venkysyn1001'
output_folder = 'venkysyn1001_exported'

print("Getting access token for synapse...")
a = Utils()

# synapse_dev_token = a.get_access_token(azure_client_id, azure_tenant_id, azure_client_secret)
# print( synapse_dev_token)

print('Exporting notebooks...')
a.export_notebooks(azure_client_id, azure_tenant_id, azure_client_secret, synapse_workspace_name, output_folder)
