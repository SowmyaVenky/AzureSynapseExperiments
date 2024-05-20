from datetime import datetime
import json
import os
import requests
import base64

class Utils:

    @staticmethod
    def get_access_token(azure_client_id, azure_tenant_id, azure_client_secret):
        url = f"https://login.microsoftonline.com/{azure_tenant_id}/oauth2/token"

        payload = {
            "grant_type": "client_credentials",
            "client_id": {azure_client_id},
            "client_secret": {azure_client_secret},
            "resource": f"https://dev.azuresynapse.net/"
        }

        headers = {'Content-Type': 'application/x-www-form-urlencoded'}

        response = requests.post(url, data=payload, headers=headers)
        response_json = json.loads(response.text)
        synapse_dev_token = response_json["access_token"]

        return synapse_dev_token


    @staticmethod
    def export_notebooks(azure_client_id, azure_tenant_id, azure_client_secret, synapse_workspace_name, output_folder):
        resource_type = "notebooks"
        Utils.export_resources(resource_type, azure_client_id, azure_tenant_id, azure_client_secret, synapse_workspace_name, output_folder)

    @staticmethod
    def export_resources(resource_type, azure_client_id, azure_tenant_id, azure_client_secret, synapse_workspace_name, output_folder):

        base_uri = f"{synapse_workspace_name}.dev.azuresynapse.net"
        # api_version = "2020-12-01"
        api_version = "2019-11-01-preview"
        synapse_dev_token = Utils.get_access_token(azure_client_id, azure_tenant_id, azure_client_secret)
        res_exported = 0
        resources_exported = {}

        url = f"https://{base_uri}/{resource_type}?api-version={api_version}"

        headers = {
            'Authorization': f'Bearer {synapse_dev_token}',
            'Content-Type': 'application/json'
        }
        
        print(url)
        print(headers)

        response = requests.request("GET", url, headers=headers)
        print(response)

        if response != None and response.ok:
            response_json = response.json()
            print(f"Exporting individual resources of type '{resource_type}' from '{synapse_workspace_name}' Azure Synapse workspace...")
            if "value" in response_json:
                response_json = response_json['value']
            elif "items" in response_json:
                response_json = response_json['items']
            for artifact in response_json:
                if "name" in artifact:
                    resource_name = artifact["name"]
                elif "Name" in artifact:
                    resource_name = artifact["Name"]
                print(f"Exporting '{resource_name}' ...")
                resource_url = f"https://{base_uri}/{resource_type}/{resource_name}?api-version={api_version}"
                resource_response = requests.request("GET", resource_url, headers=headers)

                if resource_response != None and response.ok:
                    resource_response_json = resource_response.json()

                    if (resource_type == "sparkJobDefinitions"):
                        sjd_json = resource_response_json
                        file_name = f"{resource_name}.json"
                        data = json.dumps(sjd_json, indent=4)
                    elif (resource_type == "notebooks"):
                        notebook_json = resource_response_json['properties']
                        tags_to_clean = ['outputs']
                        updated_ntbk_json = Utils.clean_notebook_cells(notebook_json, tags_to_clean)
                        file_name = f"{resource_name}.ipynb"
                        data = json.dumps(updated_ntbk_json, indent=4)
                    
                    #mssparkutils.fs.put(f"{output_folder}/{resource_type}/{file_name}", data, False)
                    print(data)
                    res_exported += 1
                    resources_exported[resource_type] = res_exported
                    
        else:
            raise RuntimeError(f"Exporting items of type '{resource_type}' failed: {response.status_code}: {response}")
