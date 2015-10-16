import requests
import json

# System details
server = "http://localhost:8080"
datapath = "/home/ubuntu/data/api-test-data"
archivepath = "/home/ubuntu/data/api-test-archive"

# Test user details
username = "user1"

# Utility functions
def create_filestore(storageClass, label, path):
  print("create_filestore : " + label)
  payload = {"storageClass": storageClass, "label": label, "properties":{"rootPath":path}}
  headers = {'Content-type': 'application/json', 'X-UserID': username}
  response = requests.post(server + '/datavault-broker/filestores', data=json.dumps(payload), headers=headers)
  print(response.status_code)
  print(response.json())

def create_archivestore(storageClass, label, path):
  print("create_archivestore : " + label)
  payload = {"storageClass": storageClass, "label": label, "properties":{"rootPath":path}}
  headers = {'Content-type': 'application/json', 'X-UserID': username}
  result = requests.post(server + '/datavault-broker/archivestores', data=json.dumps(payload), headers=headers)
  print(response.status_code)
  print(response.json())

def create_vault(name, description, policyID):
  print("create_vault : " + name)
  payload = {"name": name, "description": description, "policyID": policyID}
  headers = {'Content-type': 'application/json', 'X-UserID': username}
  response = requests.post(server + '/datavault-broker/vaults', data=json.dumps(payload), headers=headers)
  print(response.status_code)
  print(response.json())

def list_vaults():
  print("list_vaults")
  headers = {'Content-type': 'application/json', 'X-UserID': username}
  result = requests.get(server + '/datavault-broker/vaults', headers=headers)
  print(response.status_code)
  print(response.json())

# Init the test environment
def setup():
  create_filestore("org.datavaultplatform.common.storage.impl.LocalFileSystem", "Test data source", datapath)
  create_archivestore("org.datavaultplatform.common.storage.impl.LocalFileSystem", "Test archive", archivepath)

# Test script body
print("API test : " + username)
setup()
create_vault("Test vault", "Automatically created vault", "UNIVERSITY")
print(list_vaults())

