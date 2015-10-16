import os
import shutil
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
  print("\t" + str(response.status_code))
  return(response.json())

def create_archivestore(storageClass, label, path):
  print("create_archivestore : " + label)
  payload = {"storageClass": storageClass, "label": label, "properties":{"rootPath":path}}
  headers = {'Content-type': 'application/json', 'X-UserID': username}
  response = requests.post(server + '/datavault-broker/archivestores', data=json.dumps(payload), headers=headers)
  print("\t" + str(response.status_code))
  return(response.json())

def create_vault(name, description, policyID):
  print("create_vault : " + name)
  payload = {"name": name, "description": description, "policyID": policyID}
  headers = {'Content-type': 'application/json', 'X-UserID': username}
  response = requests.post(server + '/datavault-broker/vaults', data=json.dumps(payload), headers=headers)
  print("\t" + str(response.status_code))
  return(response.json())

def list_vaults():
  print("list_vaults")
  headers = {'Content-type': 'application/json', 'X-UserID': username}
  response = requests.get(server + '/datavault-broker/vaults', headers=headers)
  print("\t" + str(response.status_code))
  return(response.json())

# Init the test environment
def setup():
  clear_data()
  generate_test_data()
  create_filestore("org.datavaultplatform.common.storage.impl.LocalFileSystem", "Test data source", datapath)
  create_archivestore("org.datavaultplatform.common.storage.impl.LocalFileSystem", "Test archive", archivepath)

def clear_data():
  clear_directory(datapath)
  clear_directory(archivepath)

def clear_directory(path):
  for root, dirs, files in os.walk(path):
    for f in files:
      print("unlink: " + f)
      os.unlink(os.path.join(root, f))
    for d in dirs:
      print("rmtree: " + f)
      shutil.rmtree(os.path.join(root, d))

def generate_test_data():
  create_file("250M", datapath + "/" + "test_data_250MB.bin"
  create_file("100M", datapath + "/" + "test_data_100MB.bin"
  create_file("50M", datapath + "/" + "test_data_50MB.bin"
  create_file("25M", datapath + "/" + "test_data_25MB.bin"

def create_file(size, path):
  print("create_file: " + path)
  command = "fallocate -l " + size + " " + path

# Test script body
print("API test : " + username)
setup()
create_vault("Test vault", "Automatically created vault", "UNIVERSITY")
vaults = list_vaults()
for vault in vaults:
  print("Vault: " + vault['id'] + " Name: " + vault['name'])


