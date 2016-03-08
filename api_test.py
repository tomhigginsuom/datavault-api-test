import os
import shutil
import requests
import json
import time

verbose = False

# System details
server = "http://127.0.0.1:8080"
client_key = "datavault-webapp"
datapath = "/home/ubuntu/data/api-test-data"
archivepath = "/home/ubuntu/data/api-test-archive"
restoreDir = "restore"

# Test user details
username = "user1"
vault_policy = "UNIVERSITY"
vault_dataset = "MOCK-DATASET-1"
vault_group = "CHSS"

# Utility functions
def create_filestore(storageClass, label, path):
  if verbose:
    print("create_filestore : " + label)
  payload = {"storageClass": storageClass, "label": label, "properties":{"rootPath":path}}
  headers = {'Content-type': 'application/json', 'X-UserID': username, 'X-Client-Key': client_key}
  response = requests.post(server + '/datavault-broker/filestores', data=json.dumps(payload), headers=headers)
  return(response.json())

def list_filestores():
  if verbose:
    print("list_filestores")
  headers = {'Content-type': 'application/json', 'X-UserID': username, 'X-Client-Key': client_key}
  response = requests.get(server + '/datavault-broker/filestores', headers=headers)
  return(response.json())

def list_files(filestoreId):
  if verbose:
    print("list_files : " + filestoreId)
  headers = {'Content-type': 'application/json', 'X-UserID': username, 'X-Client-Key': client_key}
  response = requests.get(server + '/datavault-broker/files/' + filestoreId, headers=headers)
  return(response.json())

def create_archivestore(storageClass, label, path):
  if verbose:
    print("create_archivestore : " + label)
  payload = {"storageClass": storageClass, "label": label, "properties":{"rootPath":path}}
  headers = {'Content-type': 'application/json', 'X-UserID': username, 'X-Client-Key': client_key}
  response = requests.post(server + '/datavault-broker/archivestores', data=json.dumps(payload), headers=headers)
  return(response.json())

def create_vault(name, description, policyID, groupID, datasetID):
  if verbose:
    print("create_vault : " + name)
  payload = {"name": name, "description": description, "policyID": policyID, "groupID": groupID, "datasetID": datasetID}
  headers = {'Content-type': 'application/json', 'X-UserID': username, 'X-Client-Key': client_key}
  response = requests.post(server + '/datavault-broker/vaults', data=json.dumps(payload), headers=headers)
  return(response.json())

def list_vaults():
  if verbose:
    print("list_vaults")
  headers = {'Content-type': 'application/json', 'X-UserID': username, 'X-Client-Key': client_key}
  response = requests.get(server + '/datavault-broker/vaults', headers=headers)
  return(response.json())

def create_deposit(vaultId, note, filePath):
  if verbose:
    print("create_deposit : " + note)
  payload = {"note": note, "filePath": filePath}
  headers = {'Content-type': 'application/json', 'X-UserID': username, 'X-Client-Key': client_key}
  response = requests.post(server + '/datavault-broker/vaults/' + vaultId + "/deposits", data=json.dumps(payload), headers=headers)
  return(response.json())

def list_vault_deposits(vaultId):
  if verbose:
    print("list_vault_deposits : " + vaultId)
  headers = {'Content-type': 'application/json', 'X-UserID': username, 'X-Client-Key': client_key}
  response = requests.get(server + '/datavault-broker/vaults/' + vaultId + "/deposits", headers=headers)
  return(response.json())
  
def get_deposit(vaultId, depositId):
  if verbose:
    print("get_deposit : " + vaultId + "/" + depositId)
  headers = {'Content-type': 'application/json', 'X-UserID': username, 'X-Client-Key': client_key}
  response = requests.get(server + '/datavault-broker/vaults/' + vaultId + "/deposits/" + depositId, headers=headers)
  return(response.json())

def create_restore(vaultId, depositId, note, restorePath):
  if verbose:
    print("create_restore : " + note)
  payload = {"note": note, "restorePath": restorePath}
  headers = {'Content-type': 'application/json', 'X-UserID': username, 'X-Client-Key': client_key}
  response = requests.post(server + '/datavault-broker/vaults/' + vaultId + "/deposits/" + depositId + "/restore", data=json.dumps(payload), headers=headers)
  return(response.json())

# Init the test environment
def setup():
  clear_data()
  generate_test_data()
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
      print("rmtree: " + d)
      shutil.rmtree(os.path.join(root, d))

def generate_test_data():
  os.mkdir(datapath + "/" + "restore")
  create_file("250M", datapath + "/" + "test_data_250MB.bin")
  create_file("100M", datapath + "/" + "test_data_100MB.bin")
  create_file("50M", datapath + "/" + "test_data_50MB.bin")
  create_file("25M", datapath + "/" + "test_data_25MB.bin")
  create_file("5M", datapath + "/" + "test_data_5MB.bin")
  create_file("2M", datapath + "/" + "test_data_2MB.bin")

def create_file(size, path):
  print("create_file: " + path)
  command = "fallocate -l " + size + " " + path
  os.system(command)

def dump_info():
  print("")
  print("System state")
  print("------------")
  print("")

  print("Filestores")
  print("----------")
  filestores = list_filestores()
  print("Count: " + str(len(filestores)))
  for filestore in filestores:
    print("Filestore: " + filestore['id'] + " Label: " + filestore['label'])
  print("")
  
  print("Vaults")
  print("------")
  vaults = list_vaults()
  print("Count: " + str(len(vaults)))
  for vault in vaults:
    print("Vault: " + vault['id'] + " Name: " + vault['name'])
    vault_deposits = list_vault_deposits(vault['id'])
    for vault_deposit in vault_deposits:
      print("Deposit: " + vault_deposit['id'] + " Status: " + vault_deposit['status'] + " Note: " + vault_deposit['note'])
  print("")

# Test script body
print("API test : " + username)
setup()

filestore = create_filestore("org.datavaultplatform.common.storage.impl.LocalFileSystem", "Test data source", datapath)
filestoreId = filestore['id']
print("Created file store: " + filestoreId)

# Carry out some deposits and then restore the test data

tracked_deposits = []

for x in range(0,4):
  vault = create_vault("Test vault " + str(x), "Automatically created vault", vault_policy, vault_group, vault_dataset)
  vaultId = vault['id']
  print("Created vault with ID: " + vaultId)

  files = list_files(filestoreId)
  for file in files:
    if not file['isDirectory']:
      print("File: " + file['key'] + " Name: " + file['name'])
      deposit = create_deposit(vaultId, "Test deposit - " + file['name'], file['key'])
      tracked_deposits.append((vaultId, deposit['id']))

while(len(tracked_deposits) > 0):
  print("")
  print("Tracking " + str(len(tracked_deposits)) + " deposits:")
  deposit_statuses = {}
  for tracked_deposit in tracked_deposits:
    vaultId = tracked_deposit[0]
    depostId = tracked_deposit[1]
    deposit = get_deposit(vaultId, depostId)
    print("Deposit: " + vaultId + "/" + depostId + " - " + deposit['status'])
    if deposit['status'] == "COMPLETE":
      tracked_deposits.remove(tracked_deposit)
      restore = create_restore(vaultId, depostId, "Test restore", filestoreId + "/" + restoreDir)

  time.sleep(5)

# Carry out a large number of small deposits

for x in range(0,400):
  vault = create_vault("Test small vault " + str(x), "Automatically created small vault", vault_policy, vault_group, vault_dataset)
  vaultId = vault['id']
  print("Created vault with ID: " + vaultId)
  
  files = list_files(filestoreId)
  for file in files:
    if not file['isDirectory']:
      if file['name'] == "test_data_2MB.bin":
        print("File: " + file['key'] + " Name: " + file['name'])
        deposit = create_deposit(vaultId, "Test small deposit - " + file['name'], file['key'])

dump_info()
