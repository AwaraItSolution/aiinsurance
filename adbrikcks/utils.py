# Databricks notebook source
def get_dir_content(ls_path):
    dir_paths = dbutils.fs.ls(ls_path)
    return [ [p.path.replace('dbfs:','/dbfs'), os.path.splitext(p.path)[1].replace('.','')] 
                for p in dir_paths 
                    if p.isFile()] 

def exists(path):
    try:
        dbutils.fs.ls()
        return True
    except:
        return False
    
def createIfNotExists(path):
    if (not exists(path)):
        dbutils.fs.mkdirs(path)

# COMMAND ----------

## define path and mount to cluster
## Обратить внимание:
# 1. pointer_folder - следует формировать с учетом полного пути к папке с учетом родительских подпапок
# 2. Если по пути монтирования уже есть папка, которая смонтирована с другим хранилищем, то сначала нужно отмонтировать старое хранилище. Например, к папке rawdata был примонтировано Blob Storage, затем эту же папку хотим примонтировать к Data Lake хранилищу.
def define_path_and_mount(container, staccount):
    print("define_path_and_mount: container-{}, staccount-{}".format(container, staccount))
    sp_clientId = "465f0038-39af-4f0c-9e40-8dbfbd99936f"
    sp_tenantId = "72162faa-c4d3-4ed6-89bd-a37642170063"
    db_scope_name = "scope-adept"
    db_keyvault_name = "secret-adept-4-adls-databricks"
    db_endpoint = "https://login.microsoftonline.com/{}/oauth2/token".format(sp_tenantId)
    
    uri_adls = "abfss://{}@{}.dfs.core.windows.net/".format(container, staccount)    
    pointer_folder = "/mnt/{}/".format(container)

    configs = {"fs.azure.account.auth.type": "OAuth",
              "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
              "fs.azure.account.oauth2.client.id": sp_clientId,
              "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope=db_scope_name,key=db_keyvault_name),
              "fs.azure.account.oauth2.client.endpoint": db_endpoint}
    #print(configs)
    # Optionally, you can add <directory-name> to the source URI of your mount point.
    try:
        dbutils.fs.mount(
        source = uri_adls,
        mount_point = pointer_folder,
        extra_configs = configs)

        print ('The mount point folder is mounted.')
    except:
        print ('The mount point folder is mounted yet.')

    return pointer_folder

def put_log(url, msg_template, state, message):
    msg = msg_template.replace("$state", state)
    msg = msg.replace("$message", message)
    print("put_log url:{}, message:{}".format(url, msg))
    params = {'message':msg}
    try:
        r = requests.post(url, params=params)
        print(r.status_code, r.reason)
    except Exception as ex:
        print(ex)

# COMMAND ----------

import requests

def put_log(url, msg_template, state, message):
    msg = msg_template.replace("$state", state)
    msg = msg.replace("$message", message)
    print("put_log url:{}, message:{}".format(url, msg))
    params = {'message':msg}
    try:
        r = requests.post(url, params=params)
        print(r.status_code, r.reason)
    except Exception as ex:
        print(ex)
    
#url = 'https://fn-upload-file-to-adls.azurewebsites.net/api/QueueRequest?code=DEbXSIGQF1WT9HYB8epmymzw5USPFDK5/kbvi1ph4vbx9Ww60y6y2w==&command=put&key-dir=2021-12-13-12-49-23-437b2a97-41e7-430e-85e3-666e592b94c3'
#put_log(url, 'Message From ADB')
