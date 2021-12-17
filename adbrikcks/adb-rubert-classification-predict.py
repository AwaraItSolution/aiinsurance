# Databricks notebook source
import logging
logging.getLogger("py4j").setLevel(logging.INFO)
logging.getLogger('pyspark').setLevel(logging.ERROR)
logger = logging.getLogger('pyspark')

# COMMAND ----------

!pip install mlflow

# COMMAND ----------

!pip install --upgrade pip
!pip install transformers==4.12.5
!pip install simpletransformers==0.63.3
!pip install tensorboardX==2.4
#!pip install torch==1.7.1+cu110
!pip install tensorflow==2.6.2

# COMMAND ----------

!pip install torch==1.7.1

# COMMAND ----------

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

container = "adept"
staccount = "bruweadls001"
base_folder = "UDL/Internal Sources/Manual Files/Agreements/"
converted_path = "Converted/"
processed_path = "Processed/"
key_directory  = "2021-12-13-12-49-23-437b2a97-41e7-430e-85e3-666e592b94c3"
url_logging = 'https://fn-upload-file-to-adls.azurewebsites.net/api/QueueRequest?code=DEbXSIGQF1WT9HYB8epmymzw5USPFDK5/kbvi1ph4vbx9Ww60y6y2w==&command=put&key-dir=2021-12-13-12-49-23-437b2a97-41e7-430e-85e3-666e592b94c3'
msg_template = "{\"state\": \"$state\",\"message\":\"$message\"}"
state_outer = "55"

try:
    container = dbutils.widgets.get("container")
except:
    pass
try:
    staccount = dbutils.widgets.get("storage")
except:
    pass
try:
    base_folder = dbutils.widgets.get("basePath")
except:
    pass
try:
    converted_path = dbutils.widgets.get("subPathConverted")
except:
    pass
try:
    processed_path = dbutils.widgets.get("subPathProcessed")
except:
    pass
try:
    key_directory = dbutils.widgets.get("keyDirectory")
except:
    pass
try:
    url_logging = dbutils.widgets.get("urlLogging")
except:
    pass
try:
    msg_template = dbutils.widgets.get("msgTemplate")
except:
    pass
try:
    state_outer = dbutils.widgets.get("stateOuter")
except:
    pass

# COMMAND ----------

import mlflow.tensorflow
import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient
import os
from simpletransformers.classification import ClassificationModel, ClassificationArgs
import pandas as pd
import numpy as np
from sklearn.metrics import f1_score, accuracy_score, roc_auc_score
import requests
#import my_utils

print("Main forecasting")
base_path = define_path_and_mount(container, staccount)
src_path = os.path.join(base_path, base_folder, converted_path, key_directory) 
dst_path = os.path.join(base_path, base_folder, processed_path, key_directory)
print(base_path)
print(src_path)
print(dst_path)
createIfNotExists(dst_path)

#model_test = ClassificationModel('bert', 'https://github.com/AwaraItSolution/ADBricks-MLFlows', use_cuda = False) # использование CPU
model_test = ClassificationModel('bert', 'SvyatoslavA/model_awara_text', use_cuda = False) # использование GPU
    

# COMMAND ----------

input_files = get_dir_content(src_path)
print(input_files)
for full_file_name, extension in input_files:
    #print(full_file_name)
    file_name = os.path.basename(full_file_name)
    try:
        if extension == 'csv':
            df = pd.read_csv(full_file_name, quotechar='"')
            # удаляем строки, содержащие null в столбце 'text'
            df.drop(index=df.loc[df['text'].isna()].index.tolist(), inplace=True)
        
            test_text = df['text'].values.tolist()
            predictions = model_test.predict(test_text)
            df['result'] = predictions[0]

            path_out = '/dbfs' + os.path.join(dst_path, file_name)
            df.to_csv(path_out, index=False, quotechar='"')
            
            put_log(url_logging, msg_template, state_outer, "Файл: {} прогноз выполнен".format(file_name))
    except Exception as ex:
        print(ex)
        put_log(url_logging, msg_template, state_outer, "Ошибка прогнозирования файла {}: {}".format(file_name, ex))

# COMMAND ----------

#print(src_path)
#dbutils.fs.ls('/mnt/adept/UDL/Internal Sources/Manual Files/Agreements/Models/')

# COMMAND ----------

#d = {'point': ['1.1','1.2'], 'text': ["Настоящие правила регулируют отношения между АО «СК «ПАРИ» (далее - Страховщик) и юридическими или дееспособными физическими лицами (далее - Страхователи) при страховании #воздушным, морским, речным грузов, перевозимых транспортом. автомобильным, железнодорожным, ",
#                                      "По договору страхования Страховщик обязуется за обусловленную договором страхования плату (страховую премию) при наступлении предусмотренного в договоре страхования события #(страхового случая) возместить Страхователю или иному лицу, в пользу которого заключен договор страхования события убытки в застрахованном грузе (выплатить страховое возмещение) в пределах обусловленной договором #страхования суммы (страховой суммы). (Выгодоприобретателю), причиненные вследствие этого "],
#     'result': ['',''], 'annotation': ['','']}

#df = pd.DataFrame(data=d)

# COMMAND ----------

#test_text = df['text'].values.tolist()
#predictions = model_test.predict(test_text)
#df['result'] = predictions[0]
#path_out = os.path.join(data_path_out, filename)
#print(df)

# COMMAND ----------

#client = MlflowClient()
import './aiutils'

#%run "/Users/evgeny.popovich@awara-it.com/aiutils"
#%run "./aiutils"

# COMMAND ----------

path = define_path_and_mount(container, staccount)
print(path)
