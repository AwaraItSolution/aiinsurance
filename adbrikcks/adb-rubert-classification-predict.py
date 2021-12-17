# Databricks notebook source
# MAGIC %run "./utils"

# COMMAND ----------

import logging
logging.getLogger("py4j").setLevel(logging.INFO)
logging.getLogger('pyspark').setLevel(logging.ERROR)
logger = logging.getLogger('pyspark')

# COMMAND ----------

!pip install mlflow
#!pip install --upgrade pip
!pip install transformers==4.12.5
!pip install simpletransformers==0.63.3
!pip install tensorboardX==2.4
#!pip install torch==1.7.1+cu110
!pip install tensorflow==2.6.2
!pip install torch==1.7.1

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
files_total = len(input_files)
files_pross = 0
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
            
            put_log(url_logging, msg_template, state_outer, "Прогноз выполнен {}".format(file_name))
            files_pross += 1
    except Exception as ex:
        print(ex)
        put_log(url_logging, msg_template, state_outer, "Ошибка прогнозирования {}: {}".format(file_name, ex))

if (files_pross == 0):
    raise Exception('Отсутствуют файлы для прогнозирования')

# COMMAND ----------

#print(src_path)
#dbutils.fs.ls('/mnt/adept/UDL/Internal Sources/Manual Files/Agreements/Models/')
#client = MlflowClient()
