# Databricks notebook source
!pip install --upgrade pip
!pip install mlflow
!pip install transformers==4.12.5
!pip install simpletransformers==0.63.3
!pip install tensorboardX==2.4
!pip install tensorflow==2.6.2
!pip install torch==1.7.1
#!pip install torch==1.7.1+cu110

# COMMAND ----------

import logging
logging.getLogger("py4j").setLevel(logging.INFO)
logging.getLogger('pyspark').setLevel(logging.ERROR)
logger = logging.getLogger('pyspark')

# COMMAND ----------

container = "adept"
staccount = "bruwe60001adls"
base_folder = "UDL/Internal Sources/Manual Files/Agreements/"
converted_path = "Converted/"
processed_path = "Processed/"
key_directory  = "2021-12-13-12-49-23-437b2a97-41e7-430e-85e3-666e592b94c3"
url_logging = 'https://bruwe-fs-d-60001-func-forecast.azurewebsites.net/api/QueueRequest?code=Z6wZwValDaFpWaCOT5zjela9f7Gxqs0Mg5lhxRrd2rmgRu4EzrqRnw==&command=put&key-dir=2021-12-13-12-49-23-437b2a97-41e7-430e-85e3-666e592b94c3'
msg_template = "{\"state\": $state,\"message\":\"$message\"}"
state_outer = "60"

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

# MAGIC %run "./utils"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Загрузка модели

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

#%run "./annotation"

# COMMAND ----------

input_files = get_dir_content(src_path)
print(input_files)
files_total = len(input_files)
files_pross = 0
for full_file_name, extension in input_files:
    print(full_file_name)
    file_name = os.path.basename(full_file_name)
    try:
        if extension == 'csv':
            df = pd.read_csv(full_file_name, quotechar='"')
            # удаляем строки, содержащие null в столбце 'text'
            df.drop(index=df.loc[df['text'].isna()].index.tolist(), inplace=True)
            # если у параграфа отсутствует наименование раздела, вставить пробел
            df.loc[df['chapter'].isna(),'chapter']=' '
            
            test_text = df['text'].values.tolist()
            predictions = model_test.predict(test_text)
            
            df['result'] = predictions[0]
            #df['annotation'] = annotate(df)
            
            path_out = '/dbfs' + os.path.join(dst_path, file_name)
            df.to_csv(path_out, index=True, index_label='id', quotechar='"')
            
            put_log(url_logging, msg_template, state_outer, "Прогноз выполнен: {}".format(file_name))
            files_pross += 1
        else:
            put_log(url_logging, msg_template, state_outer, "Файл пропущен: {}".format(file_name))
    except Exception as ex:
        print(ex)
        put_log(url_logging, msg_template, state_outer, "Ошибка прогнозирования: {}: {}".format(file_name, ex))

if (files_pross == 0):
    put_log(url_logging, msg_template, state_outer, "Отсутствуют данные для прогнозирования")
    raise Exception("Отсутствуют данные для прогнозирования")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Генерация одного файла результатов
# MAGIC 1. Создаем zip архив во временной папке на локальном диске /tmp, т.к. в смонтированный диск пишут одновременно ноды всего кластера, а библиотека zipfile не поддерживает параллельную запись
# MAGIC 2. Созданный архив копируем из локальной папки в примонтированную папку озера dst_path с обработанными файлами

# COMMAND ----------

import zipfile
import os
path_zip = os.path.join('/tmp', key_directory) + '.zip'
print(path_zip)
with zipfile.ZipFile(path_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
    path_data_dbfs = '/dbfs' + dst_path
    for root, dirs, files in os.walk(path_data_dbfs):
        for file in files:
            fullpath = os.path.join(path_data_dbfs, file)
            if (not zipfile.is_zipfile(fullpath)):
                print("archived: {}".format(fullpath))
                zipf.write(filename=fullpath, arcname=os.path.relpath(fullpath, os.path.join(path_data_dbfs, '..')))
                put_log(url_logging, msg_template, state_outer, "{} добавлен к архиву".format(file))
dbutils.fs.cp("file:{}".format(path_zip), dst_path)
