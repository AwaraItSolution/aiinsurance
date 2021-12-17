# Databricks notebook source
pip install pdfminer.six

# COMMAND ----------

import os
import re
import csv
import math
import pdfminer.high_level
import requests

# COMMAND ----------

container = "adept"
staccount = "bruweadls001"
base_folder = "UDL/Internal Sources/Manual Files/Agreements/"
landed_path = "Landed/"
converted_path = "Converted/"
key_directory = "2021-12-13-12-49-23-437b2a97-41e7-430e-85e3-666e592b94c3"
url_logging = 'https://fn-upload-file-to-adls.azurewebsites.net/api/QueueRequest?code=DEbXSIGQF1WT9HYB8epmymzw5USPFDK5/kbvi1ph4vbx9Ww60y6y2w==&command=put&key-dir=2021-12-13-12-49-23-437b2a97-41e7-430e-85e3-666e592b94c3'
msg_template = "{\"state\": \"$state\",\"message\":\"$message\"}"
state_outer = "40"

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
    landed_path = dbutils.widgets.get("subPathLanded")
except:
    pass

try:
    converted_path = dbutils.widgets.get("subPathConverted")
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

#print(converted_folder)
#dbutils.fs.ls(converted_folder)
#dbutils.fs.ls("/mnt/adept/UDL/Internal Sources/Manual Files/Agreements/Landed/")
#/mnt/UDL/Internal Sources/Manual Files/Agreements/

# COMMAND ----------

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

# COMMAND ----------

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

#dbutils.fs.unmount('/mnt/adept')

# COMMAND ----------

#adept / UDL / Internal Sources / Manual Files / Agreements
#dbutils.fs.ls('/mnt/adept')
#dbutils.fs.ls('/mnt/adept/UDL/Internal Sources/Manual Files/Agreements/')

# COMMAND ----------

def get_dir_content(ls_path):
    dir_paths = dbutils.fs.ls(ls_path)
    return [ [p.path.replace('dbfs:','/dbfs'), os.path.splitext(p.path)[1].replace('.','')] 
                for p in dir_paths 
                    if p.isFile()]  

def clear_paragraph(paragraph):
    delimiter = '.'
    # отделить впередистоящий не числовой символ перед параграфом
    pattern = re.compile(r'(\D){0,1}')
    result = re.match(pattern, paragraph)

    if result:
        paragraph = paragraph[result.end():]

    # удалить последний симвой параграфа, если это точка
    if paragraph[len(paragraph)-1] == delimiter:
        paragraph = paragraph[:-1]    
        
    return paragraph

def get_paragrapf_eq(text):
    # расчитать числовой эквивалент параграфа
    sum_eq = 0
    offset = 6
    delimiter = '.'
    
    if len(text) > 2:
        #if text[len(text)-1] == delimiter:
        #    text = text[:-1]

        par_list = text.split(delimiter)

        for item in par_list:
            sum_eq += math.log(int(item), math.e) * 100**(offset-1)
            offset -= 1

    return sum_eq

# COMMAND ----------

#convert_pdf('/dbfs/mnt/adept/UDL/Internal Sources/Manual Files/Agreements/Гайде_Правила_страхования_грузов.pdf')
#with open("/dbfs/mnt/adept/UDL/Internal Sources/Manual Files/Agreements/Гайде_Правила_страхования_грузов.pdf", 'r') as f:
#  print('Ok')

# COMMAND ----------

def convert_pdf(source_file):
    print("convert_pdf: {}".format(source_file))
    with open(source_file, 'rb') as file:
        text = pdfminer.high_level.extract_text(file)

    raw_text = text.replace('\x0c','')
    # вырезаем номера страниц
    pattern = re.compile(r'(\n[0-9]+\n)')
    raw_text = re.sub(pattern, '', raw_text)

    pattern = re.compile(r'(\n+){1,}')
    raw_text = re.sub(pattern, '\n', raw_text)

    # поиск 2-х и более пробелов, переводов строк
    pattern = re.compile(r'( ){2,}')
    raw_text = re.sub(pattern, ' ', raw_text)
    #print (raw_text)    

    list_text = re.split('\n', raw_text)

    patChapter   = re.compile(r'(.){0,1}[0-9]+[.][^0-9]')

    # этот шаблон требует точки в конче параграфа!!!
    #patParagraph = re.compile(r'(.){0,1}([0-9]+[.]){2,}')

    # этот шаблон позволяет читать параграфы у которых нет в конце точки, например 7.5 добавлено {0,1}
    # НО из-за этого могут появиться другие проблемы. 
    # Требуется сделать регулярку, которая бы игнорировала отсутствие ТОЛЬКО последней точки!!!
    patParagraph = re.compile(r'(.){0,1}([0-9]+[.]{0,1}){2,}')

    paragrMax = 512
    paragrDict= {}
    parLast = ''
    parCurr = ''
    parEqLast = 0
    parEqCurr = 0

    for line in list_text:
        #print(line)
        matchC =re.match(patChapter, line)
        # отделяем название разделов 
        if not matchC:
            matchP = re.match(patParagraph, line)
            # ищем параграфы
            if matchP:
                #print(matchP)
                # в начале строки нашли номер параграфа
                parCurr = clear_paragraph(matchP.group())
                parEqCurr = get_paragrapf_eq(parCurr)
                #print('paragrapf:{}\tcurrent:{}\tlast:{}'.format(parCurr, parEqCurr, parEqLast))                            
                #print(parCurr)
                # в справочнике номеров параграфов, считанный параграф отсутствует
                if (parEqCurr >= parEqLast and parCurr != parLast):
     #               print(parCurr)
                    paragrDict[parCurr] = line[matchP.end():].lstrip()
                    parLast = parCurr
                    parEqLast = parEqCurr

                else:
                    parLast = ''

                # в начале строки НЕ нашли номер параграфа
            else:
                # если наполняем параграф
                if (parLast in paragrDict):
                    #str_len = len(paragrDict.get(parLast))
                    #if (str_len > 0 & str_len < paragrMax):
                    paragrDict[parLast] += line
    return paragrDict

# COMMAND ----------

def save_file(paragraphs, full_path_file):
    print("save_file: {}".format(full_path_file))
    with open(full_path_file, 'w', newline='') as f:
        fieldnames = ['point','text','result','annotation']
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        writer.writeheader()

        for key in paragraphs:
            writer.writerow({fieldnames[0]: key, fieldnames[1]: paragraphs[key], fieldnames[2]: '', fieldnames[3]: ''})

# COMMAND ----------

print("Main processing")
base_path = define_path_and_mount(container, staccount)
src_path = os.path.join(base_path, base_folder, landed_path, key_directory) 
dst_path = os.path.join(base_path, base_folder, converted_path, key_directory)
print(base_path)
print(src_path)
print(dst_path)
createIfNotExists(dst_path)
dst_ext  = '.csv'

ingest_files = get_dir_content(src_path)
print(ingest_files)
for full_file_name, extension in ingest_files:
    #print(full_file_name)
    file_name = os.path.basename(full_file_name)
    try:
        if extension == 'pdf':
            paragraphs= convert_pdf(full_file_name)
            if len(paragraphs) > 0:
                #file_name = os.path.basename(full_file_name)
                name, extension = os.path.splitext(file_name)
                save_file(paragraphs, '/dbfs' + os.path.join(dst_path, name + dst_ext))
                put_log(url_logging, msg_template, state_outer, "Файл: {} преобразован в csv".format(file_name))
    except Exception as ex:
        print(ex)
        put_log(url_logging, msg_template, state_outer, "Ошибка преобразования файла {}: {}".format(file_name, ex))

# COMMAND ----------

#get_list_files(src_full)
#print(src_full)
