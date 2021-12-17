# Databricks notebook source
pip install pdfminer.six

# COMMAND ----------

import os
import re
import csv
import math
import pdfminer.high_level
import pandas as pd
from shutil import copyfile
from os.path import isfile, isdir, join

# COMMAND ----------

# test
## define path and mount to cluster
## Обратить внимание:
# 1. pointer_folder - следует формировать с учетом полного пути к папке с учетом родительских подпапок
# 2. Если по пути монтирования уже есть папка, которая смонтирована с другим хранилищем, то сначала нужно отмонтировать старое хранилище. Например, к папке rawdata был примонтировано Blob Storage, затем эту же папку хотим примонтировать к Data Lake хранилищу.
def define_path_and_mount():
    sp_clientId = "465f0038-39af-4f0c-9e40-8dbfbd99936f"
    sp_tenantId = "72162faa-c4d3-4ed6-89bd-a37642170063"
    db_scope_name = "scope-adept"
    db_keyvault_name = "secret-adept-4-adls-databricks"
    db_endpoint = "https://login.microsoftonline.com/{}/oauth2/token".format(sp_tenantId)

    container = "adept"
    staccount = "bruweadls001"
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

    pointer_folder += 'UDL/Internal Sources/Manual Files/Agreements/'
    
    return pointer_folder

# COMMAND ----------

print(define_path_and_mount())

# COMMAND ----------

#dbutils.fs.unmount('/mnt/adept')

# COMMAND ----------

#adept / UDL / Internal Sources / Manual Files / Agreements
#dbutils.fs.ls('/mnt/adept')
dbutils.fs.ls('/mnt/adept/UDL/Internal Sources/Manual Files/Agreements/')

# COMMAND ----------

def get_dir_content(ls_path):
    dir_paths = os.listdir(ls_path)
    return [ [join(ls_path, p), os.path.splitext(p)[1].replace('.','')] 
                for p in dir_paths 
                    if isfile(join(ls_path, p))]  

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

def convert_pdf(source_file):
    print(source_file)
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

    with open(full_path_file, 'w', newline='') as f:
        fieldnames = ['point','text','result']
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        writer.writeheader()

        for key in paragraphs:
            writer.writerow({fieldnames[0]: key, fieldnames[1]: paragraphs[key], fieldnames[2]: ''})

# COMMAND ----------

src_path = define_path_and_mount()
src_mask = '*.pdf'
src_full = src_path + src_mask
arc_path = os.path.join(src_path, 'Archive')
dst_path = os.path.join(src_path, 'Converted')
dst_ext  = '.csv'

ingest_files = get_dir_content(src_path)
print(ingest_files)
for full_file_name, extension in ingest_files:
    try:
        if extension == 'pdf':
            paragraphs= convert_pdf(full_file_name)
            if len(paragraphs) > 0:
                file_name = os.path.basename(full_file_name)
                name, extension = os.path.splitext(file_name)

                save_file(paragraphs, os.path.join(dst_path, name + dst_ext))

            copyfile(full_file_name, os.path.join(arc_path, file_name))
            #move(full_file_name, os.path.join(arc_path, file_name ))
    except:
        print('Что-то пошло не так')

# COMMAND ----------

get_list_files(src_full)
print(src_full)

# COMMAND ----------

def get_dir_content_recurse(ls_path):
    dir_paths = dbutils.fs.ls(ls_path)
    subdir_paths = [get_dir_content(p.path) for p in dir_paths if p.isDir() and p.path != ls_path]
    flat_subdir_paths = [p for subdir in subdir_paths for p in subdir]
    return list(map(lambda p: p.path, dir_paths)) + flat_subdir_paths
    
def get_dir_content(ls_path):
    files = []
    dir_paths = dbutils.fs.ls(ls_path)
    for p in dir_paths:
        if p.isFile():
            files.append(p.name)

    return files

paths = get_dir_content('/mnt/adept/UDL/Internal Sources/Manual Files/Agreements/')
[print(p) for p in paths]
paths = get_dir_content_recurse('/mnt/adept/UDL/Internal Sources/Manual Files/Agreements/')
[print(p) for p in paths]
