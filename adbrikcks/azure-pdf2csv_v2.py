# Databricks notebook source
pip install pdfminer.six

# COMMAND ----------

import os
import re  
import csv
import math

# COMMAND ----------

container = "adept"
staccount = "bruwe60001adls"
base_folder = "UDL/Internal Sources/Manual Files/Agreements/"
landed_path = "Landed/"
converted_path = "Converted/"
key_directory = "2021-12-13-12-49-23-437b2a97-41e7-430e-85e3-666e592b94c3"
url_logging = 'https://bruwe-fs-d-60001-func-forecast.azurewebsites.net/api/QueueRequest?code=Z6wZwValDaFpWaCOT5zjela9f7Gxqs0Mg5lhxRrd2rmgRu4EzrqRnw==&command=put&key-dir=2021-12-13-12-49-23-437b2a97-41e7-430e-85e3-666e592b94c3'
msg_template = "{\"state\": $state,\"message\":\"$message\"}"
state_outer = "50"

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

def clear_paragraph(paragraph):
    delimiter = '.'
    # отделить впередистоящий не числовой символ перед параграфом
    pattern = re.compile(r'(\D){0,1}')
    result = re.match(pattern, paragraph)

    if result:
        paragraph = paragraph[result.end():]

    # удалить последний символ параграфа, если это точка
    if paragraph[len(paragraph)-1] == delimiter:
        paragraph = paragraph[:-1]    
        
    return paragraph
    
def is_chapter(chapterPrev, chapterCurr):
    heapPrev = 0
    heapCurr = 0
    pos = chapterPrev.find('.')
    if (pos > 0):
        heapPrev = int(chapterPrev[:pos])
    pos = chapterCurr.find('.')
    if (pos > 0):
        heapCurr = int(chapterCurr[:pos])
    if (heapPrev == heapCurr or heapPrev+1 == heapCurr):
        return True
    else:
        return False


# COMMAND ----------

from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from six import StringIO

def read_pdf_by_margin(source_file):
    resource_manager = PDFResourceManager()
    device = None
    pdf_text = ''
    try:
        with StringIO() as string_writer, open(source_file, 'rb') as pdf_file:
            laparams = LAParams(line_margin = 0.1)
            device = TextConverter(resource_manager, string_writer, codec='utf-8', laparams=laparams)
            interpreter = PDFPageInterpreter(resource_manager, device)

            for page in PDFPage.get_pages(pdf_file, maxpages=0):
                interpreter.process_page(page)

            pdf_text = string_writer.getvalue()
    finally:
        if device:
            device.close()
    return pdf_text
#print(read_pdf_by_margin('/dbfs/mnt/adept/UDL/Internal Sources/Manual Files/Agreements/Landed/2021-12-13-12-49-23-437b2a97-41e7-430e-85e3-666e592b94c3/Правила страхования грузов для транспортных операторов (приказ 37).pdf'))

# COMMAND ----------

import pdfminer.high_level

def read_pdf_high_level(source_file):
    pdf_text = ''
    with open(source_file, 'rb') as file:
        pdf_text = pdfminer.high_level.extract_text(file)
    return pdf_text

# COMMAND ----------

def convert_pdf(source_file):
    print("convert_pdf: {}".format(source_file))
    
    #text = read_pdf_high_level(source_file)
    text = read_pdf_by_margin(source_file)
    #print(text)
    # вырезаем номера страниц, спец.символ 0c, кавычки
    pattern = re.compile(r'(\n[0-9]+\s{0,}\n|\x0c|")')
    raw_text = re.sub(pattern, '', text)
    
    # заменяем множественные переводы строки на один перевод строки
    pattern = re.compile(r'(\n){2,}')
    raw_text = re.sub(pattern, '\n', raw_text)

    # поиск 2-х и более пробелов
    pattern = re.compile(r'( ){2,}')
    raw_text = re.sub(pattern, ' ', raw_text)
    #print (raw_text)    

    list_text = re.split('\n', raw_text)
    #print (list_text)
    
    patChapter   = re.compile(r'(.){0,1}[0-9]+[.][^0-9]')

    patParagraph = re.compile(r'(.){0,1}([0-9]+[.][0-9]+[.0-9]{0,})')

    paragrMax = 512
    paragrDict={}
    parLast = ''
    parCurr = ''
    bad_paragraph = False
    bad_count = 0
    chapter   = ''
    isChapter= False

    for line in list_text:
        #print(line)
        matchC =re.match(patChapter, line)
        #print("matchC:{}".format(matchC))
        # отделяем название разделов 
        if matchC:
            chapter = line.strip()
            isChapter = True
        else:
            matchP = re.match(patParagraph, line)
            #print("matchP:{}".format(matchP))
            # ищем параграфы
            if matchP:
                isChapter = False
                #print("matchP:{}".format(matchP))
                # в начале строки нашли номер параграфа
                parCurr = clear_paragraph(matchP.group())
                #print('parCurr:{} parLast:{}'.format(parCurr, parLast))                            
                # в справочнике номеров параграфов, считанный параграф отсутствует
                if (parCurr != parLast and  is_chapter(parLast, parCurr)):
                    bad_paragraph = False
                    bad_count = 0
                    #print("Новый параграф:{}".format(parCurr))
                    paragrDict[parCurr] = [chapter,line[matchP.end():].lstrip()]
                    parLast = parCurr
                else: # новый параграф не прошел проверку, установить флаг ошибки. Если больше 3-х ошибок парсинг прекращаем
                    if (bad_count >= 3):
                        break
                    else:
                        bad_paragraph = True
                        bad_count += 1
            else:
                # если наполняем параграф
                if (len(parLast) > 0 and not bad_paragraph):
                    if (isChapter):
                        chapter += line
                    else:
                        paragrDict[parLast][1] += line
    return paragrDict

# COMMAND ----------

def save_file(paragraphs, full_path_file):
    print("save_file: {}".format(full_path_file))
    with open(full_path_file, 'w', newline='') as f:
        fieldnames = ['point','chapter','text','result','annotation']
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        writer.writeheader()

        for key in paragraphs:
            #print(key)
            writer.writerow({fieldnames[0]: key, fieldnames[1]: paragraphs[key][0],fieldnames[2]: paragraphs[key][1], fieldnames[3]: '', fieldnames[4]: ''})

# COMMAND ----------

# MAGIC %run "./utils"

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
i = 0
for full_file_name, extension in ingest_files:
    #print(full_file_name)
    file_name = os.path.basename(full_file_name)
    try:
        if extension == 'pdf':
            paragraphs= convert_pdf(full_file_name)
            #print("paragraphs: ".format(len(paragraphs)))
            if len(paragraphs) > 0:
                i+=1
                name, extension = os.path.splitext(file_name)
                save_file(paragraphs, '/dbfs' + os.path.join(dst_path, name + dst_ext))
                put_log(url_logging, msg_template, state_outer, "Файл: {} преобразован в csv".format(file_name))
            else:
                put_log(url_logging, msg_template, state_outer, "Файл: {} не конвертирован".format(file_name))
    except Exception as ex:
        print(ex)
        put_log(url_logging, msg_template, state_outer, "Ошибка преобразования файла {}: {}".format(file_name, ex))
if (i==0):
    put_log(url_logging, msg_template, state_outer, "Данные не были преобразованы")
    raise Exception("Данные не были преобразованы")

# COMMAND ----------

#get_list_files(src_full)
#print(src_full)
