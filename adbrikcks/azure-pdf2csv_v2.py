# Databricks notebook source
#%run "./utils"

# COMMAND ----------

pip install pdfminer.six

# COMMAND ----------

import os
import re  
import csv
import math
import pdfminer.high_level

# COMMAND ----------

container = "adept"
staccount = "bruwe60001adls"
base_folder = "UDL/Internal Sources/Manual Files/Agreements/"
landed_path = "Landed/"
converted_path = "Converted/"
key_directory = "2021-12-13-12-49-23-437b2a97-41e7-430e-85e3-666e592b94c3"
url_logging = 'https://bruwe-fs-d-60001-func-forecast.azurewebsites.net/api/QueueRequest?code=Z6wZwValDaFpWaCOT5zjela9f7Gxqs0Mg5lhxRrd2rmgRu4EzrqRnw==&command=put&key-dir=2021-12-13-12-49-23-437b2a97-41e7-430e-85e3-666e592b94c3'
msg_template = "{\"state\": $state,\"message\":\"$message\"}"
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
  
def is_paragraph(pointPrev, pointCurr):
    if (pointCurr > pointPrev or abs(pointCurr-pointPrev) < 693147.2):
        return True
    else: 
        return False
    
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

#convert_pdf('/dbfs/mnt/adept/UDL/Internal Sources/Manual Files/Agreements/Landed/2021-12-13-12-49-23-437b2a97-41e7-430e-85e3-666e592b94c3/Правила страхования грузов Зетта.pdf')
#with open("/dbfs/mnt/adept/UDL/Internal Sources/Manual Files/Agreements/Гайде_Правила_страхования_грузов.pdf", 'r') as f:
#  print('Ok')

# COMMAND ----------

def convert_pdf(source_file):
    print("convert_pdf: {}".format(source_file))
    with open(source_file, 'rb') as file:
        text = pdfminer.high_level.extract_text(file)

    raw_text = text.replace('\x0c','')
    
    # вырезаем двойные кавычки внутри текста, чтобы не создавались неучтенные столбцы
    raw_text = text.replace('"','')
    
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

    patParagraph = re.compile(r'(.){0,1}([0-9]+[.][0-9]+[.0-9]{0,})')
    # Описание шаблона поиска номера параграфа в начале строки документа
    #(.){0,1} пропуск пробела, если он присутствует в начале строки
    # запоминаемая последовательность в виде группы символов
    #([0-9]+      любая цифра любое количество раз
    #[.]          обязательная точка
    #[0-9]+       любая цифра любое количество раз
    #[.0-9]{0,})  точка или цифра от 0 до любого количества раз

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
                #print('parCurr:{} parEqCurr:{} parLast:{} parEqLast:{}'.format(parCurr, parEqCurr, parLast, parEqLast))
                # Новый параграф отличается от предыдущего И не более, чем на заданную величину
                if (parCurr != parLast and is_paragraph(parEqLast, parEqCurr) and is_chapter(parLast, parCurr)):
                    #print("Новый параграф:{}".format(parCurr))
                    paragrDict[parCurr] = line[matchP.end():].lstrip()
                    parLast = parCurr
                    parEqLast = parEqCurr
                #else:
                #    paragrDict[parLast] += line
            else:
                # в начале строки НЕ нашли номер параграфа
                #if (parLast in paragrDict):
                if (len(parLast) > 0):
                    #str_len = len(paragrDict.get(parLast))
                    paragrDict[parLast] += line
    return paragrDict

# COMMAND ----------

def convert_pdf_ex(source_file):
    print("convert_pdf: {}".format(source_file))
    with open(source_file, 'rb') as file:
        text = pdfminer.high_level.extract_text(file)

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
    parEqLast = 0
    #parEqCurr = 0
    bad_paragraph = False
    bad_count = 0

    for line in list_text:
        #print(line)
        matchC =re.match(patChapter, line)
        #print("matchC:{}".format(matchC))
        # отделяем название разделов 
        if not matchC:
            matchP = re.match(patParagraph, line)
            #print("matchP:{}".format(matchP))
            # ищем параграфы
            if matchP:
                #print("matchP:{}".format(matchP))
                # в начале строки нашли номер параграфа
                parCurr = clear_paragraph(matchP.group())
                #parEqCurr = get_paragrapf_eq(parCurr)
                #print('parCurr:{} parEqCurr:{} parLast:{} parEqLast:{}'.format(parCurr, parEqCurr, parLast, parEqLast))                            
                # в справочнике номеров параграфов, считанный параграф отсутствует
                #if (parEqCurr >= parEqLast and parCurr != parLast): #is_paragraph(parEqLast, parEqCurr) and
                if (parCurr != parLast and  is_chapter(parLast, parCurr)):
                    bad_paragraph = False
                    bad_count = 0
                    #print("Новый параграф:{}".format(parCurr))
                    paragrDict[parCurr] = line[matchP.end():].lstrip()
                    parLast = parCurr
                    #parEqLast = parEqCurr
                else: # новый параграф не прошел проверку, установить флаг ошибки. Если больше 3-х ошибок парсинг прекращаем
                    if (bad_count >= 3):
                        break
                    else:
                        bad_paragraph = True
                        bad_count += 1

            # в начале строки НЕ нашли номер параграфа
            else:
                # если наполняем параграф
                if (len(parLast) > 0 and not bad_paragraph):
                    paragrDict[parLast] += line
                    #print("paragraph:{}".format(paragrDict[parLast]))
    return paragrDict

# COMMAND ----------

def save_file(paragraphs, full_path_file):
    #print("save_file: {}".format(full_path_file))
    with open(full_path_file, 'w', newline='') as f:
        fieldnames = ['point','text','result','annotation']
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        writer.writeheader()

        for key in paragraphs:
            writer.writerow({fieldnames[0]: key, fieldnames[1]: paragraphs[key], fieldnames[2]: '', fieldnames[3]: ''})

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
            paragraphs= convert_pdf_ex(full_file_name)
            #print("paragraphs: ".format(len(paragraphs)))
            if len(paragraphs) > 0:
                #file_name = os.path.basename(full_file_name)
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
