# Databricks notebook source
!pip install --upgrade pip
!pip install spacy==3.0.0
!python -m spacy download ru_core_news_sm
!pip install gensim==3.8.3

# COMMAND ----------

# Библиотики
import pandas as pd
from gensim.summarization.summarizer import summarize
from gensim.summarization import keywords
import spacy
import ru_core_news_sm
import re
import copy

# COMMAND ----------

# функция аннотации
def summary(text):
    sentenses = re.split("\. |\.\.\. ", text)
     
    for i in sentenses:
        if len(i)<2:
            sentenses.remove(i)

    if len(sentenses) > 1:
        nlp = ru_core_news_sm.load()
        S = summarize(text, ratio = 0.5 )
        if not S:
            return text
        else:
            return summarize(text, ratio = 0.4 )
    else:
        return text

# COMMAND ----------

# Это датасет для проверки работы скрипта, можно удалить 
df = pd.read_csv('пример для аннотации1.csv', delimiter=';', encoding='utf8')
df['text'][3]

# COMMAND ----------

# применяем функцию аннотирования на df c не нулевыми классами
# Аннотирование работает для текста с 2 и более предложениями
df['annotation'] = list(map(summary, df['text'].values.tolist()))

# COMMAND ----------

df['annotation']

# COMMAND ----------

!pip freeze
