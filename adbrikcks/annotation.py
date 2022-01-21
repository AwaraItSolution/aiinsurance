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
def summary(row):
    text = ""
    if(row[0] != 0):
        text = row[1]
        
        #print(text)
        
        sentenses = re.split("\. |\.\.\. ", text)
        #print(len(sentenses))
        for i in sentenses:
            #print(i)
            if len(i)<2:
                #print('if len(i)<2:)')
                sentenses.remove(i)

        if len(sentenses) > 1:
            #print('before load')
            nlp = ru_core_news_sm.load()
            #print('before S')
            S = summarize(text, ratio = 0.5 )
            #print(S)
            if S:
                text = summarize(text, ratio = 0.4 )
    #print('')
    return text

# COMMAND ----------

def annotate(df):
    #print(df[['result', 'text']].apply(summary, axis=1))
    return df[['result', 'text']].apply(summary, axis=1)

# COMMAND ----------

# не работает на этом тексте
#text =  "Помимо возмещения убытка в связи с повреждением или гибелью груза, произошедшего в результате наступления событий, указанных в пп.3.5.1-3.5.3 настоящих Правил страхования, Страхователю возмещаются все #необходимые и целесообразно произведенные расходы по спасанию и уменьшению убытка, но не более 10 (Десять) % от стоимости груза.   "
#summary([1,text])
#summarize(text, ratio = 0.5 )

#sentenses = re.split("\. |\.\.\. ", text)
#print(len(sentenses))
#for i in sentenses:
#    print('into text')
#    print(len(i))
#    if len(i)<2:
#        sentenses.remove(i)


# COMMAND ----------

#df = pd.DataFrame({
#  "text":["Aaaaaa", "Bbbbbbbbb", "Cccccccccccc", "Ddddddddddddd", "Eeeeeeeee"], 
#  "result":[0, 1, 0, 1, 0], 
#  "annotation":["", "", "", "", ""]})
#print(df)
#df['annotation'] = df[['result', 'text']].apply(summarize, axis=1)
#df['result']==1
#df_4annotation = df[df['result']==1]['text']
#df_4annotation.index
#df.loc[df_4annotation.index]
#df.loc[df_4annotation.index, 'annotation'] = ['text1', 'text2']
#df['annotation'] = annotate(df)

#annotate(df[["result", "text"]].values.tolist())
#df
