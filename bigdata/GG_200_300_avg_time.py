from xml.etree.cElementTree import ElementTree as ET
from functools import reduce
from typing import Counter
import os
import operator
from datetime import datetime, timedelta
from numpy import average
import pandas as pd

def chunkify(iterable,len_of_chunk):
    for i in range(0,len(iterable),len_of_chunk):
        yield iterable[i:i + len_of_chunk]

current_folder=os.getcwd()
tree=ET()
tree.parse(f'{current_folder}/bigdata/posts.xml')
root=tree.getroot()
data_chunks1=chunkify(root,50)
data_chunks2=chunkify(root,50)

date_format='%Y-%m-%dT%H:%M:%S.%f'

def get_needed_titles(data):
    score=data.attrib['Score']
    int_score=int(score)
    creation_date=data.attrib['CreationDate']   #"2009-03-11T12:51:01.480"
    datefm_creation_Date=datetime.strptime(creation_date,date_format)
    last_activity=data.attrib['LastActivityDate']
    datefm_last_activity_Date=datetime.strptime(last_activity,date_format)
    datedif=(((datefm_last_activity_Date-datefm_creation_Date).seconds)/3600)
    int_datedif=int(datedif)
    #print(datedif)
    return  int_score,int_datedif

def question_posts(data):
    score=data.attrib['Score']
    int_score=int(score)
    post_id=data.attrib['Id']
    post_type_id=int(data.attrib['PostTypeId'])
    creation_date=data.attrib['CreationDate']
    try:
        accepted_answ_id=data.attrib['AcceptedAnswerId']
    except:
        return
    return  accepted_answ_id,int_score,post_id,creation_date,post_type_id

def answer_posts(data):
    post_id=data.attrib['Id']
    post_type_id=int(data.attrib['PostTypeId'])
    creation_date=data.attrib['CreationDate']
    return  post_id,creation_date,post_type_id

def prueba(data):
    if data[4]=='1':
        return data[4]
    else:
        return

def mapeado_question(data):    
    post_mapeados= list(map(question_posts, data))
    post_mapeados=list(filter(None,post_mapeados))
    post_mapeados=list(filter(lambda x:x[4]==1,post_mapeados))
    return list(post_mapeados)

def mapeado_answer(data):    
    answer_mapeado= list(map(answer_posts, data))
    answer_mapeado=filter(None,answer_mapeado)
    answer_mapeado=list(filter(lambda x:x[2]==2,answer_mapeado))
    return list(answer_mapeado)

question_mapped=list(map(mapeado_question,data_chunks1))
answer_mapped=list(map(mapeado_answer,data_chunks2))

def unify(data1,data2):
    for tupla in data2:
        data1.append(tupla)
    return data1

question_unified=reduce(unify,question_mapped)
answer_unified=reduce(unify,answer_mapped)

def merged(question,answer):
    id = operator.itemgetter(0)
    answ_info = {id(post_id): post_id[1:] for post_id in answer}
    merged = [quest_id + answ_info[id(quest_id)] for quest_id in question if id(quest_id) in answ_info]
    return merged

merged_list=merged(question_unified,answer_unified)

def order_list(data):
    ordered_list=sorted(data, key=operator.itemgetter(1), reverse=True)
    return ordered_list

ordered_merged_list=order_list(merged_list)
top_200_300_first=ordered_merged_list[200:300]

question_dates_string=[x[3] for x in top_200_300_first]

def set_date(data):
    lista_aux =[]
    for dato in data:
        correct_Date=datetime.strptime(dato,date_format)
        lista_aux.append(correct_Date)
    return lista_aux

answer_dates_string=[x[5] for x in top_200_300_first]

question_dates = [datetime.strptime(date, date_format) for date in question_dates_string]

answer_dates=[datetime.strptime(date, date_format) for date in answer_dates_string]

operator_dif=list(map(operator.sub,answer_dates,question_dates))

quest=list(pd.to_datetime(question_dates_string))
answ=list(pd.to_datetime(answer_dates_string))

pandas_dif=list(map(operator.sub,answ,quest))


total_hours=(sum(operator_dif,timedelta()))
total_seconds=(total_hours.total_seconds())
average_response_time_in_hours=(total_seconds/100)/3600

print(type(total_hours))
print(total_hours.total_seconds())
print(total_hours.days, total_hours.seconds//3600, (total_hours.seconds//60)%60)
print(total_hours)
print('Average response time',average_response_time_in_hours,' hours')