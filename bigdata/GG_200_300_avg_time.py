from xml.etree.cElementTree import ElementTree as ET
from functools import reduce
from typing import Counter
import os
import operator
from datetime import datetime

def chunkify(iterable,len_of_chunk):
    for i in range(0,len(iterable),len_of_chunk):
        yield iterable[i:i + len_of_chunk]

current_folder=os.getcwd()
tree=ET()
tree.parse(f'{current_folder}/112010 Stack Overflow/posts.xml')
root=tree.getroot()
data_chunks=chunkify(root,50)

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

def mapeado(data):    
    post_mapeados= list(map(get_needed_titles, data))
    return post_mapeados

mapped_list=list(map(mapeado,data_chunks))

def unify_chunks(data1,data2):
    for tupla in data2:
        data1.append(tupla)
    return data1

unified_chunks=reduce(unify_chunks,mapped_list)

def set_int_tuple(unified_chunks):
    lista_aux =[]
    for dato in unified_chunks:
        lista_aux.append((int(dato[0]),int(dato[1])))
    return lista_aux

int_list=set_int_tuple(unified_chunks)

def order_list(unified_chunks):
    ordered_list=sorted(unified_chunks, key=operator.itemgetter(0), reverse=True)
    return ordered_list

ordered_list=order_list(int_list)
top_200_300=ordered_list[200:300]

avg_time=sum(tup[1] for tup in top_200_300)/100

print(avg_time,' hours.')