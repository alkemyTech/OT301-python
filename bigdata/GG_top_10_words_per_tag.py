from xml.etree.cElementTree import ElementTree as ET
from functools import reduce
from typing import Counter
import re
import os
import operator

def chunkify(iterable,len_of_chunk):
    for i in range(0,len(iterable),len_of_chunk):
        yield iterable[i:i + len_of_chunk]

def get_tags_and_body(data):
    try:
        tags=data.attrib['Tags']
    except:
        return
    body=data.attrib['Body']
    body=re.sub('<[^<]+?>', '', body)
    tags=re.findall('<(.+?)>',tags)
    words_count=Counter(body.split())
    return tags, words_count

def separar_tags_y_palabras(data):
    return dict([tag, data[1].copy()]for tag in data[0])

def reducir_contadores(data1,data2):
    for key,value in data2.items():
        if key in data1.keys():
            data1[key].update(data2[key])
        else:
            data1.update({key: value})
    return data1

def mapeado(data_chunks):    
    post_mapeados= list(map(get_tags_and_body, data_chunks))
    cleaned_map=list(filter(None,post_mapeados))
    words_per_tag=list(map(separar_tags_y_palabras,cleaned_map))
    try:
        reduced_list=reduce(reducir_contadores,words_per_tag)
    except:
        return
    return reduced_list

def unify_chunks(data1,data2):
    for tupla in data2:
        data1.append(tupla)
    return data1

def calculate_top_10(data):
    return data[0], data[1].most_common(10)

current_folder=os.getcwd()
tree=ET()
tree.parse(f'{current_folder}/posts.xml')
root=tree.getroot()
data_chunks=chunkify(root,50)
mapped_list=list(map(mapeado,data_chunks))
mapped_list=list(filter(None,mapped_list))
reduced_list=reduce(reducir_contadores,mapped_list)
top_10=dict(map(calculate_top_10,reduced_list.items()))

print(reduced_list)