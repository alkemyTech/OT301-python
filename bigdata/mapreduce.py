from xml.etree.cElementTree import ElementTree as ET
from functools import reduce
from typing import Counter
import re
import os
import operator


def chunkify(iterable,len_of_chunk):
    for i in range(0,len(iterable),len_of_chunk):
        yield iterable[i:i + len_of_chunk]

def get_title_and_reviews(data):
    id=data.attrib['Id'] #NO BORRAR
    views=data.attrib['ViewCount']
    return views, id,  

def mapeado(data):    
    post_mapeados= list(map(get_title_and_reviews, data))
    return post_mapeados

def order_data(post_mapeado):
    #ordered_data=post_mapeado.sort(key=operator.itemgetter(1))
    int_tuple=tuple(tuple(map(int, x)) for x in post_mapeado)
    ordered_data=sorted(int_tuple, key=operator.itemgetter(0), reverse=True) #poner 0 en el parentesis 
                                                                             #si vuelvo a poner la ID
                                                                             #mas arriba
    return ordered_data

def get_top_10_posts(ordered_data):
    return list(map(max,zip(*ordered_data)))

def max_value(top_10_post):
    return max(top_10_post)

def answer_visit_list_merge(data1,data2):
    for tupla in data2:
        data1.append(tupla)
    return data1

current_folder=os.getcwd()
tree=ET()
tree.parse(f'{current_folder}/112010 Stack Overflow/posts.xml')
root=tree.getroot()
data_chunks=chunkify(root,50)
post_mapeado=list(map(mapeado,data_chunks))

def ordenar(data):
    lista_aux =[]
    for dato in data:
        lista_aux.append((int(dato[0]),int(dato[1])))
    return lista_aux

ordenado=(reduce(answer_visit_list_merge,post_mapeado))

lista_ordenada=ordenar(ordenado)

def order_list(lista_ordenada):
    ordered_list=sorted(lista_ordenada, key=operator.itemgetter(0), reverse=True)
    return ordered_list

lista_oficial=order_list(lista_ordenada)

top_10=(lista_oficial[0:10])

print(top_10)