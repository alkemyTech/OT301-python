from xml.etree.cElementTree import ElementTree as ET
from functools import reduce
from typing import Counter
import re
import os
import operator


def chunkify(iterable,len_of_chunk):
    for i in range(0,len(iterable),len_of_chunk):
        yield iterable[i:i + len_of_chunk]

def get_title_and_views(data):
    id=data.attrib['Id'] #NO BORRAR
    views=data.attrib['ViewCount']
    return views, id,  

def mapeado(data):    
    post_mapeados= list(map(get_title_and_views, data))
    return post_mapeados

def order_data(post_mapeado):
    #ordered_data=post_mapeado.sort(key=operator.itemgetter(1))
    int_tuple=tuple(tuple(map(int, x)) for x in post_mapeado)
    ordered_data=sorted(int_tuple, key=operator.itemgetter(0), reverse=True) # poner 0 en el parentesis 
    return ordered_data                                                      # si vuelvo a poner la ID
                                                                             # mas arriba

def max_value(top_10_post):
    return max(top_10_post)

def unify_chunks(data1,data2):
    for tupla in data2:
        data1.append(tupla)
    return data1

current_folder=os.getcwd()
tree=ET()
tree.parse(f'{current_folder}/112010 Stack Overflow/posts.xml')
root=tree.getroot()
data_chunks=chunkify(root,50)
post_mapeado=list(map(mapeado,data_chunks))

def set_int_tuple(data):
    lista_aux =[]
    for dato in data:
        lista_aux.append((int(dato[0]),int(dato[1])))
    return lista_aux

unify_list=(reduce(unify_chunks,post_mapeado))

ordered_list=set_int_tuple(unify_list)

def order_list(ordered_list):
    ordered_list=sorted(ordered_list, key=operator.itemgetter(0), reverse=True)
    return ordered_list

official_list=order_list(ordered_list)

top_10=(official_list[0:10])

print(top_10)