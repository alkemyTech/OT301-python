##!/usr/bin/python
#
#"""mapper.py"""
#
#import sys
#import xml.etree.ElementTree as ET
#import re
#
#with open('/home/ubuntu/Documentos/Alkemy/OT301-python/bigdata/datasets/posts.xml','r') as f:
#        posts = f.readlines()                    
#        f.close()
#
#for line in posts:
#    if line[:6] == '  <row':
#        element = ET.fromstring(line)
#        #Accederemos a los datos
#        ViewCount = int(element.attrib['ViewCount'])
#        body = element.attrib['Body']
#        body = re.findall('(?<!\S)[A-Za-z]+(?!\S)|(?<!\S)[A-Za-z]+(?=:(?!\S))',body)
#        len_palabras = len(body)
#        #Imprimiremos el dato si cumple los requisitos
#        if ViewCount != None and len_palabras != 0:
#            print ('%s\t%s' % (ViewCount, len_palabras))

#!/usr/bin/python

"""reducer.py"""

import sys

current_id = None
scores = []
parent_id = None

with open('/home/ubuntu/Documentos/Alkemy/OT301-python/bigdata/hadoopSanabria/salida','r') as f:
        posts = f.readlines()                    
        f.close()

for line in posts:
    line = line.strip()                     #Eliminamos posibles saltos de linea
    parent_id, score = line.split('\t', 1)  #Separamos el input en las dos salidas del mapper
    try:
        score = int(score)                  #Convertimos el puntaje en valor numérico
        #Mientras estemos en el mismo id, acumularemos puntajes
        if current_id == parent_id:
            scores.append(score)            
        else:
            if current_id:
                print ('%s\t%s' % (current_id, scores))
            current_id = parent_id
            scores = [score]
    except ValueError:
        pass
   

#Cuando termina de mapear,devolvemos el último valor
if current_id == parent_id:
    print ('%s\t%s' % (current_id, scores))