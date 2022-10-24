#!/usr/bin/python

"""mapper.py"""

import sys
import xml.etree.ElementTree as ET

for line in sys.stdin:                                #Accedemos linea por linea a los post
    if line[:6] == '  <row':                          #Solo lo accedemos si es un post
        element = ET.fromstring(line)                 #Convertimos texto xml en un objeto
        if element.attrib['PostTypeId'] == "2":       #Procesamos solo si es un comentario
            #Extraemos los atributos
            try:
                parent_id = int(element.attrib['ParentId'])         
                score = int(element.attrib['Score'])
                print('%s\t%s' % (parent_id, score))
            except:
                pass