#!/usr/bin/python

"""mapper.py"""

import sys
import xml.etree.ElementTree as ET
import re

for line in sys.stdin:                                #Accedemos linea por linea a los post
    if line[:6] == '  <row':                          #Solo lo accedemos si es un post
        element = ET.fromstring(line)                 #Convertimos texto xml en un objeto
        #Accederemos a los datos
        ViewCount = int(element.attrib['ViewCount'])
        body = element.attrib['Body']
        body = re.findall('(?<!\S)[A-Za-z]+(?!\S)|(?<!\S)[A-Za-z]+(?=:(?!\S))',body)
        len_palabras = len(body)
        #Imprimiremos el dato si cumple los requisitos
        if ViewCount != None and len_palabras != 0:
            print ('%s\t%s' % (ViewCount, len_palabras))