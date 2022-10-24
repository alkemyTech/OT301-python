#!/usr/bin/python

"""mapper.py"""

import sys
import xml.etree.ElementTree as ET

for line in sys.stdin:                                #Accedemos linea por linea a los post
    if line[:6] == '  <row':                          #Solo lo accedemos si es un post
        element = ET.fromstring(line)                 #Convertimos texto xml en un objeto
        if element.attrib['PostTypeId'] == "1":       #Procesamos si es una pregunta
            #Extraemos los atributos
            try:
                question_id = int(element.attrib['Id'])
                favoriteCount = int(element.attrib['FavoriteCount'])
                print ('%s\t%s' % (question_id, favoriteCount))
            except:
                pass