#!/usr/bin/python

"""mapper.py"""

import sys
import xml.etree.ElementTree as ET
import re

for line in sys.stdin:                                #Accedemos linea por linea a los post
    if line[:6] == '  <row':                          #Solo lo accedemos si es un post
        element = ET.fromstring(line)                 #Convertimos texto xml en un objeto
        #Accederemos a los datos
        try:
            tags = element.attrib['Tags']
            tags = re.findall('<(.+?)>', tags)
            tags = list(set(tags))
            #A partir de aquí, imprimiremos el dato si cumple los requisitos
            try:
                if element.attrib['AcceptedAnswerId'] != None or element.attrib['AcceptedAnswerId'] != '0':
                    pass
                else:
                    for tag in tags:
                        print ('%s\t%s' % (tag, 1))    
            except:
                for tag in tags:
                    print ('%s\t%s' % (tag, 1))
        except:
            pass