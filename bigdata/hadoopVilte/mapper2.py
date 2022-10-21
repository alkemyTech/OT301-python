#!/usr/bin/python

"""mapper.py"""

import sys
import xml.etree.ElementTree as ET
import operator

for line in sys.stdin:                                
    if line[:6] == '  <row':                          
        element = ET.fromstring(line)                 
        if element.attrib['Id']:       
            #Extraemos los atributos
            try:
                post_id = int(element.attrib['Id'])
                viewCount = int(element.attrib['ViewCount'])
                print ('%s\t%s' % (post_id, viewCount))
            except:
                pass