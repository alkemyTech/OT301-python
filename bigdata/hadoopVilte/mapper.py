#!/usr/bin/env python
"""mapper.py"""



import re
import xml.etree.ElementTree as ET
import sys
from collections import Counter

with open('/home/tomi/OT301-python/bigdata/datasets/posts.xml', 'r') as f:
    posts = f.readlines()
    f.close()



i = 0 
    # lee linea por linea la entrada del archivo posts.xml
for line in sys.stdin:
    if i > 1:
        # las dos primeras lineas de la entrada son etiquetas del XML que no hay que tomar.
        try:
            root = ET.fromstring(line)
            dir_row = root.attrib
            if (dir_row.get('Body')):
                    post_type = dir_row['PostTypeId']
                    body = dir_row['Body']
                    body = re.findall('(?<!\S)[A-Za-z]+(?!\S)|(?<!\S)[A-Za-z]+(?=:(?!\S))', body)
                    words = 1
                    print('%s\t%s' % (post_type, words))
                                         
        except:
        # para saltear las etiquetas al final del archivo posts.xml
            pass
    i = i + 1