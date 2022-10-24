#!/usr/bin/env python3

"""mapper.py"""

import xml.etree.ElementTree as ET
import sys
import operator
import os
import re
from typing import Counter

for line in sys.stdin:
    if line[:6] == '  <row':
        element=ET.fromstring(line)
        try:
            tags=element.attrib['Tags']
        except:
            pass
        body=element.attrib['Body']
        body=re.sub('<[^<]+?>', '', body)
        body=body.strip()
        words=body.split()
        tags=tags.replace('>',' ')
        tags=tags.replace('<','')
        tags=tags.replace(' ','/')
        tags=tags[:-1]
        for tag in tags.split('/'):    
            for word in body.split(' '):
                dic={}
                dic[tag]=word,int(1)
                for tag,word_and_count in dic.items():
                    print ('%s\t%s' % (tag,word_and_count))