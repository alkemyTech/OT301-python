#!/usr/bin/python

"""mapper.py"""

import xml.etree.ElementTree as ET
import sys
import operator
import os

with open('/home/seba/docker-hadoop/posts.xml','r') as f:
    posts=f.readlines()
    f.close()

for line in posts:#sys.stdin
    if line[:6] == '  <row':
        element=ET.fromstring(line)
        ids=(element.attrib['Id'])
        views=int(element.attrib['ViewCount'])
        asdf={}
        asdf[ids]=views
        for key,view_count in asdf:
            oficial=dict(dic)
        ordered_list=sorted(oficial.items(), key=lambda x:x[1], reverse=True)
        #top_10=views[0:10]
        print(ordered_list)
        #print ('%s\t%s' % (views,ids))