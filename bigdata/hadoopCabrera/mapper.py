#!/usr/bin/python

"""mapper.py"""

import xml.etree.ElementTree as ET
import sys
import operator
import os

for line in sys.stdin:
    if line[:6] == '  <row':
        element=ET.fromstring(line)
        ids=(element.attrib['Id'])
        views=int(element.attrib['ViewCount'])
        print ('%s\t%s' % (views,ids))