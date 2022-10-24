#!/usr/bin/env python
"""mapper.py"""

import sys
import xml.etree.ElementTree as ET
import re

'''The next 3 lines are only used for a testing purpose.'''

with open(('/home/fer/OT301-python/Stack Overflow 11-2010/112010 Meta Stack Overflow/posts.xml'), 'r') as f:
        posts=f.readlines()
        f.close()

'''In order to execute the mapper.py in our hadoop environment,
    we'll have to replace
    the 'for line in posts:' for this next line:
    'for line in sys.stdin:'    '''

for line in posts: #Reading lines from our .xml

    if  line [:6] == '  <row': #Accesing the posts through a common attribute 
                               #that is in each of them.
        try:
            element = ET.fromstring(line)
            CommentCount =(element.attrib['CommentCount']) #Getting one of the integer attributes
            body = element.attrib['Body'] #Getting the second integer attribute
            body = re.findall('(?<!\S)[A-Za-z]+(?!\S)|(?<!\S)[A-Za-z]+(?=:(?!|S))', body)
            len_comments = len(body)
            if CommentCount != None and len_comments != 0: #Making sure there's no empty variables
                print('%s\t%s'%(CommentCount, len_comments))
                ''' The print function will give us the amount of comments first 
                    and the amount of words in a post. To later, in a new line, give us
                    a new pair.'''

        except KeyError:
            continue


