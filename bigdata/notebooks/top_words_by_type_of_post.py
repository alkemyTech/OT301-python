import seaborn as sns
import matplotlib.pyplot as plt
import os 
import xml.etree.ElementTree as ET
import operator
import pandas as pd
from functools import reduce
import re
import pandas as pd
from typing import Counter
import collections

if __name__ == '__main__':

    def chunckify(iterable, len_of_chunk):
        for i in range(0, len(iterable), len_of_chunk):
            yield iterable[i:i + len_of_chunk]



    def words_by_type(data):
        post_type = data.attrib['PostTypeId']
        body = data.attrib['Body']
        # Get all the words of the body in a list, and remove html code using regular expression
        body = re.findall('(?<!\S)[A-Za-z]+(?!\S)|(?<!\S)[A-Za-z]+(?=:(?!\S))', body)
        # Count the words
        words = Counter(body)

        return post_type, words

    def separate_by_type(data):
        return dict([[postid, data[1].copy()]for postid in data[0]])

    def reduce_counters(data1, data2):
        for key, value in data2.items():
            if key in data1.keys():
                data1[key].update(data2[key])
            else:
                data1.update({key:value})
            return data1

    def get_top10(data):
        return data[0], data[1].most_common(10)

    def mapper(data):
        words_count = list(map(words_by_type, data))
        words_count = list(map(separate_by_type, words_count))

        reduced = reduce(reduce_counters, words_count)

        return reduced

    root_path = os.path.abspath('')

    # xml file path
    file_xml = os.path.join(root_path, 'bigdata/datasets/posts.xml')
    #output = os.path.join(root_path, '../outputs/')

    # Read posts.xml file
    tree = ET.parse(file_xml)
    root = tree.getroot()
        
    data_chunks = chunckify(root, 50)

    mapped = list(map(mapper, data_chunks))
    reduce_mapped = reduce(reduce_counters, mapped)

    top = dict(map(get_top10, reduce_mapped.items()))
