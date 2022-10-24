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
from pandas import to_datetime
from timeit import default_timer

if __name__ == '__main__':

    def chunckify(iterable, len_of_chunk):
        for i in range(0, len(iterable), len_of_chunk):
            yield iterable[i:i + len_of_chunk]


    def post(data):
        post_id = data.attrib['Id']
        score = data.attrib['Score']
        creation_date = data.attrib['CreationDate']
        try:
            answer_id = data.attrib['AcceptedAnswerId']
        except:
            answer_id = None
        t = (int(score), answer_id, creation_date)
        d = {post_id: t}
        return d


    def reduce_items(data1, data2):
        if data2 != None:
            for key, value in data2.items():
                if value[0] != 0:
                    data1.update(data2)
                elif 0 in data1.values():
                    data1.popitem()
            return data1
        else:
            return data1


    def filter_none(data):
        for key, value in data.items():
            if data == None:
                pass
            return value[1] != None


    def mapper(data):
        post_score = list(map(post, data))
        filtering = list(filter(filter_none, post_score))
        if len(filtering) > 0:
            reduced = reduce(reduce_items, filtering)
            return reduced


    def by_score(data):
        count = Counter(data)
        count = count.most_common(400)
        return dict(count)


    def average_time(top400):
        a = 0
        for key, value in top400.items():
            question_date = value[2]
            answer_row = root.findall(f".//row[@Id='{value[1]}']")
            answer_date = answer_row[0].attrib['CreationDate']
            a += to_datetime(answer_date).to_numpy() - to_datetime(question_date).to_numpy()
        return int(((a / 400) / 10 ** 9)) / 60
    
    root_path = os.path.abspath('')

    # xml file path
    file_xml = os.path.join(root_path, 'bigdata/datasets/posts.xml')
    # output = os.path.join(root_path, '../outputs/')

    # Read posts.xml file
    tree = ET.parse(file_xml)
    root = tree.getroot()

    data_chunks = chunckify(root, 50)

    mapped = list(map(mapper, data_chunks))
    reduced = reduce(reduce_items, mapped)
    top400 = by_score(reduced)
    promedio = average_time(top400)
    # print(promedio)
    # inicio = default_timer()
    print(f'promedio: {promedio}')


 
