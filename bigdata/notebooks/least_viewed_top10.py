import os 
import xml.etree.ElementTree as ET
import operator
from functools import reduce



if __name__ == '__main__':


    def chunckify(iterable, len_of_chunk):
        for i in range(0, len(iterable), len_of_chunk):
            yield iterable[i:i + len_of_chunk]

    def least_viewed(data):
    # Id de cada post
        post_id = data.attrib['Id']
    # Cuenta de las vistas
        view_count = data.attrib['ViewCount']
        dictionary = {post_id: int(view_count)}
        return dictionary

    def reduce_items(data1,data2):
        for key, value in data2.items():
            if value != 0:
                data1.update(data2)
            elif 0 in data1.values():
                data1.popitem()
        return data1
    
    def top10_least_viewed(data):
        s = sorted(data.items(), key=operator.itemgetter(1), reverse=False)
        d = []
        for i in range(10):
            d.append(s[i])
        return dict(d)
    
    def mapper(data):
        post_viewd = list(map(least_viewed, data))
        reducing = reduce(reduce_items, post_viewd)
        return reducing

    # Ruta del directorio
    root_path = os.path.abspath('')

    # Ruta del archivo xml
    file_xml = os.path.join(root_path, 'bigdata/datasets/posts.xml')

    # Lee archivo posts.xml
    tree = ET.parse(file_xml)
    root = tree.getroot()
    

    data_chunks = chunckify(root,50)

    mapped = list(map(mapper, data_chunks))
    reduce_mapped = reduce(reduce_items, mapped)

    top10 = top10_least_viewed(reduce_mapped)