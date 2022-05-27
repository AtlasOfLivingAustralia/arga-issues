from xmltodict import parse, ParsingInterrupted
import sqlite3 as sl
import json

con = sl.connect('biosamples.db')

with con:
    con.execute("""
        CREATE TABLE IF NOT EXISTS SAMPLE (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            accession TEXT,
            attributeJson TEXT
        );
    """)

sql = 'INSERT INTO SAMPLE (accession, attributeJson) values(?, ?)'
# data = [
#     (1, 'Alice', 21),
#     (2, 'Bob', 22),
#     (3, 'Chris', 23)
# ]
# with con:
#     con.executemany(sql, data)
#bioSampleNameList = []

def handle_biosample(path, item):
    accessionId = path[1][1]['accession']
    atrributeList = item['Attributes']['Attribute']
    tempAttrs = {}
    #print("|",atrributeList)
    for index in range(len(atrributeList)):
        #print("#",atrributeList[index])
        attrs = atrributeList[index]
        key = ""
        key1 = [v for k, v in attrs.items() if k == '@attribute_name'] # all Attribute elements should have this 
        key2 = [v for k, v in attrs.items() if k == '@harmonized_name'] # only some elements have this but its preferable
        if key2:
            #tempAttrs['name'] = key2[0] 
            key = key2[0] 
        elif key1:
            #tempAttrs['name'] = key1[0]
            key = key1[0]
        #tempAttrs['value'] = [v for k, v in attrs.items() if k == '#text'][0]
        tempAttrs[key] = [v for k, v in attrs.items() if k == '#text'][0]
        #     if key == '@harmonized_name':
        #         print("1",atrributeList[index][key])
        #     elif key == '@attribute_name':
        #         print("1",atrributeList[index][key])
        #     if key == '#text':
        #         print("2", atrributeList[index][key])
            #print(key,"||",atrributeList[index][key])
    #values = , ]
    # print(path[1][1]['accession'])
    # print(item['Attributes']['Attribute'][1]['@attribute_name'])
    jsonStr = json.dumps(tempAttrs)
    #print(accessionId, "|", jsonStr)
    sqlData = [(accessionId, jsonStr)]
    with con:
        con.executemany(sql, sqlData)
    return True

parse(open("/data/arga-data/biosample_set.xml", "rb").read(), item_depth=2, item_callback=handle_biosample)