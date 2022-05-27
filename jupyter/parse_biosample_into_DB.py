from bigxml import Parser, xml_handle_element, xml_handle_text
from dataclasses import dataclass, field
from typing import Dict, List
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

@xml_handle_element("BioSampleSet", "BioSample")
@dataclass
class Entry:
    id: str 
    props: List[list] = field(default_factory=list)

    def __init__(self, node):
         self.id = node.attributes['accession']
         self.props = []
         #print(node.attributes['accession'])

    @xml_handle_element("Attributes","Attribute")
    def handle_title(self, items):
        #thisAttr = {items.attributes['attribute_name']: items.text }
        thisAttr = items.attributes['attribute_name']
        self.props.append(thisAttr)
        #print(self)
        #yield from items


with open("/data/arga-data/biosample_set_small.xml", "rb") as f:
    for item in Parser(f).iter_from(Entry):
        #print("> ",item)
        #print("item: ", item)
        jsonStr = json.dumps(item.props)
        #print(accessionId, "|", jsonStr)
        sqlData = [(item.id, jsonStr)]
        with con:
            con.executemany(sql, sqlData)

with con:
    data = con.execute("SELECT * FROM SAMPLE LIMIT 10")
    for row in data:
        print(row)