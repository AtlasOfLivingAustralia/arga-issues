from bigxml import Parser, xml_handle_element, xml_handle_text
from dataclasses import dataclass, field
from typing import Dict, List
import sqlite3 as sl
import json

con = sl.connect('biosamples.db')

with con:
    con.execute("""
        DROP TABLE IF EXISTS SAMPLE;
    """)

with con:
    con.execute("""
        CREATE TABLE IF NOT EXISTS SAMPLE (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            accession TEXT,
            email TEXT,
            link_url TEXT,
            attribute_json TEXT
        );
    """)

sql = 'INSERT INTO SAMPLE (accession, email, link_url, attribute_json) values(?, ?, ?, ?)'

@xml_handle_element("BioSampleSet", "BioSample")
@dataclass
class Entry:
    id: str 
    email: str = ''
    url: str = ''
    props: Dict[str, str] = field(default_factory=dict)

    def __init__(self, node):
         self.id = node.attributes['accession']
         self.props = {}

    @xml_handle_element("Attributes","Attribute")
    def handle_attribute(self, items):
        thisAttr = { items.attributes['attribute_name']: items.text }
        self.props.update(thisAttr)


    @xml_handle_element("Owner","Contacts","Contact")
    def handle_email(self, node):
        self.email = node.attributes['email']
        #print(self.email)
    
    @xml_handle_element("Links","Link")
    def handle_link(self, node):
        if node.attributes['type'] == 'url':
            self.url = node.text


with open("/data/arga-data/biosample_set_small.xml", "rb") as f:
    for item in Parser(f).iter_from(Entry):
        #print("> ",item)
        jsonStr = json.dumps(item.props)
        sqlData = [(item.id, item.email, item.url, jsonStr)]
        with con:
            con.executemany(sql, sqlData)

with con:
    data = con.execute("SELECT * FROM SAMPLE LIMIT 10")
    for row in data:
        print(row)
    count = con.execute("SELECT COUNT(*) FROM SAMPLE")
    print("count = ", count.fetchone()[0])