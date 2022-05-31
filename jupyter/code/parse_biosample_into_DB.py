from bigxml import Parser, xml_handle_element, xml_handle_text
from dataclasses import dataclass, field
from typing import Dict, List
from datetime import datetime
import sqlite3 as sl
import json

start = datetime.now()
print("Start:",start)

con = sl.connect('../data/biosamples_medium.db')

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

with con:
    con.execute("""
        CREATE INDEX idx_sample_accession on SAMPLE(accession);
""") 

sql = 'INSERT INTO SAMPLE (accession, email, link_url, attribute_json) values(?, ?, ?, ?)'

# See example record HTML version: https://www.ncbi.nlm.nih.gov/biosample/?term=SAMN06198159
@xml_handle_element("BioSampleSet", "BioSample")
@dataclass
class Entry:
    id: str 
    email: str = ''
    url: str = ''
    dwc_EarliestDateCollected: str = ''
    dwc_otherCatalogNumbers: str = ''
    dwc_sex: str = ''
    dwc_lifeStage: str = ''
    dwc_MaterialSample: str = ''
    dwc_materialSampleID: str  = ''
    attrs: Dict[str, str] = field(default_factory=dict)

    def __init__(self, node):
        if node.attributes.get('accession'):
            self.id = node.attributes['accession']
        elif node.attributes.get('id'):
            self.id = "SAMN" + node.attributes['id'].zfill(8)
        else:
            self.id = 'none'
        self.attrs = {}

    @xml_handle_element("Owner","Contacts","Contact")
    def handle_email(self, node):
        if node.attributes.get('email'):
            self.email = node.attributes['email']

    @xml_handle_element("Attributes","Attribute")
    def handle_attribute(self, items):
        # Attributes can have different field names - pick the "best" available
        thisAttr = {} # use a dict to strore attribute fields
        if items.attributes.get('harmonized_name'):
            thisAttr = { items.attributes['harmonized_name']: items.text }
        elif items.attributes.get('attribute_name'):
            thisAttr = { items.attributes['attribute_name']: items.text }
        self.attrs.update(thisAttr) # add to dataclass field (dict)
        # extract any DwC fields so upstream code doesn't need to know about the XML structure
        if (items.attributes.get('attribute_name') and items.attributes['attribute_name'] == 'collection_date'):
            self.dwc_EarliestDateCollected = items.text
        if (items.attributes.get('attribute_name') and items.attributes['attribute_name'] == 'specimen_voucher'):
            self.dwc_otherCatalogNumbers = items.text
        if (items.attributes.get('attribute_name') and items.attributes['attribute_name'] == 'sex'):
            self.dwc_sex = items.text
        if (items.attributes.get('attribute_name') and items.attributes['attribute_name'] == 'developmental stage'):
            self.dwc_lifeStage = items.text
        if (items.attributes.get('attribute_name') and items.attributes['attribute_name'] == 'sample type'):
            self.dwc_MaterialSample = items.text
        if (items.attributes.get('attribute_name') and items.attributes['attribute_name'] == 'isolate'):
            self.dwc_materialSampleID = items.text

    @xml_handle_element("Links","Link")
    def handle_link(self, link):
        if link.attributes['type'] == 'url':
            self.url = link.text


with open("/data/arga-data/biosample_set_big.xml", "rb") as f:
    for item in Parser(f).iter_from(Entry):
        print("> ",item)
        jsonStr = json.dumps(item.attrs)
        sqlData = [(item.id, item.email, item.url, jsonStr)]
        with con:
            con.executemany(sql, sqlData)

with con:
    data = con.execute("SELECT * FROM SAMPLE LIMIT 10")
    for row in data:
        print(row)
    count = con.execute("SELECT COUNT(*) FROM SAMPLE")
    print("count =", count.fetchone()[0])

end = datetime.now()
print("End:", end)
print("The total execution time in seconds is:", str(end-start)[:9])