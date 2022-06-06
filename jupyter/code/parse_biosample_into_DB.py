from bigxml import Parser, xml_handle_element, xml_handle_text
from dataclasses import dataclass, field
from datetime import datetime
from datalite import datalite
from datalite.fetch import fetch_if, fetch_all, fetch_range, fetch_from, fetch_equals, fetch_where
from datalite.migrations import basic_migrate, _drop_table
import json
import progressBar as pgb

start = datetime.now()
print("Start:",start)
# clear DB
#_drop_table('../data/biosamples_full.db', 'Entry')
# CREATE INDEX idx_entry_id on ENTRY(id);

# See example record HTML version: https://www.ncbi.nlm.nih.gov/biosample/?term=SAMN06198159
@xml_handle_element("BioSampleSet", "BioSample")
@datalite(db_path="../data/biosamples_DBL.db")
@dataclass
class Entry:
    """
    A single record from the BioSample Set XML file from NCBI

    A subset of fields and values are pulled out and stored in a SQLite3 database. 
    This DB will be used in the DwC file code to populate additional values not present
    in the NCBI-refseq CSV dump from NCBI.
    """
    id: str = ''
    scientificName: str = ''
    email: str = ''
    lab: str = ''
    url: str = ''
    dwc_EarliestDateCollected: str = ''
    dwc_otherCatalogNumbers: str = ''
    dwc_sex: str = ''
    dwc_lifeStage: str = ''
    dwc_MaterialSample: str = ''
    dwc_materialSampleID: str  = ''
    attrs: str = ''

    def __init__(self, node):
        # <BioSample submission_date="2008-07-24T12:44:10.763" last_update="2013-06-12T08:42:39.317" publication_date="2008-09-15T09:31:24.973" access="public" id="355" accession="SAMN00000355">
        if node.attributes.get('accession'):
            self.id = node.attributes['accession']
        elif node.attributes.get('id'):
            self.id = "SAMN" + node.attributes['id'].zfill(8)
        else:
            self.id = 'none'
        #self.attrs = {}

    # <Organism taxonomy_id="4932" taxonomy_name="Saccharomyces cerevisiae"/>
    @xml_handle_element("Description","Organism")
    def handle_organism(self, node):
        if node.attributes.get('taxonomy_name'):
            self.scientificName = node.attributes['taxonomy_name']

    # <Contact email="shuse@mbl.edu" lab="MARBILAB">
    @xml_handle_element("Owner","Contacts","Contact")
    def handle_email(self, node):
        if node.attributes.get('email'):
            self.email = node.attributes['email']
        if node.attributes.get('lab'):
            self.lab = node.attributes['lab']

    # <Attribute attribute_name="strain" harmonized_name="strain" display_name="strain">BY4741</Attribute>
    @xml_handle_element("Attributes","Attribute")
    def handle_attribute(self, items):
        # Attributes can have different field names - pick the "best" available
        thisAttr = {} # use a dict to strore attribute fields
        if self.attrs:
            thisAttr = json.loads(self.attrs) # load any existing data from previous Attribute entries     
        if items.attributes.get('harmonized_name'):
            thisAttr.update({ items.attributes['harmonized_name']: items.text })
        elif items.attributes.get('attribute_name'):
            thisAttr.update({ items.attributes['attribute_name']: items.text })
        #self.attrs.update(thisAttr) # add to dataclass field (dict)
        self.attrs = json.dumps(thisAttr) # JSON stringified
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

    # <Link type="url" label="GEO Web Link">http://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSM282598</Link>
    @xml_handle_element("Links","Link")
    def handle_link(self, link):
        if link.attributes['type'] == 'url':
            self.url = link.text

debug = False
totalRecords = 26497644
count = 0
with open("/data/arga-data/biosample_set.xml", "rb") as f:
    for item in Parser(f).iter_from(Entry):
        count +=  1
        if (debug and count % 1000 == 0):
            print(count,item)
        # if (not debug and count % 10000 == 0):
        #     print('.', end='', sep='')
        # if count % 800000 == 0:
        #     print('') # 80 column line length
        pgb.printProgress(count, totalRecords)
        item.create_entry()  # Adds the entry to the table associated in db.db.

# print('') # force a newline
# Show first 10 entries - failing for some reason
# print(fetch_all(Entry, 1, 10))

end = datetime.now()
print("End:", end)
print("Count =","{:,d}".format(count))
print("The total execution time in seconds is:", str(end-start)[:9])