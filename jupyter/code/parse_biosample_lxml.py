import sys
import xml.etree.ElementTree as ET
from lxml import etree
import time

file_path = "/data/arga-data/biosample_set_big.xml"
count = 0

tic = time.perf_counter()
for event, elem in ET.iterparse(file_path, events=("end",)):
    if event == "end":
        if elem.tag == 'Attribute':
            count += 1
        elem.clear()
toc = time.perf_counter()
print(f"1. Total parse time = {toc - tic:0.4f} seconds")
print("Count of Attribute tags:", count)

count1 = 0
tic1 = time.perf_counter()
# for event, element in etree.iterparse(file_path, tag="Attribute"):
for ev, element in etree.iterparse(file_path):
    # if element.get('accession'):
    #     print('accession:', element.get('accession'))
    # for child in element:
    #     print('children:', child.tag, child.text, child.keys(), sep='|')
 #   nameEl = etree.SubElement(element, "Name")
 #   nameEl = element.xpath('//Name')
    # for el in nameEl:
    # print("Name:", el.text, "| abbreviation:", el.get('abbreviation'))
    # attributeEl = etree.SubElement(element, "Attribute")
    # attributeEl = element.get('Attribute')
    # attributeEl = element.xpath('//Attribute')
    # for el in attributeEl:
    #     count1 += 1
    if ev == "end":
        if element.tag == 'Attribute':
            count1 += 1
        element.clear()

toc1 = time.perf_counter()
print(f"2. Total parse time = {toc1 - tic1:0.4f} seconds")
print("Count of Attribute tags:", count1)
