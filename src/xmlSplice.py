from xml.etree import cElementTree as etree

def getelements(filename_or_file, tag):
    context = iter(etree.iterparse(filename_or_file, events=('start', 'end')))
    _, root = next(context) # get root element
    for event, elem in context:
        if event == 'end' and elem.tag == tag:
            yield elem
            root.clear() # free memory

if __name__ == '__main__':
    with open('../data/output.xml', 'wb') as file:
        file.write(b'<root>') # start root
        for page in getelements('../data/biosample.xml', 'BioSample'):
            file.write(etree.tostring(page, encoding='utf-8'))
            break

        file.write(b'</root>') # close root