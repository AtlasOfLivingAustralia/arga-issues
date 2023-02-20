import argparse
from pathlib import Path
from xml.etree import cElementTree as etree

def getelements(filename_or_file):
    context = iter(etree.iterparse(filename_or_file, events=('start', 'end')))
    _, root = next(context) # get root element

    event, element = next(context)
    tag = element.tag

    topLevelTag = tag

    while tag is not root.tag:
        if event == 'end' and element.tag == topLevelTag:
            yield element
            root.clear()

        event, element = next(context)
        tag = element.tag

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Get splices from an xml")
    parser.add_argument('filepath', help="Path to input file")
    parser.add_argument('-f', '--firstEntry', type=int, default=0, help="First entry to grab entries from")
    parser.add_argument('-e', '--entries', type=int, default=1, help="Amount of entries to grab")
    args = parser.parse_args()

    path = Path(args.filepath)
    outputDir = path.parent

    with open(outputDir / "xmlSplice.xml", 'wb') as fp:
        for idx, page in enumerate(getelements(path)):
            if idx >= args.firstEntry:
                fp.write(etree.tostring(page, encoding='utf-8'))

            if idx >= (args.firstEntry + (args.entries - 1)):
                break

