import requests
import json
import xmltodict
import os
import config

if __name__ == '__main__':
    # Generate mapping json from all dwc extensions
    # Additionally, generate a helpful collation of all dwc extensions available

    dwcExtensions = "https://rs.gbif.org/extensions.json"
    outputFolder = config.genFolder

    response = requests.get(dwcExtensions)
    extensionMap = response.json()

    rawOutput = {}
    dwcCollation = {}
    conversionMap = {}

    for ext in extensionMap["extensions"]:
        subcatagory = requests.get(ext["url"])
        decodedSubcatagory = subcatagory.content.decode("utf-8")
        output = json.loads(json.dumps(xmltodict.parse(decodedSubcatagory)))
        rawOutput[ext["title"]] = output

        for prop in output["extension"]["property"]:
            dwcCollation[prop["@name"]] = {
                    "group": prop.get("@group", ""),
                    "relation": prop.get("@dc:relation", ""),
                    "description": prop.get("@dc:description", "No description available."),
                    "examples": prop.get("@examples", ""),
                    "qualName": prop.get("@qualName", ""),
                    "required": prop.get("@required", "Unknown")
                }

            conversionMap[prop["@name"]] = []

    with open(os.path.join(outputFolder, "rawDwCExtensions.json"), "w") as fp:
        json.dump(rawOutput, fp, indent=4)

    with open(os.path.join(outputFolder, "DwCExtensionCollation.json"), "w") as fp:
        json.dump(dwcCollation, fp, indent=4)

    if not os.path.exists(config.mappingPath):
        with open(config.mappingPath, "w") as fp:
            json.dump(conversionMap, fp, indent=4)
