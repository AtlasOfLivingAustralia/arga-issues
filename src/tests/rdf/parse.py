from rdflib import Graph, URIRef
import requests
from io import StringIO

def getTTL(url: str) -> Graph:
    print(f"Retrieving '{url}' ...")
    response = requests.get(url)
    if response.status_code != 200:
        return None
    
    ioStream = StringIO(response.text)
    graph = Graph()

    return graph.parse(ioStream, format="ttl")

def printGraph(g: Graph) -> None:
    for subj, pred, obj in g:
        print(f"{subj} | {pred} | {obj}")

dwcEndpoint = "http://rs.tdwg.org/dwc.ttl"
g = getTTL(dwcEndpoint)

# Get latest version
version = URIRef("http://purl.org/dc/terms/hasVersion")
versions = [object for object in g.objects(predicate=version)]
latest = versions[-1]
print(latest)

latestTTL = f"{latest}.ttl"
g2 = getTTL(latestTTL)

printGraph(g2)