import pandas as pd

def addBinAndTaxon(url, bin, taxon):
    taxon = taxon.replace(" ", "%20")
    return f"{url}bin={bin}&taxon={taxon}"

sequence = "http://v4.boldsystems.org/index.php/API_Public/sequence?"
trace = "http://v4.boldsystems.org/index.php/API_Public/trace?"

df = pd.read_csv("data.tsv", encoding="ISO-8859-1", sep="\t", dtype=str)
df["fastaLink"] = df.apply(lambda x: addBinAndTaxon(sequence, x.bin_uri, x.species_name), axis=1)
df["traceLink"] = df.apply(lambda x: addBinAndTaxon(trace, x.bin_uri, x.species_name), axis=1)
df.to_csv("dataWithLinks.csv", index=False)