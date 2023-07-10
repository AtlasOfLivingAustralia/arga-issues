import pandas as pd

df = pd.read_csv("rawprimers.csv")
df["notes"] = df["notes"].apply(lambda x: str(x).replace("\n", "").replace("\r", "") if x else "")
df["reference"] = df["reference"].apply(lambda x: str(x).replace("\n", "").replace("\r", "") if x else "")
df.drop("id", axis=1, inplace=True)
df["cocktail"] = df["cocktail"].apply(lambda x: "No" if x == "f" else "Yes")
df["public"] = df["public"].apply(lambda x: "No" if x == "f" else "Yes")
df["notes"] = df["notes"].apply(lambda x: "" if x == "nan" else x)
df = df[["code", "name", "alias", "marker", "cocktail", "nuc", "direction", "reference", "notes", "public", "submitter", "updated", "posreference"]]
df.to_csv("primers.csv", index=False, sep='\t')