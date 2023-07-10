import pandas as pd
import numpy as np

df = pd.read_csv("../../data/bold/austsv/raw/bold_data.tsv", encoding="iso-8859-1", on_bad_lines="skip", sep="\t")
# print(df.columns)
df2 = df[["recordID", "bin_uri"]]
df2["primers"] = df["seq_primers"].str.split('|')
df2["primers"].replace('', np.nan, inplace=True)
df2.dropna(subset=["primers"], inplace=True)
df2.to_csv("wrong_primers.csv", index=False)
