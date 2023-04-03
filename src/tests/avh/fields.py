import requests
import pandas as pd

full = "https://avh.ala.org.au/fields?max=1000&order=ASC&sort=name&filter="
data = requests.get(full)
df = pd.read_html(data.text)
df = df[0]
df.drop("Other attributes", axis=1, inplace=True)
df.to_csv("avhFields.csv", index=False)