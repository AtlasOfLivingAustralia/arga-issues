from bs4 import BeautifulSoup
import requests
import pandas as pd

def buildUrl(limit, offset):
    return f"https://v4.boldsystems.org/index.php/Public_Primer_PrimerSearch/getSearchResultPage?offset={offset}&limit={limit}&cartToken=general_49407BD4-19BD-4C0D-9667-C251F71B0A35&_=1679547117834"

def buildAjax(id):
    return f"https://v4.boldsystems.org/index.php/Public_Ajax_PrimerView?id={id}&token=token&username=username&userid=0"

pages = 6
entriesPerPage = 500
records = []

for page in range(pages):
    response = requests.get(buildUrl(entriesPerPage, page*entriesPerPage))
    soup = BeautifulSoup(response.text, 'html.parser')
    for idx, div in enumerate(soup.find_all("div")):
        if idx % 2 != 0:
            continue

        ajax = requests.get(buildAjax(div["id"]))
        data = ajax.json()
        records.append(data["primer"])

df = pd.DataFrame.from_records(records)
df.to_csv("rawprimers.csv", index=False)