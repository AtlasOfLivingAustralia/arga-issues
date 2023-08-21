import requests
import json

baseURL = "https://fishbase.ropensci.org/"
sealifebase = "sealifebase/"

response = requests.get(baseURL + sealifebase + "docs")
print(response.text)
