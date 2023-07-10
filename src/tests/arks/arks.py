import requests
from bs4 import BeautifulSoup
from datetime import datetime

baseURL = "https://koalagenomes.s3.ap-southeast-2.amazonaws.com/index.html#"

folders = {}

# with open("awsInfo.txt") as fp:
#     data = fp.read().splitlines()

# with open("dynamicProperties.txt", 'w') as fp:
#     for line in data:
#         # print(line)
#         onAWS, file, folder = line.split()
#         # if onAWS != 'Y':
#         #     fp.write('\n')
#         #     continue

#         folderPath = f"{baseURL}{folder}/bam/"

#         # if folder not in folders:
#         #     data = requests.get(folderPath)
#         #     soup = BeautifulSoup(data.text, 'html.parser')
#         #     rows = soup.find('table').find('tbody')
#         #     break
#         #     for row in rows:
#         #         print(row)
#         #         cells = row.find_all("td")
#         #         print(cells)

#         url = f"{folderPath}{file}.bam"
#         fp.write(str({"WholeGenomeBAM": url}) + '\n')

with open("awsTimestamps.txt") as fp:
    data = fp.read().splitlines()

with open("formattedTime.txt", 'w') as fp:
    for line in data:
        date, time = line.split()
        if len(time.split(':')[0]) == 1:
            time = f"0{time}"

        fp.write(f"{date}T{time}Z\n")
