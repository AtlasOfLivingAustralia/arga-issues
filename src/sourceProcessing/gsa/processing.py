from lib.crawler import Crawler
import pandas as pd

def build(outputFilePath):
    url = "http://download.cncb.ac.cn/gsa/"
    regexMatch = ".*\\.gz"

    crawler = Crawler(url, regexMatch)
    files, leftoverFolders = crawler.crawl()

    if leftoverFolders:
        print("Exited early, writing file/folder progress to file")

        with open(outputFilePath.parent / "files.txt", 'w') as fp:
            fp.write("\n".join(files))

        with open(outputFilePath.parent / "folders.txt", 'w') as fp:
            fp.write("\n".join(leftoverFolders))

        exit()

    data = []
    titles = ["project_accession", "sample_accession", "experiment_accession", "run_accession"]
    for file in files:
        path = file.strip(url)
        info = {title: layer for title, layer in zip(titles, path.split("/"))} | {"url": file}
        data.append(info)

    df = pd.DataFrame.from_records(data)
    df.to_csv(outputFilePath, index=False)
