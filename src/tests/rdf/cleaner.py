from pathlib import Path

filePath = Path("2023-09-18_DWC_TTL.ttl")
outputFile = f"{filePath.stem}_fixed.ttl"

with open(filePath) as fp1, open(outputFile, "w") as fp2:
    for line in fp1.readlines():
        line = line.replace(" :", "")
        fp2.write(line)
