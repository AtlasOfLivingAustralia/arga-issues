from pathlib import Path
import pandas as pd

filePath = Path("../../../data/ncbi/refseqSummary/dwc/assembly_summary_refseq-dwc.csv")
df = pd.read_csv(filePath)

events = {
    1: "Collection",
    2: "AccessionToInstitution",
    3: "Subsampling",
    4: "Extraction",
    5: "Sequencing",
    6: "Assembly",
    7: "Annotation",
    8: "DataAccessionToRepository"
}

columns = {}
for column in df.columns:
    eventNumber = int(column[1])
    if eventNumber not in columns:
        columns[eventNumber] = {}

    columns[eventNumber][column] = column.split(":", 1)[1]

with pd.ExcelWriter(filePath.parent / "refseq_dwc.xlsx") as writer:

    for eventNumber, mapping in columns.items():
        subDF = df[list(mapping.keys())]
        newDF = subDF.rename(mapping, axis=1)

        # newDF.to_csv(filePath.parent / f"refseq_{events[eventNumber]}_dwc.csv", index=False)
        newDF.to_excel(writer, sheet_name=events[eventNumber], index=False)