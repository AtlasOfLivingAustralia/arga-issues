import pandas as pd
import lib.dataframeFuncs as dff
import lib.commonFuncs as cmn

def augmentBiosample(df: pd.DataFrame) -> pd.DataFrame:
    return dff.splitField(df, "ncbi_lat long", cmn.latlongToDecimal, ["decimalLatitude", "decimalLongitude"])
