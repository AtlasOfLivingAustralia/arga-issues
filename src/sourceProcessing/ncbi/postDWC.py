import lib.commonFuncs as cmn
import lib.dataframeFuncs as dff

def biosample(df):
    df = dff.splitField(df, "ncbi_lat long", cmn.latlongToDecimal, ["decimalLatitude", "decimalLongitude"])
    return df
