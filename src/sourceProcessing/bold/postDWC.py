import numpy as np
import pandas as pd

clusterPrefix = "http://www.boldsystems.org/index.php/Public_BarcodeCluster?clusteruri="

def augment(df: pd.DataFrame):
    df['species'] = df['species'].fillna("sp. {" + df['bold_bin_uri'].astype(str) + "}")
    df['bold_bin_uri'] = np.where(df['bold_bin_uri'].notna(), clusterPrefix + df['bold_bin_uri'], df['bold_bin_uri'])

    return df
