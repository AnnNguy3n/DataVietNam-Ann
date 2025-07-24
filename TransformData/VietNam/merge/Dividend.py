import _lib_path_
import pandas as pd
import sys

from base.PATH_UPDATE import *
from VAR_GLOBAL_CONFIG import *
from base.Financial import CafeF,VietStock
from base.Setup import *
from Flow.ulis import *

CURRENT = 0
Data = pd.read_excel(f"{PATH_COMPARE}/Dividend.xlsx")
Data = Data[["Time","FIX","Symbol"]][Data["FIX"].notnull()]
Data["Money"] = Data["FIX"].apply(lambda row:row.split("_")[0])
Data["Stock"] = Data["FIX"].apply(lambda row:row.split("_")[-1])
Data[["Time","Money","Stock","Symbol"]].to_excel(f"{FU.PATH_MAIN_CURRENT}/DIVIDEND.xlsx",index=False)
