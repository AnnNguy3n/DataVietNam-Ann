import _lib_path_
import pandas as pd
import sys

from base.PATH_UPDATE import *
from VAR_GLOBAL_CONFIG import *
from base.Setup import *
from Flow.ulis import *

CURRENT = 0
VALUE = pd.DataFrame()
for symbol in SYMBOL:
    CURRENT+=1
    try:
        df = pd.read_csv(FU.joinPath(FU.PATH_MAIN_CURRENT,"Close","CafeF","F1",f"{symbol}.csv"))
        df["Symbol"] = [symbol for i in df.index]
    except:
        print(symbol)
        continue
    VALUE = pd.concat([VALUE,df], ignore_index=True)
    progress_bar(CURRENT,TOTAL,text="Gom Giá")

VALUE.to_csv(f"{FU.PATH_MAIN_CURRENT}/VALUE_ARG.csv",index=False)