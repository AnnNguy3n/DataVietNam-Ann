import _lib_path_
import sys

from Flow import Folder
import math
from Flow.PATH_env import PATH_ENV
from Flow.ulis import *
from base.Price import *
from base.Setup import *

Value = pd.DataFrame()
for symbol in SYMBOL:
    try:
        df = pd.read_csv(FU.joinPath(FU.PATH_MAIN_CURRENT,"Close","CafeF","F0",f"{symbol}.csv"))
        df = df[["Ngay","KhoiLuongKhopLenh","GiaTriKhopLenh"]]
        df = df.rename(columns={"Ngay":"Time",
                "KhoiLuongKhopLenh":"VolumeTrading",
                "GiaTriKhopLenh":"ValueTrading"})
        df["Time"] = df["Time"].apply(lambda row: coverTime(row))
        df = df.sort_values(by="Time").reset_index(drop=True)
        df.to_csv(FU.joinPath(FU.PATH_MAIN_CURRENT,"Close","CafeF","F1",f"{symbol}.csv"),index=False)
    except Exception as ex:
        print(symbol, ex.args)