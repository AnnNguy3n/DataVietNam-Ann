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

from datetime import datetime, timedelta
DATE_FORMAT = "%Y-%m-%d"

# Xét từ END_DAY_UPDATE trở về trước để lấy số ngày có giao dịch, từ đó tính trung bình
# ValARG và VolARG
MAX_DATE_COUNT_ARG = 30

start_date = (datetime.strptime(END_DAY_UPDATE, DATE_FORMAT) - timedelta(MAX_DATE_COUNT_ARG - 1)).strftime(DATE_FORMAT)

VALUE = VALUE[(VALUE["Time"] >= start_date) & (VALUE["Time"] <= END_DAY_UPDATE)].reset_index(drop=True)

VALUE.to_csv(f"{FU.PATH_MAIN_CURRENT}/VALUE_ARG.csv",index=False)