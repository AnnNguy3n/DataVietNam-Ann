from VAR_GLOBAL_CONFIG import FOLDER_DATALAKE, END_DAY_UPDATE
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm

DATE_FORMAT = "%Y-%m-%d"

# Xét từ END_DAY_UPDATE trở về trước để lấy số ngày có giao dịch, từ đó tính trung bình
# ValARG và VolARG
MAX_DATE_COUNT_ARG = 30

# Số ngày tối thiểu có giao dịch, nếu số ngày ít hơn thì cho = 0
MIN_DATE_COUNT_ARG = 7

df_ARG = pd.read_csv(f"{FOLDER_DATALAKE}/Raw_VIS/{END_DAY_UPDATE}/VALUE_ARG.csv")
start_date = (datetime.strptime(END_DAY_UPDATE, DATE_FORMAT) - timedelta(MAX_DATE_COUNT_ARG)).strftime(DATE_FORMAT)

print(start_date, END_DAY_UPDATE)


#
ValARG_and_VolARG = []
for symbol in tqdm(df_ARG["Symbol"].unique()):
    df = df_ARG[(df_ARG["Symbol"] == symbol)
                & (df_ARG["Time"] <= END_DAY_UPDATE)
                & (df_ARG["Time"] >= start_date)].reset_index(drop=True)
    if len(df) >= MIN_DATE_COUNT_ARG:
        ValARG_and_VolARG.append((
            symbol,
            df["ValueTrading"].mean(),
            df["VolumeTrading"].mean()
        ))

ValARG_and_VolARG = pd.DataFrame(ValARG_and_VolARG, columns=["Symbol", "ValueARG", "VolumeARG"])
ValARG_and_VolARG.to_excel(f"{FOLDER_DATALAKE}/Raw_VIS/{END_DAY_UPDATE}/ValARG_VolARG.xlsx", index=False)
