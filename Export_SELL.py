from VAR_GLOBAL_CONFIG import END_DAY_UPDATE, FOLDER_DATALAKE, START_DAY_UPDATE
from datetime import datetime, timedelta
import os
import pandas as pd

DATE_FORMAT = "%Y-%m-%d"

# Số ngày tối đa xét từ END_DAY_UPDATE trở về trước để tính giá đóng cửa
MAX_DATE = 10

# Ngày bắt đầu lấy dividend (kết thúc dividend ở END_DAY_UPDATE)
START_DATE_DIVIDEND = START_DAY_UPDATE

print(START_DATE_DIVIDEND, END_DAY_UPDATE)

# Giá đóng cửa (chưa có dividend)
start_date = (datetime.strptime(END_DAY_UPDATE, DATE_FORMAT) - timedelta(MAX_DATE)).strftime(DATE_FORMAT)

SYMBOL = pd.read_csv(f"{FOLDER_DATALAKE}/Raw_VIS/{END_DAY_UPDATE}/List_company.csv")["Mã CK▲"].to_list()
PriceCloseForSell = []

for symbol in SYMBOL:
    try:
        df = pd.read_csv(f"{FOLDER_DATALAKE}/Raw_VIS/{END_DAY_UPDATE}/Close/CafeF/F0/{symbol}.csv")
        df["Ngay"] = pd.to_datetime(df["Ngay"], format="%d/%m/%Y").dt.strftime(DATE_FORMAT)
        df = df[(df["Ngay"] <= END_DAY_UPDATE) & (df["Ngay"] >= start_date)]
        if len(df) > 0:
            PriceCloseForSell.append((symbol, float(df.iloc[0]["GiaDongCua"])))
        else:
            PriceCloseForSell.append((symbol, 0.0))
    except Exception as ex:
        PriceCloseForSell.append((symbol, 0.0))
        print(symbol, ex.args)

PriceCloseForSell = pd.DataFrame(PriceCloseForSell, columns=["Symbol", "Close"])
PriceCloseForSell.set_index("Symbol", inplace=True)

# Giá bán (đã kết hợp dividend)
DIVIDEND = pd.read_excel(f"{FOLDER_DATALAKE}/Raw_VIS/{END_DAY_UPDATE}/DIVIDEND.xlsx")


def get_SELL(symbol):
    close = PriceCloseForSell.loc[symbol, "Close"]
    dividend = DIVIDEND[(DIVIDEND["Symbol"] == symbol)
                        & (DIVIDEND["Time"] <= END_DAY_UPDATE)
                        & (DIVIDEND["Time"] >= START_DATE_DIVIDEND)].reset_index(drop=True)
    if len(dividend) == 0:
        return close

    sum = 0.0
    cp = 1
    for i in range(len(dividend)):
        if dividend.loc[i, "Money"] != "NAN":
            tyle = dividend.loc[i, "Money"]
            sum += cp * 10.0 * eval(tyle)
        if dividend.loc[i, "Stock"] != "NAN":
            tyle = dividend.loc[i, "Stock"]
            cp = cp * 1.0 / eval(tyle) + cp

    return close * cp + sum

PriceCloseForSell["SELL"] = pd.NA
for symbol in PriceCloseForSell.index:
    PriceCloseForSell.loc[symbol, "SELL"] = get_SELL(symbol)

PriceCloseForSell.to_excel(f"{FOLDER_DATALAKE}/Raw_VIS/{END_DAY_UPDATE}/SELL.xlsx", index=False)
