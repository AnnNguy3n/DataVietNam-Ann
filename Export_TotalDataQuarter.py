from VAR_GLOBAL_CONFIG import FOLDER_DATALAKE, END_DAY_UPDATE, QUARTER_KEY
import pandas as pd

FOLDER = f"{FOLDER_DATALAKE}/Raw_VIS/{END_DAY_UPDATE}"

qkey = QUARTER_KEY.replace("/", "_")

Financial = pd.read_excel(f"{FOLDER}/FINANCIAL_{qkey}.xlsx")
Financial.sort_values("Symbol", ascending=True, inplace=True, ignore_index=True)

# Date_Buy
BUY = pd.read_excel(f"{FOLDER}/BUY.xlsx")
BUY.sort_values("Symbol", ascending=True, inplace=True, ignore_index=True)
assert (BUY["Symbol"] == Financial["Symbol"]).all()
Financial["Date_Buy"] = BUY["Date_Buy"]

# Time_Investment_Number
m, y = map(int, QUARTER_KEY.split("/"))
Time_Investment_Number = y * 4 + m - 8000
Financial["Time_Investment_Number"] = Time_Investment_Number

# Buy
Financial["BUY"] = BUY["Close"]

# VolumeARG and ValueARG
Financial["VolumeARG"] = pd.NA
Financial["ValueARG"] = pd.NA

VolARG_ValARG = pd.read_excel(f"{FOLDER}/ValARG_VolARG.xlsx")
VolARG_ValARG.set_index("Symbol", inplace=True)
for i in range(len(Financial)):
    sym = Financial.loc[i, "Symbol"]
    try:
        Financial.loc[i, ["VolumeARG", "ValueARG"]] = VolARG_ValARG.loc[sym, ["VolumeARG", "ValueARG"]]
    except:
        print("Khong tim thay VolumeARG, ValueARG cua", sym)

# SELL = 0
Financial["SELL"] = 0.0

# Exchange
ListCom = pd.read_csv(f"{FOLDER}/List_company.csv")
ListCom.sort_values("Mã CK▲", inplace=True, ascending=True, ignore_index=True)
assert (ListCom["Mã CK▲"] == Financial["Symbol"]).all()
Financial["Exchange"] = ListCom["Sàn"]

# Volume
VOLUME = pd.read_excel(f"{FOLDER}/Volume.xlsx")
VOLUME.sort_values("Symbol", ascending=True, inplace=True, ignore_index=True)
assert (VOLUME["Symbol"] == Financial["Symbol"]).all()
Financial["Volume"] = VOLUME["Volume"]

# PROFIT = 0
Financial["PROFIT"] = 0.0

# MARKET_CAP
Financial["MARKET_CAP"] = Financial["BUY"] * Financial["Volume"] * 1000.0

#
Financial.to_excel(f"{FOLDER}/QUARTER_{Time_Investment_Number}.xlsx", index=False)
