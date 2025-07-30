from VAR_GLOBAL_CONFIG import END_DAY_UPDATE, FOLDER_DATALAKE
import pandas as pd

DATE_FORMAT = "%Y-%m-%d"

print(END_DAY_UPDATE)


#
SYMBOL = pd.read_csv(f"{FOLDER_DATALAKE}/Raw_VIS/{END_DAY_UPDATE}/List_company.csv")["Mã CK▲"].to_list()
PriceCloseForBUY = []

for symbol in SYMBOL:
    try:
        df = pd.read_csv(f"{FOLDER_DATALAKE}/Raw_VIS/{END_DAY_UPDATE}/Close/CafeF/F0/{symbol}.csv")
        df["Ngay"] = pd.to_datetime(df["Ngay"], format="%d/%m/%Y").dt.strftime(DATE_FORMAT)
        df = df[(df["Ngay"] == END_DAY_UPDATE)]
        if len(df) > 0:
            PriceCloseForBUY.append((symbol, float(df.iloc[0]["GiaDongCua"]), df.iloc[0]["Ngay"]))
        else:
            PriceCloseForBUY.append((symbol, 0.0, pd.NA))
    except Exception as ex:
        PriceCloseForBUY.append((symbol, 0.0))
        print(symbol, ex.args)

PriceCloseForBUY = pd.DataFrame(PriceCloseForBUY, columns=["Symbol", "Close", "Date_Buy"])
PriceCloseForBUY.to_excel(f"{FOLDER_DATALAKE}/Raw_VIS/{END_DAY_UPDATE}/BUY.xlsx", index=False)