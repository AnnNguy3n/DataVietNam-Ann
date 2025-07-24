import _lib_path_
from Crawler.VietStock import VietStock_Dividend
from CONFIG import LAST_DATE
from _0_CreateFolder import FOLDER_INGESTION_NOW


if __name__ == "__main__":
    vs = VietStock_Dividend(last_date=LAST_DATE)
    df = vs.get_all_dividend_1y()
    df.to_csv(f"{FOLDER_INGESTION_NOW}/Dividend/VietStock/Dividend.csv", index=False)
