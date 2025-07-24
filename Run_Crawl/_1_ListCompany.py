import _lib_path_
from Crawler.VietStock import VietStock_ListCompany
from _0_CreateFolder import FOLDER_INGESTION_NOW


LISTCOM_PATH = f"{FOLDER_INGESTION_NOW}/List_company.csv"


if __name__ == "__main__":
    vs = VietStock_ListCompany()
    df = vs.get_listCompany(
        exclude_sectors=["tài chính"],
        list_exchange=["hose", "hnx", "upcom"]
    )
    df.to_csv(LISTCOM_PATH, index=False)
