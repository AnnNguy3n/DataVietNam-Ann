import _lib_path_
from Crawler.VietStock import VietStock_Financial
from CONFIG import CYCLE_TYPE
from _0_CreateFolder import FOLDER_INGESTION_NOW, CYCLE_KEY
from _1_ListCompany import LISTCOM_PATH
import pandas as pd


if __name__ == "__main__":
    vs = VietStock_Financial(cycle_type=CYCLE_TYPE)
    dict_data = vs.get_all_data_financial(pd.read_csv(LISTCOM_PATH))
    for key in dict_data.keys():
        dict_data[key].to_csv(f"{FOLDER_INGESTION_NOW}/Financial/VietStock/{CYCLE_KEY}/{key}.csv", index=False)
