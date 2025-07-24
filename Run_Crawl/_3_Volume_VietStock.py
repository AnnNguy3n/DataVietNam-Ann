import _lib_path_
from Crawler.VietStock import VietStock_Volume
from _0_CreateFolder import FOLDER_INGESTION_NOW
from _1_ListCompany import LISTCOM_PATH
import pandas as pd


if __name__ == "__main__":
    vs = VietStock_Volume()
    dict_data = vs.get_all_data_volume(pd.read_csv(LISTCOM_PATH))
    for key in dict_data.keys():
        dict_data[key].to_csv(f"{FOLDER_INGESTION_NOW}/Volume/VietStock/VolumeNow/{key}.csv", index=False)
