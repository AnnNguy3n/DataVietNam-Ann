import _lib_path_
import datetime
import sys
import pandas as pd
from Flow import Folder
from base.Setup import *
from VAR_GLOBAL_CONFIG import FOLDER_DATALAKE

# FC = Folder.FolderCrawl()

PRICE = pd.read_json(f"{FU.PATH_MAIN_CURRENT}/PRICE.json").rename(columns={"Date":"Time"})

for day_ in range(13,23):
    day = datetime.datetime(2023,3,day_).strftime('%Y-%m-%d')
    data_day = PRICE[PRICE["Time"]==day]
    data_day["Price"] = data_day["Close"]*1000
    data_day.to_csv(f'{FOLDER_DATALAKE}/Ingestion/RealDay/CloseFix/{day}.csv',index=False)
