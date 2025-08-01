import _lib_path_
import pandas as pd
import sys
import warnings
warnings.filterwarnings("ignore")

from base import Compare
from base.Financial import CafeF,VietStock
from base.PATH_UPDATE import *
from base.Setup import *
# Type_Time = "Year"
Type_Time = "Quarter"
# CafeF
# SYMBOL = ["AAA"]
df_check_list = pd.DataFrame()
def transform(symbol,field):
    '''
    Chuyển đổi data từ CafeF và VietStock\n
    Input:\n
    symbol: mã cổ phiếu\n
    field: tên loại thời gian \n
    Output:\n
    df_check_list: DataFrame
    '''
    global df_check_list
    CF = CafeF(dict_path_cf)
    cf = CF.run(symbol,field)
    VS = VietStock(dict_path_vs)
    vs = VS.run(symbol,field)
    df = pd.DataFrame({ "Symbol": [symbol],
                        "Type_Time": [field],
                        "CafeF": [cf],
                        "VietStock":[vs]})
    df_check_list = pd.concat([df_check_list,df],ignore_index=True)
    return df_check_list

for ii, symbol in enumerate(SYMBOL):
    transform(symbol,"Quarter")
    print("Done", ii, symbol)

df_check_list.to_excel(FU.joinPath(FU.PATH_COMPARE,f"Financial_{Type_Time}_CheckList.xlsx"))

def setup_Feature(type_time):
    '''
    Lấy dữ liệu cần lấy \n
    Input: \n
    type_time: loại thời gian \n
    Output: \n
    data_field: dữ liệu cần lấy \n'''
    if type_time == "Year":
        sheet_name = "Total"
    else:
        sheet_name = "Quarter"
    data_field = pd.read_excel(FILE_FEATURE,sheet_name=sheet_name)
    data_field = data_field.rename(columns={"column":"Feature"})
    return data_field

def RunCompare(type_time):
    '''
    Chạy so sánh \n
    Input: \n
    type_time: loại thời gian \n
    Output: None \n
    '''
    can_t_compare = []
    data_field = setup_Feature(type_time)
    for symbol in SYMBOL:
        try:
            C = Compare.CompareFinancial(symbol,PATH_FT,type_time,data_field)
            data = C.get_field("CF","VS")
            data.to_csv(FU.joinPath(FU.PATH_COMPARE,"Financial",type_time,f"{symbol}.csv"),index=False)
            print('perfect')
        except Exception as ex:
            print(symbol, ex.args)
            can_t_compare.append(symbol)
    pd.DataFrame({"Error_Compare":can_t_compare}).to_excel(FU.joinPath(FU.PATH_COMPARE,"Error",f"{type_time}.xlsx"),index=False)

RunCompare("Quarter")
