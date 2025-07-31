import _lib_path_
import math
import pandas as pd
import sys
import numpy as np

from VAR_GLOBAL_CONFIG import *


def check_dau(a):
    if a >=0:
        return 1
    else:
        return -1


class Compare():
    '''
    So sánh đối chiếu các nguồn dữ liệu \n
    '''
    def __init__(self) -> None:
        pass

    def CompareNumber(self,a,b):
        '''
        So sánh 2 số \n
        Input: a: số thứ nhất \n
        b: số thứ hai \n
        Output:
        1: a = b \n
        0: a != b \n
        2: a hoặc b là NaN \n
        N: a và b là NaN \n'''
        if math.isnan(a) and math.isnan(b):
            return "N"
        if math.isnan(a) or math.isnan(b):
            return "2"
        if round(a)-round(b) == 0:
            return "1"
        else:
            return "0"
    def compare_2_block(self,a,b,s_a=1,s_b=1,field=""):
        '''
        So sánh 2 số \n
        Input: \n
        a: số thứ nhất \n
        b: số thứ hai \n
        s_a: đơn vị tiền của a \n
        s_b: đơn vị tiền của b \n
        field: tên cột \n
        Output: \n
        1: a = b \n
        0: a != b \n
        2: a hoặc b là NaN \n
        N: a và b là NaN \n
        '''
        if math.isnan(a) and math.isnan(b):
            return "N"
        if math.isnan(a) or math.isnan(b):
            return "2"
        if field in ['Basic earnings per share','Diluted earnings per share']:
            s_a = 1
            s_b = 1
        dau_a = check_dau(a)
        dau_b = check_dau(b)
        a = abs(a)
        b = abs(b)
        x,y = a,b
        x = a/s_a+0.0000001
        y = b/s_b+0.0000001
        x = dau_a*x
        y = dau_b*y
        if round(x)-round(y) == 0:
            return "1"
        else:
            return "0"
    def compare_2_string(self,a,b):
        '''
        So sánh 2 chuỗi \n
        Input: \n
        a: chuỗi thứ nhất \n
        b: chuỗi thứ hai \n
        Output: \n
        1: a = b \n
        0: a != b \n
        2: a hoặc b là NaN \n
        N: a và b là NaN \n

        '''
        # if math.isnan(a) and math.isnan(b):
        #     return "N"
        # if math.isnan(a) or math.isnan(b):
        #     return "2"
        if a == b:
            return "1"
        else:
            return "0"

class CompareFinancial(Compare):
    '''
    So sánh đối chiếu các nguồn dữ liệu tài chính \n
    '''
    def __init__(self,symbol,path_,type_time,data_field) -> None:
        '''
        Input: \n
        symbol: mã cổ phiếu \n
        path_: đường dẫn \n
        type_time: loại thời gian \n
        data_field: dữ liệu cần lấy \n
        Output: None \n
        '''
        self.symbol = symbol
        self.path_main = path_
        self.type_time = type_time
        self.dict_data={
            "CF":{"path":[self.path_main+f"/Financial/CafeF/F3/{type_time}/"],"company":pd.DataFrame({}),"money":1},
            "VS":{"path":[self.path_main+f"/Financial/VietStock/F3/{type_time}/"],"company":pd.DataFrame({}),"money":1}
        }
        self.getDataField(data_field)
        self.getData()


    def getDataField(self,data_field):
        '''
        Lấy dữ liệu cần lấy \n
        Input: data_field: dữ liệu cần lấy \n
        Output: None \n'''
        self.data_field = data_field[["Feature"]]

    def getData(self):
        '''
        Lấy dữ liệu từ các nguồn \n
        Input: None \n
        Output: None \n
        '''
        for key in self.dict_data.keys():
            try:
                df = pd.read_csv("{}/{}.csv".format(self.dict_data[key]["path"][0],self.symbol))
                for column in df.columns[1:]:
                    df[column] = df[column].astype(float)
            except:
                df = self.data_field.copy()
                df[QUARTER_KEY] = [np.nan for i in df.index]
            df = pd.merge(self.data_field,df,on=["Feature"],how="left")
            self.dict_data[key]["company"] = df

    def getTime(self,data):
        '''
        Lấy danh sách thời gian \n
        Input: data: dữ liệu \n
        Output: list thời gian \n'''
        return data.columns[1:]

    def get_field(self,key_1,key_2):
        '''
        Lấy dữ liệu từ 2 nguồn \n
        Input: \n
        key_1: nguồn thứ nhất \n
        key_2: nguồn thứ hai \n
        Output: DataFrame \n
        '''
        df = pd.merge( self.dict_data[key_1]["company"], self.dict_data[key_2]["company"],on=["Feature"],how="inner")
        list_year = self.getTime(self.dict_data["CF"]["company"])
        s_a,s_b =  self.dict_data[key_1]["money"], self.dict_data[key_2]["money"]
        for year in list_year:
            df["Compare"] = df.apply(lambda row: self.compare_2_block(row[f"{year}_x"],row[f"{year}_y"],s_a,s_b,row["Feature"]),axis=1)
        return df