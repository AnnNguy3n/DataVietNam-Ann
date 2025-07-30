import _lib_path_
import datetime
import json
import math
import pandas as pd
import re
import os
import numpy as np
pd.options.mode.chained_assignment = None  # default='warn'
import sys
from VAR_GLOBAL_CONFIG import *

class TransForm():
    '''
    Chuyển đổi dữ liệu từ dạng F0 sang F3 \n'''
    def __init__(self,dict_path_) -> None:
        self.path_object = dict_path_
        self.time = []
        pass
    def replace_NaN_0(self,df):
        '''
        Thay thế NaN bằng 0 \n
        Input: df: DataFrame \n
        Output: DataFrame \n'''
        df = df.dropna(axis=1, how='all')
        df = df.fillna(0)
        for column in self.time:
            if not(column in df.columns):
                df[column]=[np.nan for i in df["Feature"]]
        return df

    def getTime(self,type_time):
        '''
        Lấy thời gian \n
        Input: type_time: loại thời gian \n
        Output: thời gian \n'''
        if type_time == "Year":
            return YEAR_KEY
        elif type_time == "Quarter":
            return QUARTER_KEY

class CafeF(TransForm):
    '''
    Chuyển đổi dữ liệu từ dạng F0 sang F3 nguồn CafeF\n

    '''
    def __init__(self,dict_path_cf) -> None:
        '''
        Khởi tạo \n
        Input:\n
        self.data_field: dữ liệu trường cần lấy \n
        self.data_field_default_year: dữ liệu trường mặc định năm \n
        self.data_field_default_quarter: dữ liệu trường mặc định quý \n
        dict_path_cf: đường dẫn \n
        Output: None \n
        '''
        super().__init__(dict_path_cf)
        df = pd.read_excel(FILE_FEATURE,sheet_name="CafeF")
        df = df.rename(columns={"VIS_Raw_F1":"field"})
        self.data_field = df
        df = pd.read_excel(FILE_FEATURE,sheet_name="Total")
        self.data_field_default_year = df
        df = pd.read_excel(FILE_FEATURE,sheet_name="Quarter")
        self.data_field_default_quarter = df

    def CheckData(self,symbol,type_time,time_detail):
        '''
        Kiểm tra dữ liệu đã được crawl chưa \n
        Input: symbol: mã cổ phiếu \n
        type_time: loại thời gian \n
        time_detail: thời gian chi tiết \n
        Output: True: đã được crawl, False: chưa được crawl \n
        '''
        for key in self.path_object["F0"].keys():
            if key.find(type_time) != -1:
                    with open(f'{self.path_object["F0"][key]}/{symbol}.json',encoding='utf8') as j:
                            data1 = json.loads(j.read())
                    if pd.isna(data1[time_detail][1][time_detail]):
                        return False
                    else:
                        return True
        return False


    def Financial_F0_to_F1(self,symbol,type_time):
            '''
            Chuyển đổi dữ liệu từ dạng F0 sang F1 \n
            Input: \n
            symbol: mã cổ phiếu \n
            type_time: loại thời gian \n
            Output: DataFrame \n'''
            data = {}
            for key in self.path_object["F0"].keys():
                if key.find(type_time) != -1:
                    with open(f'{self.path_object["F0"][key]}/{symbol}.json',encoding='utf8') as j:
                            data1 = json.loads(j.read())
                    for key,value in data1.items():
                        try:
                            data[key] = data[key]+data1[key]
                        except:
                            data[key] = []
                            data[key] = data[key]+data1[key]

            temp = pd.DataFrame({"field": []})
            for key in list(data.keys()):
                try:
                    df = pd.DataFrame.from_records(data[key])
                    dict_ = {}
                    for i in df.index:
                        df["field"][i] = ''.join([i for i in df["field"][i] if not i.isdigit()])
                        dict_[df["field"][i]] = 0

                    for i in df.index:
                        dict_[df["field"][i]]+=1
                        df["field"][i] = df["field"][i]+"__"+str(dict_[df["field"][i]])
                        col_key = []

                    for i in df:
                        if i in temp.columns:
                            col_key.append(i)
                    temp = pd.merge(temp, df, on=col_key, how="outer")
                except:
                    pass
            arr = []
            for col in temp.columns:
                try:
                    match = re.findall('([0-9]- [0-9]+)', col)
                    if len(match) <1:
                        match = re.findall('([0-9]-[0-9]+)', col)
                    time = match[0].replace("-","/").replace(" ","")
                    time = time.split("-")
                    time = "-".join([time[i] for i in range(len(time)-1,-1,-1)])
                    arr.append(time)
                except:
                    arr.append(col)
            temp.columns = arr
            temp.to_csv(f'{self.path_object["F1"][type_time]}/{symbol}.csv',index=False)
            return temp

    def Financial_F1_to_F2(self,symbol,type_time):
                '''
                Chuyển đổi dữ liệu từ dạng F1 sang F2 \n
                Input: \n
                symbol: mã cổ phiếu \n
                type_time: loại thời gian \n
                Output: DataFrame \n
                '''
                link ="{}/{}.csv".format(self.path_object["F1"][type_time],symbol)
                data = pd.read_csv(link)
                temp = pd.merge(self.data_field,data, on="field",how="outer")
                temp = temp.drop(columns=["field"])
                for column in temp.columns[1:]:
                    temp[column] = temp[column].astype(float)
                temp = temp.groupby("Feature").max().reset_index()
                temp.columns = [col.replace("]","").replace("[","") for col in temp.columns]
                temp.to_csv(f'{self.path_object["F2"][type_time]}/{symbol}.csv',index=False)
                return temp
    def Financial_F2_to_F3(self,symbol,type_time):
                '''
                Chuyển đổi dữ liệu từ dạng F2 sang F3 \n
                Input: \n
                symbol: mã cổ phiếu \n
                type_time: loại thời gian \n
                Output: DataFrame \n'''
                if type_time == "Year":
                    data_field = self.data_field_default_year
                elif type_time == "Quarter":
                    data_field = self.data_field_default_quarter
                link ="{}/{}.csv".format(self.path_object["F2"][type_time],symbol)
                data = pd.read_csv(link)
                self.time=data.columns[1:]

                data = self.replace_NaN_0(data)

                temp = pd.merge(data_field,data, on="Feature",how="inner")
                temp = temp[[data.columns[0],self.getTime(type_time)]]
                temp.to_csv(f'{self.path_object["F3"][type_time]}/{symbol}.csv',index=False)
                return temp

    def run(self,symbol,type_time):
        '''
        Chạy chuyển đổi dữ liệu \n
        Input: \n
        symbol: mã cổ phiếu \n
        type_time: loại thời gian \n
        Output: True: thành công, False: thất bại \n
        '''
        try:
            self.Financial_F0_to_F1(symbol,type_time)
            self.Financial_F1_to_F2(symbol,type_time)
            self.Financial_F2_to_F3(symbol,type_time)
        except Exception as ex:
            print(symbol,type_time,"CF",ex.args)
            return False
        return True


class VietStock(TransForm):
    '''
    Chuyển đổi dữ liệu từ dạng F0 sang F3 nguồn VietStock\n
    '''
    def __init__(self,dict_path_vs) -> None:
        '''
        Khởi tạo \n
        Input:\n
        self.data_field: dữ liệu trường cần lấy \n
        self.data_field_default_year: dữ liệu trường mặc định năm \n
        self.data_field_default_quarter: dữ liệu trường mặc định quý \n
        dict_path_vs: đường dẫn \n
        Output: None \n
        '''
        super().__init__(dict_path_vs)
        df = pd.read_excel(FILE_FEATURE,sheet_name="VietStock")
        df = df.rename(columns={"VIS_Raw_F1":"field"})
        self.data_field = df
        df = pd.read_excel(FILE_FEATURE,sheet_name="Total")
        self.data_field_default_year = df
        df = pd.read_excel(FILE_FEATURE,sheet_name="Quarter")
        self.data_field_default_quarter = df

    def CheckData(self,symbol,type_time,time_detail):
        '''
        Kiểm tra dữ liệu đã được crawl chưa \n
        Input: symbol: mã cổ phiếu \n
        type_time: loại thời gian \n
        time_detail: thời gian chi tiết \n
        Output: True: đã được crawl, False: chưa được crawl \n
        '''
        for key in self.path_object["F0"].keys():
            if key.find(type_time) != -1:
                path_in = self.path_object["F0"][key]
                if os.path.exists(f'{path_in}/{symbol}.csv'):
                    df = pd.read_csv(f'{path_in}/{symbol}.csv')
                    try:
                        df[time_detail]
                        return True
                    except:
                        return False

                else:
                    return False
    def change_data_BS(self,df_finan):
        '''
        Chuyển đổi dữ liệu báo cáo tài chính cân đối \n
        Input: df_finan: dữ liệu báo cáo tài chính cân đối \n
        Output: dữ liệu báo cáo tài chính cân đối \n
        '''
        first_col = df_finan.columns[0]
        feature_change  = '- Nguyên giá__' + df_finan[first_col].loc[df_finan[df_finan[first_col]=='- Nguyên giá'].index-1]
        feature_change.index = feature_change.index+1
        df_finan[first_col].iloc[df_finan[df_finan[first_col]=='- Nguyên giá'].index] = feature_change

        feature_change  = '- Giá trị hao mòn lũy kế__' + df_finan[first_col].loc[df_finan[df_finan[first_col]=='- Giá trị hao mòn lũy kế '].index-2]
        feature_change.index = feature_change.index+2
        df_finan[first_col].iloc[df_finan[df_finan[first_col]=='- Giá trị hao mòn lũy kế '].index] = feature_change

        feature_change  = '- Giá trị hao mòn lũy kế__' + df_finan[first_col].loc[df_finan[df_finan[first_col]=='- Giá trị hao mòn lũy kế'].index-2]
        feature_change.index = feature_change.index+2
        df_finan[first_col].iloc[df_finan[df_finan[first_col]=='- Giá trị hao mòn lũy kế'].index] = feature_change

        return df_finan

    def Financial_F0_to_F1(self,symbol,type_time):
        '''
        Chuyển đổi dữ liệu từ dạng F0 sang F1 \n
        Input: \n
        symbol: mã cổ phiếu \n
        type_time: loại thời gian \n
        Output: DataFrame \n

        '''
        data = pd.DataFrame({})
        for key in self.path_object["F0"].keys():
            if key.find(type_time) != -1:
                path_in = self.path_object["F0"][key]
                if os.path.exists(f'{path_in}/{symbol}.csv'):
                    df = pd.read_csv(f'{path_in}/{symbol}.csv')
                    df = self.change_data_BS(df)
                    data = pd.concat([data,df],ignore_index=True)
        path_out = self.path_object["F1"][type_time]
        data.to_csv(f'{path_out}/{symbol}.csv', index = False)

    def Financial_F1_to_F2(self,symbol,type_time):
        '''
        Chuyển đổi dữ liệu từ dạng F1 sang F2 \n
        Input: \n
        symbol: mã cổ phiếu \n
        type_time: loại thời gian \n
        Output: DataFrame \n
        '''
        df = pd.read_csv(f'{self.path_object["F1"][type_time]}/{symbol}.csv')
        if len(df.index) == 0:
            return df
        df =df[6:].reset_index(drop = True)
        first_col = df.columns[0]
        df = df.rename(columns = {first_col:'field'})
        df_concat = pd.merge(self.data_field, df, how = 'left', on = ['field'])
        if all(df_concat['field'] == df_concat['field']) == True:
            df = df_concat.drop(columns = ["Ingestion", 'Unnamed: 1', 'Unnamed: 2',"field"])
            df = df.rename(columns={"VIS_Raw_F2":"Feature"})
            df.columns = [col.replace("Q","") for col in df.columns]
            path_out = self.path_object["F2"][type_time]
            df.to_csv(f'{path_out}/{symbol}.csv', index = False)
            return df
        else:
            return False

    def Financial_F2_to_F3(self,symbol,type_time):
            '''
            Chuyển đổi dữ liệu từ dạng F2 sang F3 \n
            Input: \n
            symbol: mã cổ phiếu \n
            type_time: loại thời gian \n
            Output: DataFrame \n
            '''
            if type_time == "Year":
                data_field = self.data_field_default_year
            elif type_time == "Quarter":
                data_field = self.data_field_default_quarter
            link ="{}/{}.csv".format(self.path_object["F2"][type_time],symbol)
            data = pd.read_csv(link)
            self.time=data.columns[1:]

            data = self.replace_NaN_0(data)
            if type_time == "Year":
                data = data.rename(columns={data.columns[1]:data.columns[1][4:]})
            temp = pd.merge(data_field,data, on="Feature",how="inner")
            try:
                temp = temp[[data.columns[0],self.getTime(type_time)]]
            except KeyError:
                temp[self.getTime(type_time)] = [np.NaN for i in temp.index]
                # temp[QUARTER_KEY] = [np.NaN for i in temp.index]
                temp = temp[[data.columns[0],self.getTime(type_time)]]
            temp.to_csv(f'{self.path_object["F3"][type_time]}/{symbol}.csv',index=False)
            return temp
    def run(self,symbol,type_time):
        '''
        Chạy chuyển đổi dữ liệu \n
        Input: \n
        symbol: mã cổ phiếu \n
        type_time: loại thời gian \n
        Output: True: thành công, False: thất bại \n
        '''

        try:
            self.Financial_F0_to_F1(symbol,type_time)
            self.Financial_F1_to_F2(symbol,type_time)
            self.Financial_F2_to_F3(symbol,type_time)
        except Exception as ex:
            print(symbol,type_time,"VS",ex.args)
            return False
        return True
