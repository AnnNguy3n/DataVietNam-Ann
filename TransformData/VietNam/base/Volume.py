import pandas as pd
import numpy as np
class Volume():
    '''
    Lấy dữ liệu về Volume \n
    '''
    def __init__(self,dict_path_) -> None:
        ''' Khởi tạo Volume \n
        Input: \n
        dict_path_: đường dẫn \n'''
        self.path_object = dict_path_
        pass

    def find_volume_withtile(self,data,title):
        '''
        Tìm giá trị theo tiêu đề \n
        Input: \n
        data: DataFrame \n
        title: tiêu đề \n
        Output: giá trị \n
        '''
        t = list(data['Title']).index(title)
        value = list(data['Value'])[t]
        return value

class VolumeCafeF(Volume):
    def __init__(self, dict_path_) -> None:
        '''
        Lấy dữ liệu về Volume \n
        Input: \n
        dict_path_: đường dẫn \n
        '''
        super().__init__(dict_path_)

    def getVolumeNow(self,symbol):
        '''
        Lấy dữ liệu về Volume hiện tại \n
        Input: \n
        symbol: mã cổ phiếu \n
        Output: giá trị \n

        '''
        try:
            data = pd.read_csv(f'{self.path_object["F0"]["VolumeNow"]}/{symbol}.csv')
            # print(f'{self.path_object["F0"]["VolumeNow"]}/{symbol}.csv',999999)
            volume_luuhanh = self.find_volume_withtile(data,"KLCP đang lưu hành:")
            return float(volume_luuhanh)
        except Exception as ex:
            print(symbol, ex.args, "getVolumeNow CF", self.getVolumeNow)
            return np.nan

class VolumeVietStock(Volume):
    '''
    Volume VietStock \n'''
    def __init__(self, dict_path_) -> None:
        '''
        Khởi tạo Volume VietStock\n
        Input: \n
        dict_path_: đường dẫn \n'''
        super().__init__(dict_path_)
    def rename(self,data):
        '''
        Đổi tên cột \n
        Input: \n
        data: DataFrame \n
        Output: DataFrame \n'''
        data.columns = ["Title",'Value']
        return data
    def getVolumeNow(self,symbol):
        '''
        Lấy dữ liệu về Volume hiện tại \n
        Input: \n
        symbol: mã cổ phiếu \n
        Output: giá trị \n
        '''
        try:
            data = pd.read_csv(f'{self.path_object["F0"]["VolumeNow"]}/{symbol}.csv')
            data = self.rename(data)
            volume_luuhanh = self.find_volume_withtile(data,"KL Cổ phiếu đang lưu hành")
            return float(volume_luuhanh.replace(",",""))
        except Exception as ex:
            print(symbol, ex.args, "getVolumeNow VS", self.getVolumeNow)
            return np.nan