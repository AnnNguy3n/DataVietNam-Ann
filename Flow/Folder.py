import _lib_path_
from datetime import datetime
import os
from typing import Type
from Flow import PATH_env
import json


class FolderData(PATH_env.PATH_ENV):
    '''
    Create Folder for Data'''
    def __init__(self, Type_, date):
        '''
        Type_: Type of Data \n
        date: date of Data'''
        if len(date) == 0:
            super().__init__(Type_, RealDay=True)
        else:
            super().__init__(Type_, date, RealDay=False)

    def createFolder(self, path):
        '''
        Create Folder \n
        Input: path \n
        Output: path'''
        isExist = os.path.exists(path)
        if not isExist:
            os.makedirs(path)
        return path

    def GetDateUpdate(self, DAY):
        '''
        Get Date Update \n
        Input: DAY: date \n
        Output: date'''
        list_date = os.listdir(self.PATH_MAIN)
        arr = []
        for day in list_date:
            if len(day) == 10:
                arr.append(day)
        arr.sort(reverse=True)
        for i in arr:
            if i <= DAY:
                return i
        return DAY

    def getListPath(self):
        '''
        Get List Path \n
        Output: list path'''
        return os.listdir(self.PATH_MAIN)


class FolderCrawl(FolderData):
    def __init__(self, date=""):
        '''
        Create Folder for Crawl Data \n
        Input: date'''

        super().__init__("Ingestion", date)

    def folderClose(self):
        '''
        Create Folder for Close Data \n
        Input: None \n
        Output: None'''
        path = self.PATH_CLOSE
        self.createFolder(path)
        for obj in self.CloseObject:
            self.createFolder(self.joinPath(path, obj))

    def folderDividend(self):
        '''
        Create Folder for Dividend Data \n
        Input: None \n
        Output: None'''
        path = self.PATH_DIVIDEND
        self.createFolder(path)
        for obj in self.DividendObject:
            if obj == "VietStock":
                pass
                for p_obj in self.DividendPartObject:
                    self.createFolder(self.joinPath(path,obj,p_obj))
            else:
                self.createFolder(self.joinPath(path, obj))

    def folderFinancial(self):
        '''
        Create Folder for Financial Data \n
        Input: None \n
        Output: None'''
        path = self.PATH_FINANCIAL
        self.createFolder(path)
        for obj in self.FinancialObject:
            for t_time in self.Type_Time:
                for p_obj in self.FinancialPartObject:
                    self.createFolder(self.joinPath(path, obj, t_time, p_obj))

    def folderVolume(self):
        '''
        Create Folder for Volume Data \n
        Input: None \n
        Output: None'''

        path = self.PATH_VOLUME
        self.createFolder(path)
        for obj in self.VolumeObject:
            for p_obj in self.VolumePartObject:
                self.createFolder(self.joinPath(path, obj, p_obj))

    def Run_Create_Folder(self):
        '''
        Run Create Folder \n
        '''
        self.folderClose()
        self.folderDividend()
        self.folderFinancial()
        self.folderVolume()


class FolderUpdate(FolderData):
    '''
    Create Folder for Update Data'''
    def __init__(self, date):
        '''
        NeedFolderUpdate: list folder need update \n'''
        super().__init__("Raw_VIS", date=date)
        self.NeedFolderUpdate = []

    def folderClose(self):
        '''
        Create Folder for Close Data \n'''
        path = self.PATH_CLOSE
        # for obj in self.CloseObject:
        #     for PHASE in self.Phase[]:
        self.createFolder(self.joinPath(path, "CafeF", "F0"))
        self.createFolder(self.joinPath(path, "CafeF", "F1"))

    def folderDividend(self):
        '''
        Create Folder for Dividend Data \n
        Input: None \n
        Output: None    '''
        path = self.PATH_DIVIDEND
        self.createFolder(path)
        for obj in self.DividendObject:
            for P_F in self.Phase[:3]:
                if obj == "VietStock" and P_F == "F0":
                    self.createFolder(self.joinPath(path, obj, self.Temp))
                    for p_obj in self.DividendPartObject:
                        self.createFolder(self.joinPath(path, obj, P_F, p_obj))
                else:
                    self.createFolder(self.joinPath(path, obj, P_F))

    def folderFinancial(self):
        '''
        Create Folder for Financial Data \n
        Input: None \n
        Output: None
        '''
        path = self.PATH_FINANCIAL
        self.createFolder(path)
        for obj in self.FinancialObject:
            for P_F in self.Phase:
                for t_time in self.Type_Time:
                    if P_F == "F0":
                        if obj == "VietStock":
                            self.createFolder(self.joinPath(path, obj, self.Temp))
                        for p_o in self.FinancialPartObject:
                            self.createFolder(
                                self.joinPath(path, obj, P_F, t_time, p_o)
                            )
                    else:
                        for p_o in self.FinancialPartObject:
                            self.createFolder(self.joinPath(path, obj, P_F, t_time))

    def folderVolume(self):
        '''
        Create Folder for Volume Data \n
        Input: None \n
        Output: None'''
        path = self.PATH_VOLUME
        for obj in self.VolumeObject:
            for PHASE in self.Phase[:2]:
                for p_obj in self.VolumePartObject:
                    self.createFolder(self.joinPath(path, obj, PHASE, p_obj))

    def folderCompare(self):
        '''
        Create Folder for Compare Data \n
        Input: None \n
        Output: None'''
        path = self.PATH_COMPARE
        for time in self.Type_Time:
            self.createFolder(self.joinPath(path, "Financial", time))
        self.createFolder(self.joinPath(path, "Dividend"))
        self.createFolder(self.joinPath(path, "Error"))

    def Run_Create_Folder(self):
        '''
        Run Create Folder \n
        '''
        self.folderClose()
        self.folderDividend()
        self.folderFinancial()
        self.folderVolume()
        self.folderCompare()


class FolderWH(FolderData):
    '''
    Create Folder for WareHouse Data'''
    def __init__(self, date=""):
        super().__init__("WH", date)
