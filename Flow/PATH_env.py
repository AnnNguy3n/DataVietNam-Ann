import _lib_path_
import datetime
from VAR_GLOBAL_CONFIG import FOLDER_DATALAKE, FOLDER_DATA_WH

PATH_Data = f"{FOLDER_DATALAKE}/Ingestion"

day,month,year=0,0,0
if day != 0:
    date = datetime.datetime(year,month,day)
else:
    date = datetime.datetime.today()
    t = date.weekday()
    if t % 2 == 1:
        date = date - datetime.timedelta(days=1)

class PATH_ENV():
    '''
    Create Path for Data
    '''
    def __init__(self,Type_,date=date,RealDay=True):
        '''
        Type_: Loại data \n
        date: Ngày \n
        RealDay: True: Ngày thực, False: Ngày thực tế \n
        CloseObject: Các đối tượng cần lấy giá \n
        DividendObject: Các đối tượng cần lấy cổ tức \n
        DividendPartObject: Các đối tượng cần lấy cổ tức chi tiết \n
        FinancialObject: Các đối tượng cần lấy tài chính \n
        FinancialPartObject: Các đối tượng cần lấy tài chính chi tiết \n
        VolumeObject: Các đối tượng cần lấy khối lượng \n
        VolumePartObject: Các đối tượng cần lấy khối lượng chi tiết \n
        Phase: Các giai đoạn \n
        Temp: Thư mục tạm \n
        '''
        if RealDay == True:
            self.DateCurrent = date
            self.DayCurrent= date.strftime("%Y-%m-%d")
        else:
            self.DayCurrent = date
        self.setTypeForder(Type_)
        self.CloseObject = ["CafeF","StockBiz"]
        self.DividendObject = ["CafeF","VietStock"]
        self.DividendPartObject = ["CashDividend","BonusShare","StockDividend"]
        self.FinancialObject = ["CafeF","VietStock"]
        self.Type_Time = ["Year","Quarter"]
        self.FinancialPartObject = ["BalanceSheet","IncomeStatement","CashFlowDirect","CashFlowInDirect"]
        self.VolumeObject = ["CafeF","VietStock","TVSI"]
        self.VolumePartObject = ["TreasuryShares","VolumeAdditionailEvents","VolumeNow"]
        self.Phase = [f"F{i}" for i in range(4)]
        self.Temp = "Temp"

    def joinPath(self,*arg):
        '''
        Nối các đường dẫn thành thư mục \n
        Input: *arg: các đường dẫn \n
        Output: đường dẫn nối'''
        arr = []
        for i in arg:
            if i != "":
                arr.append(i)
        return "/".join(arr)

    def setTypeForder(self,Type):
        '''
        Chọn loại thư mục \n
        Input: Type: Loại thư mục \n
        Output: None'''
        if Type == "Ingestion":
            PATH_Data = f"{FOLDER_DATALAKE}/Ingestion"
        elif Type == "Raw_VIS":
            PATH_Data = f"{FOLDER_DATALAKE}/Raw_VIS"
        elif Type == "WH":
            PATH_Data = FOLDER_DATA_WH
            self.PATH_MAIN = PATH_Data
            self.PATH_CLOSE = self.joinPath(self.PATH_MAIN,"Close")
            return
        else:
            PATH_Data = f"{FOLDER_DATALAKE}/Data_Rule"
        self.PATH_MAIN = PATH_Data
        self.PATH_MAIN_CURRENT = self.joinPath(self.PATH_MAIN,self.DayCurrent)
        self.PATH_CLOSE = self.joinPath(self.PATH_MAIN,self.DayCurrent,"Close")
        self.PATH_COMPARE = self.joinPath(self.PATH_MAIN,self.DayCurrent,"Compare")
        self.PATH_FINANCIAL = self.joinPath(self.PATH_MAIN,self.DayCurrent,"Financial")
        self.PATH_DIVIDEND = self.joinPath(self.PATH_MAIN,self.DayCurrent,"Dividend")
        self.PATH_VOLUME = self.joinPath(self.PATH_MAIN,self.DayCurrent,"Volume")
        self.REAl_DAY = self.joinPath(self.PATH_MAIN,"RealDay")
        self.REAl_DAY_CLOSE = self.joinPath(self.REAl_DAY,'Close')
        self.REAl_DAY_IBOARD = self.joinPath(self.REAl_DAY,'RawIBoardSSI')