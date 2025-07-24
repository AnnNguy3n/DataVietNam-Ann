from Crawler.Base import Base
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import pandas as pd
# from io import StringIO
import json
import numpy as np
import time
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)


CAFEF_URL = {
    "BALANCE_SHEET"     : "https://cafef.vn/du-lieu/bao-cao-tai-chinh/{}/bsheet/{}/bao-cao-tai-chinh-.chn",
    "INCOME_STATEMENT"  : "https://cafef.vn/du-lieu/bao-cao-tai-chinh/{}/incsta/{}/ket-qua-hoat-dong-kinh-doanh-.chn",
    "CASHFLOW_INDIRECT" : "https://cafef.vn/du-lieu/bao-cao-tai-chinh/{}/cashflow/{}/luu-chuyen-tien-te-gian-tiep-.chn",
    "CASHFLOW_DIRECT"   : "https://cafef.vn/du-lieu/bao-cao-tai-chinh/{}/cashflowdirect/{}/luu-chuyen-tien-te-truc-tiep-.chn",
    "VOLUME"            : "https://cafef.vn/du-lieu/hose/{}.chn",
    "PRICE_CLOSE"       : "https://s.cafef.vn/Ajax/PageNew/DataHistory/PriceHistory.ashx",
}


class CafeF_Crawler(Base):
    def __init__(self, crawler_type="S"):
        super().__init__(crawler_type)


class CafeF_Financial(CafeF_Crawler):
    def __init__(self, crawler_type="R", cycle_type = "quý 1", year = "now", num_fetch_cycles = 3):
        assert crawler_type == "R"
        super().__init__(crawler_type)

        cycle_type = ["năm", "quý 1", "quý 2", "quý 3", "quý 4"].index(cycle_type)
        if type(year) == str and year == "now":
            year = datetime.now().year
        elif type(year) == int:
            pass
        else:
            raise Exception ("Invalid year type, must be str('now') or int")
        self.year = year
        self.cycle_type = cycle_type
        self.num_fetch_cycles = num_fetch_cycles

    def get_data_1_com(self, symbol, list_report_type = ["BALANCE_SHEET", "INCOME_STATEMENT", "CASHFLOW_INDIRECT", "CASHFLOW_DIRECT"]):
        data = {}
        for report_type in list_report_type:
            data[report_type] = {}
            year = self.year
            cycle_type = self.cycle_type
            for i in range(self.num_fetch_cycles):
                if i != 0:
                    if cycle_type == 0:
                        year -= 1
                    else:
                        if cycle_type == 1:
                            cycle_type = 4
                            year -= 1
                        else:
                            cycle_type -= 1

                time_in_url = f"{year}/{cycle_type}/0/0"
                url = CAFEF_URL[report_type].format(symbol.lower(), time_in_url)
                for _ in range(10):
                    try:
                        r = self.session.get(url)
                        assert r.status_code == 200
                        break
                    except: pass
                else: raise

                soup = BeautifulSoup(r.content, "html.parser")
                table = soup.find('table', {'id': 'tblGridData'})
                header = pd.read_html(str(table), displayed_only=False)[0]
                time_to_get = np.array_str(header[4].values)

                table = soup.find('table', {'id': 'tableContent'})
                financial = pd.read_html(str(table), displayed_only=False)[0]
                df = financial[[0, 4]].copy()
                df.dropna(subset=[0], inplace=True)
                df.rename(columns = {0 : "field", 4 : time_to_get}, inplace=True)

                data[report_type][df.columns[1]] = df.to_dict("records")

        return data


class CafeF_Volume(CafeF_Crawler):
    def __init__(self, crawler_type="R"):
        assert crawler_type == "R"
        super().__init__(crawler_type)

    def get_volume_1_com(self, symbol):
        try:
            for _ in range(10):
                try:
                    r = self.session.get(CAFEF_URL["VOLUME"].format(symbol.lower()))
                    assert r.status_code == 200
                    soup = BeautifulSoup(r.content, "html.parser")
                    volume_element = soup.find_all("div", {"class":"dlt-right-half"})
                    assert len(volume_element) == 1
                    ul = volume_element[0].find_all("ul")
                    assert len(ul) == 1
                    list_title = []
                    list_value = []
                    for li in ul[0].find_all("li"):
                        list_title.append(li.find("div", {"class": "l"}).b.text)
                        list_value.append(li.find("div", {"class": "r"}).text.strip().replace(",", ""))

                    return pd.DataFrame({"Title": list_title, "Value": list_value})
                except: pass
            else: raise
        except:
            raise Exception(f"Khong tai duoc du lieu")


class CafeF_Dividend(CafeF_Crawler):
    def __init__(self, crawler_type="R"):
        assert crawler_type == "R"
        super().__init__(crawler_type)

    def get_dividend_1_com(self, symbol):
        try:
            for _ in range(10):
                try:
                    r = self.session.get(CAFEF_URL["VOLUME"].format(symbol.lower()))
                    assert r.status_code == 200
                    soup = BeautifulSoup(r.content, "html.parser")
                    view_more_element = soup.find_all("div", {"class": "tt view-more-btn"})
                    assert len(view_more_element) == 1

                    infor = view_more_element[0].find_all("div", {"class": "middle"})
                    assert len(infor) == 1
                    tmp_txt = infor[0].text
                    return pd.DataFrame({"New": tmp_txt.split("-")})
                except: pass
            else: raise
        except:
            raise Exception(f"Khong tai duoc du lieu")


class CafeF_PriceClose(CafeF_Crawler):
    def __init__(self, crawler_type="R", date_range = 183, last_date = "now"):
        """
        day_end: YYYY-MM-DD
        """
        assert crawler_type == "R"
        super().__init__(crawler_type)

        if last_date == "now":
            self.last_date = datetime.now().strftime("%Y-%m-%d")
        else:
            # Check format
            datetime.strptime(last_date, "%Y-%m-%d")
            self.last_date = last_date

        self.start_date = (
            datetime.strptime(self.last_date, "%Y-%m-%d") -
            timedelta(days=date_range)
        ).strftime("%Y-%m-%d")

    def get_priceClose_1_com(self, symbol):
        params = {
            "Symbol": symbol.lower(),
            "StartDate": self.start_date,
            "EndDate": self.last_date,
            "PageIndex": "1",
            "PageSize": "100000"
        }

        for _ in range(10):
            try:
                r = self.session.get(CAFEF_URL["PRICE_CLOSE"], params=params)
                assert r.status_code == 200
                content = json.loads(r.content)
                data = content["Data"]["Data"]
                return pd.DataFrame(data)
            except: pass
        else:
            raise Exception(f"Khong tai duoc du lieu")
