from Crawler.Base import Base
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
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
    "PDF"               : "https://cafef.vn/du-lieu/Ajax/CongTy/BaoCaoTaiChinh.aspx?sym={}"
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
            # raise Exception(f"Khong tai duoc du lieu")
            temp = CafeF_Crawler()
            try:
                for trial in range(3):
                    try:
                        for _ in range(10):
                            try:
                                temp.driver.get(CAFEF_URL["VOLUME"].format(symbol.lower()))
                                if temp.driver.title.strip() not in ["Trang thông báo lỗi 404", "Lỗi kết nối"]:
                                    temp.scroll_to_bottom()
                                    time.sleep(15)
                                else: raise
                                soup = BeautifulSoup(temp.driver.page_source, "html.parser")

                                list_info = soup.find_all("div", {
                                    "class": "table-right",
                                    "id": "transaction-information-table-right"
                                })

                                assert len(list_info) == 1
                                break
                            except: pass
                        else: raise

                        list_title = []
                        list_value = []
                        for item in list_info[0].find_all("div", {"class": "table-right-item"}):
                            list_p = item.find_all("p")
                            assert len(list_p) == 2
                            list_title.append(list_p[0].text)
                            list_value.append(list_p[1].text.strip().replace(",", ""))

                        def find_and_replace_keyword(key, key_after):
                            count = 0
                            idx = None
                            for i in range(len(list_title)):
                                if list_title[i] == key:
                                    count += 1
                                    list_title[i] = key_after
                                    idx = i
                            assert count == 1, list_title
                            return idx

                        vh = find_and_replace_keyword("Vốn hóa thị trường  (tỷ đồng)", "Vốn hóa thị trường")
                        p10 = find_and_replace_keyword("KLGD khớp lệnh TB 10 phiên", "KLGD khớp lệnh trung bình 10 phiên:")
                        ny = find_and_replace_keyword("KLCP đang niêm yết", "KLCP đang niêm yết:")
                        lh = find_and_replace_keyword("KLCP lưu hành", "KLCP đang lưu hành:")
                        # temp.quit_crawler()
                        return pd.DataFrame({"Title": [list_title[i] for i in [p10, ny, lh, vh]], "Value": [list_value[i] for i in [p10, ny, lh, vh]]})
                    except Exception as e: ex = e
            except Exception as ex:
                raise Exception(ex)
            finally:
                temp.quit_crawler()


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
            # raise Exception(f"Khong tai duoc du lieu")
            temp = CafeF_Crawler()
            try:
                for trial in range(3):
                    try:
                        for _ in range(10):
                            try:
                                temp.driver.get(CAFEF_URL["VOLUME"].format(symbol.lower()))
                                if temp.driver.title.strip() not in ["Trang thông báo lỗi 404", "Lỗi kết nối"]:
                                    temp.scroll_to_bottom()
                                    time.sleep(15)
                                else: raise
                                soup = BeautifulSoup(temp.driver.page_source, "html.parser")

                                list_dividend = soup.find_all("div", {"id": "list-dividend-payment"})
                                assert len(list_dividend) == 1
                                break
                            except: pass
                        else: raise

                        list_date = []
                        list_info = []

                        for item in list_dividend[0].find_all("div", {"class": "dividend-payment-history-item"}):
                            divs = item.find_all("div")
                            assert len(divs) == 2
                            date = divs[0].text.strip()
                            datetime.strptime(date, "%d/%m/%Y")
                            list_date.append(date)

                            infos = divs[1].find_all("p")
                            temp_info = []
                            for info in infos:
                                temp_info.append(info.text)

                            list_info.append(temp_info)

                        # temp.quit_crawler()
                        if len(list_date) == 0:
                            return pd.DataFrame({"New": ["\n"]})
                        else:
                            list_record = []
                            list_record.append('''
                            ''')
                            for i in range(len(list_date)):
                                record = f" {list_date[i]}: "
                                record += "                           ".join(list_info[i])
                                list_record.append(record)

                            return pd.DataFrame({"New": list_record})
                    except Exception as e: ex = e
            except Exception as ex:
                raise Exception(ex)
            finally:
                temp.quit_crawler()


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


class CafeF_PDF(CafeF_Crawler):
    def __init__(self, type_crawler="R", type_cycle = "quý 1", year = "now"):
        assert type_crawler == "R"
        super().__init__(type_crawler)

        type_cycle = ["năm", "quý 1", "quý 2", "quý 3", "quý 4"].index(type_cycle)
        if type(year) == str and year == "now":
            year = datetime.now().year
        elif type(year) == int:
            pass
        else:
            raise Exception ("Invalid year type, must be str('now') or int")
        self.year = year
        self.type_cycle = type_cycle
        self.text_filter = ["CN", "Q1", "Q2", "Q3", "Q4"][type_cycle] + f"/{year}"

    def get_pdf_1_com(self, symbol):
        for _ in range(10):
            try:
                r = self.session.get(CAFEF_URL["PDF"].format(symbol))
                assert r.status_code == 200
                break
            except: pass
        else:
            raise Exception("Timed out: get_pdf_1_com")

        soup = BeautifulSoup(r.content, "html.parser")
        tables = soup.find_all("table")
        assert len(tables) == 2

        df = pd.read_html(StringIO(tables[1].prettify()))[0]
        assert (df.loc[0].values == ['Loại báo cáo', 'Thời gian', 'Tải về']).all()
        df.columns = df.loc[0]
        df = df.drop(index=0).reset_index(drop=True)

        list_download_url = []
        list_tr = tables[1].find_all("tr")
        assert len(list_tr) == df.shape[0] + 1

        for tr in list_tr[1:]:
            list_td = tr.find_all("td")
            assert len(list_td) == 3
            list_a = list_td[2].find_all("a")
            assert len(list_a) == 1

            list_download_url.append(list_a[0]["href"])

        df["Tải về"] = list_download_url

        temp = df[df["Thời gian"] == self.text_filter].reset_index(drop=True)

        # return dict(zip(temp["Loại báo cáo"], temp["Tải về"]))
        return temp
