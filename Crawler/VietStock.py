from Crawler.Base import Base
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
import time
from bs4 import BeautifulSoup
import re
import pandas as pd
# from io import StringIO
import numpy as np
from tqdm import tqdm
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)


VIETSTOCK_URL = {
    "LIST_COMPANY": "https://finance.vietstock.vn/doanh-nghiep-a-z?page=1",
    "FINANCIAL"   : "https://finance.vietstock.vn/truy-xuat-du-lieu/bao-cao-tai-chinh.htm",
    "VOLUME"      : "https://finance.vietstock.vn/{}/ho-so-doanh-nghiep.htm",
    "CASH_DIVIDEND"  : "https://finance.vietstock.vn/lich-su-kien.htm?page={}&from={}&to={}&exchange=-1&tab=1&group=13",
    "BONUS_SHARE"    : "https://finance.vietstock.vn/lich-su-kien.htm?page={}&from={}&to={}&exchange=-1&tab=1&group=14",
    "STOCK_DIVIDEND" : "https://finance.vietstock.vn/lich-su-kien.htm?page={}&from={}&to={}&exchange=-1&tab=1&group=15",
}


class VietStock_Crawler(Base):
    def __init__(self, crawler_type="S"):
        super().__init__(crawler_type)

        self.HEADERS = {"User-Agent": "Mozilla"}

        if crawler_type == "S":
            self.driver.get(VIETSTOCK_URL["LIST_COMPANY"])
            self.driver.maximize_window()

            # Đăng nhập
            self.find_item(By.CLASS_NAME, "navbar-login").find_element(By.CLASS_NAME, "btnlogin-link").click()

            txt_email = self.find_item(By.ID, "txtEmailLogin")
            txt_email.clear()
            txt_email.send_keys("nguyenhuuan.vis@gmail.com")

            txt_passw = self.find_item(By.ID, "passwordLogin")
            txt_passw.clear()
            txt_passw.send_keys("vis11235813")

            self.find_item(By.ID, "Remember").click()
            self.find_item(By.ID, "btnLoginAccount").click()

            # Kiểm tra đăng nhập thành công hay thất bại
            for _ in range(60):
                try:
                    accInfo = self.find_item(By.ID, "btnAccountInfo")
                    assert accInfo.text == "VIS_Ann"
                    break
                except:
                    print("Đang chờ đăng nhập")
                    time.sleep(1)
            else:
                raise Exception("Đăng nhập không thành công")


class VietStock_ListCompany(VietStock_Crawler):
    def __init__(self, crawler_type="S"):
        assert crawler_type == "S"
        super().__init__(crawler_type)
        self.driver.get(VIETSTOCK_URL["LIST_COMPANY"])

        # Chọn pageSize
        for _ in range(60):
            try:
                Select(self.driver.find_element(By.ID, "az-container").find_element(By.NAME, "pageSize")).select_by_value("50")
                break
            except:
                time.sleep(1)
        else:
            raise Exception("Không thay đổi được pageSize")

        # Đợi đến khi page load
        self.old_table = None
        self.wait_for_table(1)

    def wait_for_table(self, first_stt, exchange = None, return_cont_soup = False):
        for _ in range(60):
            try:
                soup = BeautifulSoup(self.driver.page_source, "html.parser")
                div_container = soup.find_all("div", {
                    "class": "m-b pos-relative",
                    "id"   : "az-container"
                })
                assert len(div_container) == 1, "Nhiều div_container"
                div_container = div_container[0]

                table_info = div_container.find_all("div", {"class": "pull-left"})
                assert len(table_info) == 2 and table_info[0].text == table_info[1].text, "Không xác định được table_info"
                tmp_txt = re.search(r"Tổng số \d+ bản ghi", table_info[0].text).group()
                assert tmp_txt == table_info[0].text, "Sai pattern table_info"
                num_rows = int(tmp_txt.replace("Tổng số ", "").replace(" bản ghi", ""))

                tables = div_container.find_all("table", {"class": "table table-striped table-bordered table-hover table-middle pos-relative m-b"})
                assert len(tables) == 1, "Nhiều table"
                df = pd.read_html(str(tables[0]))[0]
                assert (df["STT"] == np.arange(first_stt, min(first_stt + 50, num_rows + 1))).all(), "Cột STT không như mong đợi"

                if exchange is not None:
                    assert (df["Sàn"].str.lower() == exchange).all(), "Dữ liệu sàn không đúng"

                if self.old_table is not None:
                    assert df.columns[1].startswith("Mã CK"), "Sai tên cột"
                    assert len(self.old_table) != len(df) or (df[df.columns[1]] != self.old_table[df.columns[1]]).all(), "Dữ liệu bảng chưa thay đổi"

                self.old_table = df
                if return_cont_soup: return df, div_container
                else: return df
            except Exception as ex:
                time.sleep(1)
        else:
            raise Exception("Bảng như mong đợi không xuất hiện")

    def get_listCompany(self, exclude_sectors = ["tài chính"], list_exchange = ["hose", "hnx", "upcom"]):
        for _ in range(60):
            dict_sector_value = {}
            for e in self.find_item(By.NAME, "branch").find_elements(By.TAG_NAME, "option"):
                sector = e.text.lower()
                assert sector not in dict_sector_value.keys()
                dict_sector_value[sector] = e.get_attribute("value")

            if len(dict_sector_value) > 1:
                break
            else:
                time.sleep(1)
        else:
            raise Exception(f"Danh sách ngành bị thiếu {dict_sector_value}")

        for _ in range(60):
            dict_exchange_value = {}
            for e in self.find_item(By.NAME, "exchange").find_elements(By.TAG_NAME, "option"):
                exchange = e.text.lower()
                assert exchange not in dict_exchange_value.keys()
                dict_exchange_value[exchange] = e.get_attribute("value")

            if len(dict_exchange_value) > 1:
                break
            else:
                time.sleep(1)
        else:
            raise Exception(f"Danh sách sàn GD bị thiếu {dict_exchange_value}")

        list_exclude_sector = exclude_sectors + ["tất cả"]
        df_total = None
        while dict_sector_value:
            item = dict_sector_value.popitem()
            if item[0] in list_exclude_sector:
                continue

            for exchange in list_exchange:
                df = self.get_listCompany_by_exchange_sector(dict_exchange_value[exchange], item[1])
                try:
                    df_total = pd.concat((df_total, df), ignore_index=True)
                except:
                    df_total = df
        return df_total

    def get_listCompany_by_exchange_sector(self, exchange_value, sector_value):
        Select(self.find_item(By.NAME, "exchange")).select_by_value(exchange_value)
        Select(self.find_item(By.NAME, "branch")).select_by_value(sector_value)
        self.find_item(By.CLASS_NAME, "al-top").click()

        df_total, div_container = self.wait_for_table(1, return_cont_soup=True)
        page_info = div_container.find_all("div", {"class": "pull-right"})
        assert len(page_info) == 2 and page_info[0].text == page_info[1].text
        num_page = page_info[0].find("span", {"class": "m-r-xs"})
        num_page = int(re.search(r"Trang1\/\d+", num_page.text).group().replace("Trang1/", ""))

        first_table_stt = 51
        for _ in range(1, num_page):
            self.find_item(By.ID, "btn-page-next").click()
            df = self.wait_for_table(first_table_stt)
            first_table_stt += 50
            df_total = pd.concat((df_total, df), ignore_index=True)

        return df_total


class VietStock_Financial(VietStock_Crawler):
    def __init__(self, crawler_type="S", cycle_type = "quý 1"):
        assert crawler_type == "S"
        super().__init__(crawler_type)
        self.driver.get(VIETSTOCK_URL["FINANCIAL"])

        for e in self.find_item(By.ID, "group-option-multi").find_elements(By.CLASS_NAME, "fontNormal"):
            if e.text.lower() == cycle_type:
                continue
            e.click()

        time.sleep(60)

    def get_table_data(self):
        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        return pd.read_html(str(soup.find("table", {"id": "tbl-business-data"})))[0]

    def get_data_multiSymbol(self, list_symbol):
        txt_search_code = self.find_item(By.ID, "txt-search-code")
        txt_search_code.clear()
        txt_search_code.send_keys(",".join(list_symbol))

        self.find_item(By.CLASS_NAME, "div-statement-button").find_element(By.TAG_NAME, "button").click()
        time.sleep(30)

        cur_num_col = 1
        while True:
            df = self.get_table_data()
            assert df.shape[1] >= cur_num_col, "Lỗi loading bị mất cột"
            if df.shape[1] > cur_num_col:
                cur_num_col = df.shape[1]
                time.sleep(30)
                continue
            else:
                return df

    def get_all_data_financial(self, df_metadata):
        dict_data = {}
        for i in range(0, len(df_metadata), 50):
            list_symbol = df_metadata["Mã CK▲"].iloc[i:i+50].tolist()
            df = self.get_data_multiSymbol(list_symbol)
            dict_data[i] = df
            print("Done", i)

        return dict_data


class VietStock_Volume(VietStock_Crawler):
    def __init__(self, crawler_type="R"):
        assert crawler_type == "R"
        super().__init__(crawler_type)

    def get_all_data_volume(self, df_metadata):
        dict_data = {}
        for symbol in tqdm(df_metadata["Mã CK▲"]):
            for _ in range(10):
                try:
                    r = self.session.get(VIETSTOCK_URL["VOLUME"].format(symbol), headers=self.HEADERS)
                    assert r.status_code == 200
                    soup = BeautifulSoup(r.content, "html.parser")
                    table = soup.find("table", {"class": "table table-hover"})
                    df = pd.read_html(str(table))[0]
                    dict_data[symbol] = df
                    break
                except: pass
            else:
                print(symbol, "Error")

        return dict_data


class VietStock_Dividend(VietStock_Crawler):
    def __init__(self, crawler_type="S", last_date = "now"):
        assert crawler_type == "S"
        super().__init__(crawler_type)

        if last_date == "now":
            self.last_date = datetime.now().strftime("%Y-%m-%d")
        else:
            # Check format
            datetime.strptime(last_date, "%Y-%m-%d")
            self.last_date = last_date

        self.start_date = (
            datetime.strptime(self.last_date, "%Y-%m-%d") -
            timedelta(days=366)
        ).strftime("%Y-%m-%d")

    def wait_for_table(self, first_stt, current_page):
        for _ in range(60):
            try:
                soup = BeautifulSoup(self.driver.page_source, "html.parser")
                div_container = soup.find_all("div", {
                    "class": "container m-t m-b event-calendar",
                    "id"   : "event-calendar-content"
                })
                assert len(div_container) == 1, "Nhiều div_container"
                div_container = div_container[0]

                page_info = div_container.find_all("span", {"class": "m-r-xs"})
                assert len(page_info) == 2 and page_info[0].text == page_info[1].text, "Không xác định được page_info"
                tmp_txt = re.search(rf"Trang{current_page}/\d+", page_info[0].text).group()
                assert tmp_txt == page_info[0].text, "Sai pattern page_info"
                num_pages = int(tmp_txt.replace(f"Trang{current_page}/", ""))

                tables = div_container.find_all("table", {"id": "event-content"})
                assert len(tables) == 1, "Nhiều table"
                df = pd.read_html(str(tables[0]))[0]
                assert df.loc[0, "STT"] == first_stt, "Cột STT không như mong đợi"

                return df, num_pages
            except Exception as ex:
                print(ex.args)
                time.sleep(1)
        else:
            raise Exception("Bảng như mong đợi không xuất hiện")

    def get_all_dividendpart_1y(self, partname):
        self.driver.get(VIETSTOCK_URL["LIST_COMPANY"])
        self.driver.get(VIETSTOCK_URL[partname].format(1, self.start_date, self.last_date))

        first_stt = 1
        current_page = 1
        df_total = None
        while True:
            df, num_pages = self.wait_for_table(first_stt, current_page)
            try:
                df_total = pd.concat((df_total, df), ignore_index=True)
            except:
                df_total = df

            first_stt += 20
            current_page += 1
            if current_page > num_pages:
                return df_total

            self.find_item(By.ID, "btn-page-next").click()

    def get_all_dividend_1y(self):
        df1 = self.get_all_dividendpart_1y("CASH_DIVIDEND")
        df1["Loại Sự kiện"] = "Trả cổ tức bằng tiền mặt"
        df2 = self.get_all_dividendpart_1y("BONUS_SHARE")
        df2["Loại Sự kiện"] = "Thưởng cổ phiếu"
        df3 = self.get_all_dividendpart_1y("STOCK_DIVIDEND")
        df3["Loại Sự kiện"] = "Trả cổ tức bằng cổ phiếu"
        df = pd.concat([df1,df2,df3],ignore_index=True)
        return df
