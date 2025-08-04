import _lib_path_
from CONFIG import CYCLE_TYPE
from _0_CreateFolder import FOLDER_INGESTION_NOW
from _1_ListCompany import LISTCOM_PATH
import os
from Crawler.CafeF import CafeF_PDF
from Crawler.VietStock import VietStock_PDF
import pandas as pd
from multiprocessing import Pool
from colorama import Fore, Style

try:
    from VAR_GLOBAL_CONFIG import YEAR_KEY
except:
    from VAR_GLOBAL_CONFIG import QUARTER_KEY


FOLDER = f"{FOLDER_INGESTION_NOW}/Temp_PDF/CafeF"
NUM_PROCESS = 8


def get_data_cf(args):
    start, end = args
    listCom = pd.read_csv(LISTCOM_PATH)

    list_symbol_error = []

    if CYCLE_TYPE.startswith("q"):
        year = int(QUARTER_KEY[-4:])
    else:
        year = int(YEAR_KEY)

    vis = CafeF_PDF(type_cycle=CYCLE_TYPE, year=year)

    for idx in range(start, end):
        symbol = listCom.loc[idx, "Mã CK▲"]
        if os.path.exists(f"{FOLDER}/{symbol}.csv"): continue
        try:
            data = vis.get_pdf_1_com(symbol)

            data.to_csv(f"{FOLDER}/{symbol}.csv", index=False)

            print(Fore.LIGHTGREEN_EX, idx, symbol, "Done", Style.RESET_ALL, flush=True)
        except Exception as ex:
            list_symbol_error.append((symbol, ex))
            print(Fore.LIGHTRED_EX, idx, symbol, "Error", ex.args, Style.RESET_ALL, flush=True)

    vis.quit_crawler()

    return list_symbol_error


if __name__ == "__main__":
    os.makedirs(FOLDER, exist_ok=True)
    os.makedirs(FOLDER.replace("CafeF", "VietStock"), exist_ok=True)

    listCom = pd.read_csv(LISTCOM_PATH)
    list_args = []
    for i in range(NUM_PROCESS):
        start = int(i / NUM_PROCESS * listCom.shape[0])
        end = int((i + 1) / NUM_PROCESS * listCom.shape[0])
        list_args.append((start, end))

    assert list_args[-1][1] == listCom.shape[0]

    #### CafeF
    with Pool(processes=NUM_PROCESS) as p:
        results = p.map(get_data_cf, list_args)

    list_symbol_error = []
    for rs in results:
        list_symbol_error.extend(rs)

    for sym, err in list_symbol_error:
        print(sym, err.args)

    #### VietStock
    list_symbol_error = []
    folder = FOLDER.replace("CafeF", "VietStock")
    vis = VietStock_PDF()
    for idx in range(len(listCom)):
        symbol = listCom.loc[idx, "Mã CK▲"]
        if os.path.exists(f"{folder}/{symbol}.csv"): continue
        try:
            data = vis.get_pdf_1_com(symbol)

            data.to_csv(f"{folder}/{symbol}.csv", index=False)

            print(Fore.LIGHTGREEN_EX, idx, symbol, "Done", Style.RESET_ALL, flush=True)
        except Exception as ex:
            list_symbol_error.append((symbol, ex))
            print(Fore.LIGHTRED_EX, idx, symbol, "Error", ex.args, Style.RESET_ALL, flush=True)

    vis.quit_crawler()

    for sym, err in list_symbol_error:
        print(sym, err.args)

    #### CafeF
    start_ = "báo cáo tài chính"
    conso_att = "hợp nhất"
    if CYCLE_TYPE.startswith("q"):
        end_ = f"{CYCLE_TYPE} năm " + QUARTER_KEY[-4:]
        list_prio = [
            f"{start_} {conso_att} {end_}",
            f"{start_} {end_}"
        ]
    else:
        end_ = f"{CYCLE_TYPE} " + str(YEAR_KEY)
        end_2 = "(đã kiểm toán)"
        list_prio = [
            f"{start_} {conso_att} {end_} {end_2}",
            f"{start_} {conso_att} {end_}",
            f"{start_} {end_} {end_2}",
            f"{start_} {end_}"
        ]
    print(list_prio)

    listCom["CafeF_PDF_Name"] = None
    listCom["CafeF_PDF_URL"] = None
    for i in range(len(listCom)):
        symbol = listCom.loc[i, "Mã CK▲"]
        try:
            df = pd.read_csv(f"{FOLDER}/{symbol}.csv")
            for p in list_prio:
                try:
                    name_pdf = df[df["Loại báo cáo"].str.lower() == p].iloc[0]["Loại báo cáo"]
                    link_pdf = df[df["Loại báo cáo"].str.lower() == p].iloc[0]["Tải về"]
                    listCom.loc[i, "CafeF_PDF_Name"] = name_pdf
                    listCom.loc[i, "CafeF_PDF_URL"] = link_pdf
                    break
                except:
                    pass
        except Exception as ex:
            print(symbol, ex.args)
            pass

    #### VietStock
    start_ = "báo cáo tài chính"
    conso_att = "hợp nhất"
    if CYCLE_TYPE.startswith("q"):
        end_ = f"{CYCLE_TYPE} năm " + QUARTER_KEY[-4:]
        list_prio = [
            f"{start_} {conso_att} {end_}",
            f"{start_} {end_}"
        ]
    else:
        end_ = f"{CYCLE_TYPE} " + str(YEAR_KEY)
        kt_att = "kiểm toán"
        list_prio = [
            f"{start_} {conso_att} {kt_att} {end_}",
            f"{start_} {conso_att} {end_}",
            f"{start_} {kt_att} {end_}",
            f"{start_} {end_}",
        ]
    print(list_prio)

    listCom["VietStock_PDF_Name"] = None
    listCom["VietStock_PDF_URL"] = None
    for i in range(len(listCom)):
        symbol = listCom.loc[i, "Mã CK▲"]
        try:
            df = pd.read_csv(f"{folder}/{symbol}.csv")
            for p in list_prio:
                try:
                    name_pdf = df[df["Report Type"].str.lower() == p].iloc[0]["Report Type"]
                    link_pdf = df[df["Report Type"].str.lower() == p].iloc[0]["Download"]
                    listCom.loc[i, "VietStock_PDF_Name"] = name_pdf
                    listCom.loc[i, "VietStock_PDF_URL"] = link_pdf
                    break
                except:
                    pass
        except Exception as ex:
            print(symbol, ex.args)
            pass

    listCom.to_csv(LISTCOM_PATH, index=False)
