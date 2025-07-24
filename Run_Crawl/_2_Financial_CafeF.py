import _lib_path_
from Crawler.CafeF import CafeF_Financial
from CONFIG import CYCLE_TYPE
from _0_CreateFolder import FOLDER_INGESTION_NOW, CYCLE_KEY
from _1_ListCompany import LISTCOM_PATH
import pandas as pd
from multiprocessing import Pool, cpu_count
from colorama import Fore, Style
import json
import os


FOLDER = f"{FOLDER_INGESTION_NOW}/Financial/CafeF/{CYCLE_KEY}"
NUM_PROCESS = 8


def get_data(args):
    start, end = args
    listCom = pd.read_csv(LISTCOM_PATH)

    list_symbol_error = []
    vis = CafeF_Financial(cycle_type=CYCLE_TYPE, year="now", num_fetch_cycles=3)

    for idx in range(start, end):
        symbol = listCom.loc[idx, "Mã CK▲"]
        if os.path.exists(f"{FOLDER}/BalanceSheet/{symbol}.json"): continue
        try:
            data = vis.get_data_1_com(symbol, ["BALANCE_SHEET", "INCOME_STATEMENT"])

            with open(f"{FOLDER}/BalanceSheet/{symbol}.json", "w", encoding="utf-8") as f:
                json.dump(data["BALANCE_SHEET"], f, ensure_ascii=False)
            with open(f"{FOLDER}/IncomeStatement/{symbol}.json", "w", encoding="utf-8") as f:
                json.dump(data["INCOME_STATEMENT"], f, ensure_ascii=False)
            # with open(f"{FOLDER}/CashFlowDirect/{symbol}.json", "w", encoding="utf-8") as f:
            #     json.dump(data["CASHFLOW_DIRECT"], f, ensure_ascii=False)
            # with open(f"{FOLDER}/CashFlowInDirect/{symbol}.json", "w", encoding="utf-8") as f:
            #     json.dump(data["CASHFLOW_INDIRECT"], f, ensure_ascii=False)

            print(Fore.LIGHTGREEN_EX, idx, symbol, "Done", Style.RESET_ALL, flush=True)
        except Exception as ex:
            list_symbol_error.append((symbol, ex))
            print(Fore.LIGHTRED_EX, idx, symbol, "Error", ex.args, Style.RESET_ALL, flush=True)

    vis.quit_crawler()

    return list_symbol_error


if __name__ == "__main__":
    listCom = pd.read_csv(LISTCOM_PATH)
    list_args = []
    for i in range(NUM_PROCESS):
        start = int(i / NUM_PROCESS * listCom.shape[0])
        end = int((i + 1) / NUM_PROCESS * listCom.shape[0])
        list_args.append((start, end))

    assert list_args[-1][1] == listCom.shape[0]

    with Pool(processes=NUM_PROCESS) as p:
        results = p.map(get_data, list_args)

    list_symbol_error = []
    for rs in results:
        list_symbol_error.extend(rs)

    for sym, err in list_symbol_error:
        print(sym, err.args)
