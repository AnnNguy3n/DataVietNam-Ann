from CONFIG import FOLDER_INGESTIONS, CYCLE_TYPE, LAST_DATE
import os


FOLDER_INGESTION_NOW = f"{FOLDER_INGESTIONS}/{LAST_DATE}"

# Cycle key: Quarter | Year
assert CYCLE_TYPE in ["quý 1", "quý 2", "quý 3", "quý 4", "năm"]
CYCLE_KEY = "Quarter" if CYCLE_TYPE.startswith("q") else "Year"


if __name__ == "__main__":
    # Folder Financial CafeF
    for report_type in ["BalanceSheet", "IncomeStatement", "CashFlowInDirect", "CashFlowDirect"]:
        os.makedirs(f"{FOLDER_INGESTION_NOW}/Financial/CafeF/{CYCLE_KEY}/{report_type}", exist_ok=True)

    # Folder Financial VietStock
    os.makedirs(f"{FOLDER_INGESTION_NOW}/Financial/VietStock/{CYCLE_KEY}", exist_ok=True)

    # Folder Volume
    for source in ["CafeF", "VietStock"]:
        os.makedirs(f"{FOLDER_INGESTION_NOW}/Volume/{source}/VolumeNow", exist_ok=True)

    # Folder Dividend
    for source in ["CafeF", "VietStock"]:
        os.makedirs(f"{FOLDER_INGESTION_NOW}/Dividend/{source}", exist_ok=True)

    # Folder PriceClose
    for source in ["CafeF", "iBoardSSI"]:
        os.makedirs(f"{FOLDER_INGESTION_NOW}/Close/{source}", exist_ok=True)
