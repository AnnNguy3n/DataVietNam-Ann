# Lưu ý:
Tất cả các path phải sửa \ (gạch chéo trái) thành / (gạch chéo phải) để tránh lỗi, path dẫn đến folder thì không kết thúc bằng dấu gạch chéo.

# Phần Crawl
B1: vào file Run_Crawl/CONFIG.py chỉnh các tham số:
    - CYCLE_TYPE: quý 1, 2, 3, 4 hoặc năm
    - LAST_DATE: ngày Crawl hoặc ngày giới hạn để lấy giá và Dividend
    - FOLDER_DATALAKE: ví dụ "H:/My Drive/DataVIS_/VietNam/Data Lake"
    - Không thay đổi FOLDER_INGESTIONS
B2: Chạy lần lượt các file từ Run_Crawl/_0_CreateFolder.py đến file Run_Crawl/_5_Close_CafeF.py, không cần thay đổi gì cả.

# Phần Transform
B1: vào file VAR_GLOBAL_CONFIG.py chỉnh các tham số:
    - FOLDER_DATALAKE: giống FOLDER_DATALAKE của phần Crawl, hoặc folder khác nhưng cấu trúc phải như nhau
    - FOLDER_DATA_WH: ví dụ "H:/My Drive/DataVIS_/VietNam/Data WareHouse"
    - START_DAY_UPDATE: ngày mua cổ phiếu của chu kỳ liền trước
    - START_DAY_LIST_UPDATE và END_DAY_UPDATE: ngày mua cổ phiếu của chu kỳ hiện tại (tương ứng với ngày Crawl)
    - QUARTER_KEY: "1/2025", "2/2025", "3/2025", "4/2025"
    - TYPE_TIME: False tương ứng với data Quý
    - FILE_FEATURE: không cần thay đổi
    - YEAR_FINANCIAL_FIX_FILE: không cần thay đổi
    - QUARTER_FINANCIAL_FIX_FILE: không cần thay đổi
    - QUARTER_FINANCIAL_FIX_FILE_BY_HUMAN: không cần thay đổi

B2: vào thư mục TransformData/VietNam/run chạy lần lượt các file theo thứ tự, riêng file TransformData/VietNam/run/_7_Total_Compare.py chỉ chạy 2 key là "Financial_Quarter" và "Dividend".

B3: vào thư mục TransformData/VietNam/merge chạy lần lượt 4 file để import ra DIVIDEND, FINANCIAL Quý, VALUE_ARG và Volume.

B4: chạy Export_BUY_and_SELL.ipynb để lấy file SELL và BUY.

B5: chạy Export_ValARG_and_VolARG.ipynb để lấy ValueARG và VolumeARG.