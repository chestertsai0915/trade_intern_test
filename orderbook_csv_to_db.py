import pandas as pd
# 假設您的 DatabaseHandler 放在 utils.database 模組下
from utils.database import DatabaseHandler

def import_l2_csv_to_db(csv_path, symbol="BYBIT_BTCUSDT", chunk_size=50000):
    """
    分批讀取 L2 CSV，轉換結構並寫入 external_data
    """
    db = DatabaseHandler()
    print(f"開始將 {csv_path} 寫入資料庫 (每批 {chunk_size} 筆)...")
    
    # 紀錄總寫入的特徵數量
    total_inserted = 0

    # 使用 chunksize 避免 600 萬筆資料一次載入撐爆記憶體
    for i, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunk_size)):
        
        # 1. 使用 pd.melt 將「寬表 (21個欄位)」轉換為「長表 (metric & value)」
        df_melted = pd.melt(
            chunk,
            id_vars=['timestamp'],                 # 保持不變的主鍵
            var_name='metric',                     # 原本的欄位名稱變成 metric (例如: 'bid_price_1')
            value_name='value'                     # 原本的數字變成 value
        )

        # 2. 為了對齊 save_generic_external_data 的要求，進行欄位重命名與新增
        df_melted.rename(columns={'timestamp': 'open_time'}, inplace=True)
        df_melted['symbol'] = symbol

        # 3. 剃除空值 (例如有時候沒有掛到第 5 檔)
        df_melted.dropna(subset=['value'], inplace=True)

        # 4. 呼叫您寫好的通用儲存函數
        db.save_generic_external_data(df_melted)
        
        inserted_rows = len(df_melted)
        total_inserted += inserted_rows
        print(f"  └─ 成功寫入第 {i+1} 批次，新增 {inserted_rows} 筆 key-value 紀錄。")

    print(f" 任務大功告成！總計寫入  筆特徵紀錄至 external_data 表。")

if __name__ == "__main__":
    CSV_FILE = "processed_l2_top5.csv"
    TARGET_SYMBOL = "BYBIT_BTCUSDT"
    
    import_l2_csv_to_db(csv_path=CSV_FILE, symbol=TARGET_SYMBOL)