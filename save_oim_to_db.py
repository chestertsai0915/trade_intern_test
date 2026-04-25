import pandas as pd
import numpy as np
from utils.database import DatabaseHandler

def calculate_and_save_oim(csv_path, symbol="BYBIT_BTCUSDT", chunk_size=50000):
    """
    分批讀取 L2 報價 CSV，計算第一檔的 Order Imbalance (OIM)，並寫入資料庫
    """
    db = DatabaseHandler()
    print(f"開始處理 {csv_path} 並計算 OIM 因子 (每批 {chunk_size} 筆)...")
    
    total_inserted = 0

    try:
        # 分批讀取以節省記憶體
        for i, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunk_size)):
            
            # 1. 計算 Level 1 的 OIM 因子
            # 公式: (Bid 數量 - Ask 數量) / (Bid 數量 + Ask 數量)
            qty_sum = chunk['bid_qty_1'] + chunk['ask_qty_1']
            
            # 避免分母為 0 的情況 (雖然報價通常不會為0，但加上這行更保險)
            oim_lvl1 = np.where(qty_sum > 0, 
                                (chunk['bid_qty_1'] - chunk['ask_qty_1']) / qty_sum, 
                                np.nan)
            
            # 2. 準備符合 save_generic_external_data 格式的 DataFrame
            df_factor = pd.DataFrame({
                'open_time': chunk['timestamp'],
                'symbol': symbol,
                'metric': 'bybit_oim_lvl1',  # 這個名稱就是您日後撈資料時的 key
                'value': oim_lvl1
            })
            
            # 3. 剃除計算出 NaN 的無效值
            df_factor.dropna(subset=['value'], inplace=True)
            
            # 4. 寫入資料庫
            if not df_factor.empty:
                db.save_generic_external_data(df_factor)
                inserted_rows = len(df_factor)
                total_inserted += inserted_rows
                print(f"  └─ 成功寫入第 {i+1} 批次，新增 {inserted_rows} 筆 OIM 因子。")
                
        print(f"\n 任務大功告成！總計寫入 {total_inserted} 筆 OIM 因子紀錄至 external_data 表。")
        
    except Exception as e:
        print(f" 執行過程中發生錯誤: {e}")

if __name__ == "__main__":
    # ==========================================
    # 請確認您的 CSV 檔案名稱與路徑
    # ==========================================
    CSV_FILE = "processed_l2_top5.csv"
    
    # 這裡的 Symbol 設定會成為寫入 DB 的標籤，建議加上交易所前綴
    TARGET_SYMBOL = "BYBIT_BTCUSDT" 
    
    calculate_and_save_oim(csv_path=CSV_FILE, symbol=TARGET_SYMBOL)