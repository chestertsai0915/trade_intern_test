from binance.um_futures import UMFutures
from dotenv import load_dotenv
from data_loader import DataLoader
from data_sources.bybit_source import BybitExternalSource
from utils.database import DatabaseHandler
from utils.data_filler import DataGapFiller
import os
load_dotenv()
def run_fill_data(source, symbol, interval, start_time, end_time):
    print(f"來源: {source}, 交易對: {symbol}, 週期: {interval}")
    print(f"區間: {start_time}  ->  {end_time}\n")

    # 1. 初始化資料庫
    db = DatabaseHandler()
    

    # 2. 根據指定的來源選擇對應的 Fetch 函數與設定
    if source == 'binance':
        key = os.getenv('BINANCE_API_KEY')
        secret = os.getenv('BINANCE_SECRET_KEY')
        client = UMFutures(key=key, secret=secret)
        
        loader = DataLoader(client=client)
        fetch_function = loader.get_binance_klines
        api_limit = 1000
        target_db_symbol = symbol # 幣安維持原名 'BTCUSDT'
        
    elif source == 'bybit':
        def bybit_fetch_wrapper(symbol, interval, limit, startTime, endTime):
            bybit_inv = interval
            if interval.endswith('m'):
                bybit_inv = interval[:-1]  # 1m -> 1, 5m -> 5
            elif interval == '1h':
                bybit_inv = '60'           # 1h -> 60
            elif interval == '1d':
                bybit_inv = 'D'            # 1d -> D
                
            return bybit_source.get_klines(
                symbol=symbol, 
                interval=bybit_inv, 
                limit=limit, 
                startTime=startTime, 
                endTime=endTime
            )
        bybit_source = BybitExternalSource()
        fetch_function = bybit_source.get_klines
        api_limit = 1000
        target_db_symbol = f"BYBIT_{symbol}" # 強制加上 BYBIT_ 前綴！
        
    else:
        print(f"錯誤：不支援的資料來源 '{source}'，請選擇 'binance' 或 'bybit'")
        return

    # 3. 實例化補齊工具時，把兩個 symbol 都傳進去
    filler = DataGapFiller(
        db_handler=db,
        fetch_func=fetch_function,
        symbol=symbol,               # 'BTCUSDT'
        db_symbol=target_db_symbol,  # 'BYBIT_BTCUSDT'
        interval=interval,
        api_limit=api_limit
    )
    
    
    
    # 4. 執行掃描與補齊
    filler.check_and_fill(start_date=start_time, end_date=end_time)
    print("任務結束 ")


if __name__ == "__main__":
    
    TARGET_SOURCE = "bybit"     # 填寫 "binance"或 "bybit"
    TARGET_SYMBOL = "BTCUSDT"     # 交易對名稱
    TARGET_INTERVAL = "1m"        # K線週期 (1m, 5m, 15m, 1h, 1d)
    START_TIME = "2025-03-29 00:00:00" # 起始時間 (格式: YYYY-MM-DD HH:MM:SS)
    END_TIME   = "2026-03-29 00:00:00" # 結束時間 (格式: YYYY-MM-DD HH:MM:SS)

    # ==========================================
    
    # 執行主程式
    run_fill_data(
        source=TARGET_SOURCE,
        symbol=TARGET_SYMBOL,
        interval=TARGET_INTERVAL,
        start_time=START_TIME,
        end_time=END_TIME
    )