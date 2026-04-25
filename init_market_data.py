from binance.um_futures import UMFutures
from pybit.unified_trading import HTTP
from utils.database import DatabaseHandler
from utils.data_filler import DataGapFiller
from data_loader import DataLoader 
import os
def run_fill_data(source, symbol, interval, start_time, end_time):
    print(f"=== 啟動資料補齊任務: {source.upper()} ===")
    
    db = DatabaseHandler()
    binance_key = os.getenv('BINANCE_API_KEY')
    binance_secret = os.getenv('BINANCE_SECRET_KEY')
    client = UMFutures(key=binance_key, secret=binance_secret)
    bybit_key = os.getenv('BYBIT_API_KEY')
    bybit_secret = os.getenv('BYBIT_SECRET_KEY')

    bybit_client = HTTP(
        testnet=False,
        api_key=bybit_key,
        api_secret=bybit_secret
    )

    loader = DataLoader(
        client=client, 
        bybit_client=bybit_client,
        db=db
    )

    # 2. 像開關一樣切換目標
    if source == 'binance':
        target_fetch_func = loader.get_binance_klines
        target_db_symbol = symbol                   # 存成 BTCUSDT
        api_limit = 1000
    elif source == 'bybit':
        target_fetch_func = loader.get_bybit_klines
        target_db_symbol = f"BYBIT_{symbol}"        # 存成 BYBIT_BTCUSDT 避免污染
        api_limit = 1000
    else:
        print("錯誤：不支援的交易所來源")
        return

    # 3. 執行補齊
    filler = DataGapFiller(
        db_handler=db,
        fetch_func=target_fetch_func,
        symbol=symbol,
        db_symbol=target_db_symbol,
        interval=interval,
        api_limit=api_limit
    )

    filler.check_and_fill(start_date=start_time, end_date=end_time)
    print("=== 任務執行結束 ===")

if __name__ == "__main__":
    
   
    TARGET_SOURCE = "binance"               # "binance" 或 "bybit"
    TARGET_SYMBOL = "BTCUSDT" 
    TARGET_INTERVAL = "1m"                # 維持通用的 1m 格式
    START_TIME = "2025-03-29 00:00:00" 
    END_TIME   = "2026-04-20 00:00:00" 
    
    run_fill_data(
        source=TARGET_SOURCE,
        symbol=TARGET_SYMBOL,
        interval=TARGET_INTERVAL,
        start_time=START_TIME,
        end_time=END_TIME
    )