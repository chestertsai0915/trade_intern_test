# utils/data_filler.py
import pandas as pd
import time

class DataGapFiller:
    def __init__(self, db_handler, fetch_func, symbol="BTCUSDT", db_symbol=None, interval="1m", api_limit=1000):
        self.db = db_handler
        self.fetch_func = fetch_func
        self.api_symbol = symbol                     # 餵給 API 的名稱 (如 BTCUSDT)
        self.db_symbol = db_symbol or symbol         # 存進 DB 的名稱 (如 BYBIT_BTCUSDT)
        self.interval = str(interval)
        self.api_limit = api_limit

    def _get_interval_ms(self):
        unit = self.interval[-1]
        value = int(self.interval[:-1])
        multiplier = {'m': 60 * 1000, 'h': 60 * 60 * 1000, 'd': 24 * 60 * 60 * 1000}
        return value * multiplier[unit]

    def _get_pandas_freq(self):
        unit = self.interval[-1]
        value = int(self.interval[:-1])
        mapping = {'m': 'min', 'h': 'h', 'd': 'D'}
        return f"{value}{mapping[unit]}"

    def check_and_fill(self, start_date, end_date):
        print(f"開始檢查 {self.db_symbol} ({self.interval}) 從 {start_date} 到 {end_date} ...")

        freq = self._get_pandas_freq()
        expected_dates = pd.date_range(start=start_date, end=end_date, freq=freq)
        expected_ts = set(expected_dates.astype('int64') // 10**6)

        existing_ts_list = self.db.get_existing_timestamps(
            symbol=self.db_symbol, 
            interval=self.interval, 
            start_ts=min(expected_ts), 
            end_ts=max(expected_ts)
        )
        existing_ts = set(existing_ts_list if existing_ts_list else [])
        missing_ts = sorted(list(expected_ts - existing_ts))
        
        if not missing_ts:
            print("資料庫資料完整，無缺失區塊！\n")
            return

        print(f"發現 {len(missing_ts)} 筆缺失資料，開始分批下載...")

        interval_ms = self._get_interval_ms()
        chunks = []
        current_chunk = [missing_ts[0]]

        for ts in missing_ts[1:]:
            if ts == current_chunk[-1] + interval_ms and len(current_chunk) < self.api_limit:
                current_chunk.append(ts)
            else:
                chunks.append((current_chunk[0], current_chunk[-1]))
                current_chunk = [ts]
        if current_chunk:
            chunks.append((current_chunk[0], current_chunk[-1]))

        for start_ts, end_ts in chunks:
            print(f"下載缺失區塊: {pd.to_datetime(start_ts, unit='ms')} -> {pd.to_datetime(end_ts, unit='ms')}")
            
            #  統一介面直接呼叫，完全不需要管是不是 Bybit
            df_new = self.fetch_func(
                symbol=self.api_symbol, 
                interval=self.interval, 
                limit=self.api_limit,
                startTime=start_ts,
                endTime=end_ts
            )

            if not df_new.empty:
                self.db.save_market_data(self.db_symbol, self.interval, df_new)
                print(f"  └─ 成功寫入 {len(df_new)} 筆資料到 {self.db_symbol}")
            else:
                print(" 抓取回傳為空")
            
            time.sleep(0.5) 
            
        print("所有缺失資料補齊完畢！\n")
    
    def only_check(self, start_date, end_date):
        print(f"開始檢查 {self.db_symbol} ({self.interval}) 從 {start_date} 到 {end_date} ...")

        freq = self._get_pandas_freq()
        expected_dates = pd.date_range(start=start_date, end=end_date, freq=freq)
        expected_ts = set(expected_dates.astype('int64') // 10**6)

        existing_ts_list = self.db.get_existing_timestamps(
            symbol=self.db_symbol, 
            interval=self.interval, 
            start_ts=min(expected_ts), 
            end_ts=max(expected_ts)
        )
        existing_ts = set(existing_ts_list if existing_ts_list else [])
        missing_ts = sorted(list(expected_ts - existing_ts))
        
        if not missing_ts:
            print("資料庫資料完整，無缺失區塊！\n")
            return

        print(f"發現 {len(missing_ts)} 筆缺失資料")