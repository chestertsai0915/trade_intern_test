# utils/data_filler.py
import pandas as pd
import time

class DataGapFiller:
    def __init__(self, db_handler, fetch_func, symbol="BTCUSDT", db_symbol=None, interval="1m", api_limit=1000, metric=None):
        self.db = db_handler
        self.fetch_func = fetch_func
        self.api_symbol = symbol                     # 餵給 API 的名稱 (如 BTCUSDT)
        self.db_symbol = db_symbol or symbol         # 存進 DB 的名稱 (如 BYBIT_BTCUSDT)
        self.interval = str(interval)
        self.api_limit = api_limit
        self.metric = metric                         # 專門給 external_data 用的特徵名稱 (如 funding_rate)

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

    # 1. 專門給 Market Data (K線) 用的補齊
   
    def check_and_fill(self, start_date, end_date):
        print(f"開始檢查 K線數據 {self.db_symbol} ({self.interval}) 從 {start_date} 到 {end_date} ...")

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
                print("  抓取回傳為空")
            
            time.sleep(0.5) 
            
        print("所有缺失資料補齊完畢！\n")

   
    # 2. 專門給 External Data (如資金費率) 用的補齊
    
    def check_and_fill_external(self, start_date, end_date):
        if not self.metric:
            print("錯誤：執行 check_and_fill_external 必須在初始化時提供 metric 參數！")
            return

        print(f"開始檢查外部數據 {self.db_symbol} (特徵: {self.metric}, 頻率: {self.interval}) 從 {start_date} 到 {end_date} ...")

        freq = self._get_pandas_freq()
        expected_dates = pd.date_range(start=start_date, end=end_date, freq=freq)
        expected_ts = set(expected_dates.astype('int64') // 10**6)

        interval_ms = self._get_interval_ms()

        # 針對 external_data 表格下 SQL 查詢
        try:
            cursor = self.db.conn.cursor()
            cursor.execute('''
                SELECT timestamp FROM external_data 
                WHERE symbol = ? AND metric = ? AND timestamp >= ? AND timestamp <= ?
            ''', (self.db_symbol, self.metric, min(expected_ts) - interval_ms, max(expected_ts) + interval_ms))
            
            raw_existing_ts = set(row[0] for row in cursor.fetchall())
            
            # 【關鍵修復 1】容錯對齊 (Snapping)
            # 將資料庫撈出的時間四捨五入對齊到標準網格 (例如 8h)，消除 API 結算延遲的毫秒誤差
            existing_ts = set(round(ts / interval_ms) * interval_ms for ts in raw_existing_ts)
            
        except Exception as e:
            print(f"查詢 external_data 發生錯誤: {e}")
            existing_ts = set()

        missing_ts = sorted(list(expected_ts - existing_ts))
        
        if not missing_ts:
            print(f"[{self.metric}] 資料庫資料完整，無缺失區塊！\n")
            return

        print(f"發現 {len(missing_ts)} 筆缺失資料，開始分批下載...")

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
            # 【關鍵修復 2】放大抓取視窗
            # 加上 interval_ms - 1，給 API 一個完整的「尋找區間」(例如 08:00:00 到 15:59:59)
            window_end_ts = end_ts + interval_ms - 1
            
            print(f"下載缺失區塊: {pd.to_datetime(start_ts, unit='ms')} -> {pd.to_datetime(window_end_ts, unit='ms')}")
            
            df_new = self.fetch_func(
                symbol=self.api_symbol, 
                limit=self.api_limit,
                startTime=start_ts,
                endTime=window_end_ts
            )

            if not df_new.empty:
                df_new['symbol'] = self.db_symbol 
                self.db.save_generic_external_data(df_new)
                print(f"  └─ 成功寫入 {len(df_new)} 筆資料到 external_data ({self.metric})")
            else:
                print("  抓取回傳為空")
            
            time.sleep(0.5) 
            
        print(f"[{self.metric}] 所有缺失資料補齊完畢！\n")

   
    # 3. 僅檢查不下載 
    
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

        print(f"發現 {len(missing_ts)} 筆缺失資料\n")