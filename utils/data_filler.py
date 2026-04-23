import pandas as pd
import time

class DataGapFiller:
    def __init__(self, db_handler, fetch_func, symbol="BTCUSDT", db_symbol=None, interval="1m", api_limit=1000):
        """
        初始化資料補齊工具
        :param db_handler: DatabaseHandler 實例
        :param fetch_func: 負責呼叫 API 的函數 (例如 loader.get_binance_klines)
        :param symbol: 交易對
        :param interval: K線週期
        :param api_limit: API 單次請求限制
        """
        self.db = db_handler
        self.fetch_func = fetch_func
        self.api_symbol = symbol                     # 給 API 用的 (例如: BTCUSDT)
        self.db_symbol = db_symbol or symbol         # 給 DB 用的 (例如: BYBIT_BTCUSDT，沒填就預設同 API)
        self.interval_str = str(interval)
        self.api_limit = api_limit

    def _get_interval_ms(self):
        unit = self.interval_str[-1]
        value = int(self.interval_str[:-1])
        multiplier = {'m': 60 * 1000, 'h': 60 * 60 * 1000, 'd': 24 * 60 * 60 * 1000}
        return value * multiplier[unit]

    def _get_pandas_freq(self):
        unit = self.interval_str[-1]
        value = int(self.interval_str[:-1])
        mapping = {'m': 'min', 'h': 'h', 'd': 'D'}
        return f"{value}{mapping[unit]}"

    def check_and_fill(self, start_date, end_date):
        print(f"開始檢查 {self.db_symbol} ({self.interval_str}) 從 {start_date} 到 {end_date} 的資料...")

        freq = self._get_pandas_freq()
        expected_dates = pd.date_range(start=start_date, end=end_date, freq=freq)
        expected_ts = set(expected_dates.astype('int64') // 10**6)

        # 2. 去資料庫查詢時，使用【資料庫的專屬名稱 db_symbol】
        existing_ts_list = self.db.get_existing_timestamps(
            symbol=self.db_symbol, 
            interval=self.interval_str, 
            start_ts=min(expected_ts), 
            end_ts=max(expected_ts)
        )
        if existing_ts_list is None:
            existing_ts_list = []
        existing_ts = set(existing_ts_list)
        

        # 3. 計算缺失
        missing_ts = sorted(list(expected_ts - existing_ts))
        
        if not missing_ts:
            print("資料庫資料完整，無缺失區塊！\n")
            return

        print(f"發現 {len(missing_ts)} 筆缺失資料，開始分批下載...")

        # 4. 分群組 (Chunking)
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

        # 5. 分批獲取並寫入
        for start_ts, end_ts in chunks:
            print(f"下載缺失區塊: {pd.to_datetime(start_ts, unit='ms')} -> {pd.to_datetime(end_ts, unit='ms')}")
            # 3. 呼叫 API 下載時，使用【API的原始名稱 api_symbol】
            df_new = self.fetch_func(
                symbol=self.api_symbol, 
                interval=self.interval_str, 
                limit=self.api_limit,
                startTime=start_ts,
                endTime=end_ts
            )

            if not df_new.empty:
                # 4. 寫入資料庫時，使用【資料庫的專屬名稱 db_symbol】
                # (因為您資料庫的寫入函數名為 save_market_data，這裡配合您原本的方法名)
                self.db.save_market_data(self.db_symbol, self.interval_str, df_new)
                print(f"  └─ 成功寫入 {len(df_new)} 筆資料到 {self.db_symbol}")
            
            time.sleep(0.5)
            
        print("所有缺失資料補齊完畢！\n")

    def only_check_data(self, start_date, end_date):
        print(f"僅檢查 {self.db_symbol} ({self.interval_str}) 從 {start_date} 到 {end_date} 的資料...")

        freq = self._get_pandas_freq()
        expected_dates = pd.date_range(start=start_date, end=end_date, freq=freq)
        expected_ts = set(expected_dates.astype('int64') // 10**6)

        # 2. 去資料庫查詢時，使用【資料庫的專屬名稱 db_symbol】
        existing_ts_list = self.db.get_existing_timestamps(
            symbol=self.db_symbol, 
            interval=self.interval_str, 
            start_ts=min(expected_ts), 
            end_ts=max(expected_ts)
        )
        if existing_ts_list is None:
            existing_ts_list = []
        existing_ts = set(existing_ts_list)
        

        # 3. 計算缺失
        missing_ts = sorted(list(expected_ts - existing_ts))
        
        if not missing_ts:
            print("資料庫資料完整，無缺失區塊！\n")
            return

        print(f"發現 {len(missing_ts)} 筆缺失資料")