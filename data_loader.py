import pandas as pd
from binance.um_futures import UMFutures
from utils.database import DatabaseHandler
from pybit.unified_trading import HTTP as BybitHTTP  # 引入 Bybit V5 SDK

class DataLoader:
    def __init__(self, client: UMFutures, bybit_client: BybitHTTP=None, db: DatabaseHandler = None):
        self.client = client
        self.bybit_client = bybit_client
        self.db_handler = db
        
        
    def get_binance_klines(self, symbol, interval, limit=1000, startTime=None, endTime=None):
        """ 
        幣安 K 線抓取，支援指定起迄時間
        """
        try:
            # 將額外參數打包
            params = {
                "symbol": symbol,
                "interval": interval,
                "limit": limit
            }
            if startTime is not None:
                params["startTime"] = int(startTime)
            if endTime is not None:
                params["endTime"] = int(endTime)

            klines = self.client.klines(**params)
            
            df = pd.DataFrame(klines, columns=[
                'open_time', 'open', 'high', 'low', 'close', 'volume', 
                'close_time', 'q_vol', 'trades', 'taker_buy_vol', 'taker_buy_q_vol', 'ignore'
            ])
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            df[numeric_cols] = df[numeric_cols].astype(float)
            
            return df
        except Exception as e:
            print(f"幣安數據抓取失敗: {e}")
            return pd.DataFrame()
    def get_bybit_klines(self, symbol, interval, limit=100, startTime=None, endTime=None):
        
        bybit_interval = str(interval)
        if bybit_interval.endswith('m'):
            bybit_interval = bybit_interval[:-1]  
        elif bybit_interval == '1h':
            bybit_interval = '60'                 
        elif bybit_interval == '1d':
            bybit_interval = 'D'                  
            
        try:
            # 2. 整理參數
            params = {
                "category": "linear", 
                "symbol": symbol,
                "interval": bybit_interval, 
                "limit": limit
            }
            
            # Bybit 的時間參數名稱為 start 和 end
            if startTime is not None:
                params["start"] = int(startTime)
            if endTime is not None:
                params["end"] = int(endTime)

            # 3. 呼叫 Bybit V5 API
            response = self.bybit_client.get_kline(**params)
            
            # 加上錯誤檢查，如果 Bybit 報錯 (例如參數不對)，直接印出來方便除錯
            if response.get('retCode') != 0:
                print(f"Bybit API 拒絕請求: {response.get('retMsg')}")
                return pd.DataFrame()
            
            # Bybit 回傳的資料在 result['list'] 中
            klines = response.get('result', {}).get('list', [])
            
            if not klines:
                return pd.DataFrame()
            
            # 【關鍵】Bybit 的資料是「由新到舊」，必須反轉陣列以對齊幣安的「由舊到新」
            klines = klines[::-1]
            
            # Bybit 欄位定義: [startTime, openPrice, highPrice, lowPrice, closePrice, volume, turnover]
            df = pd.DataFrame(klines, columns=[
                'open_time', 'open', 'high', 'low', 'close', 'volume', 'turnover'
            ])
            
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            df[numeric_cols] = df[numeric_cols].astype(float)
            df['open_time'] = df['open_time'].astype(int) # 確保時間為整數毫秒
            
            return df
            
        except Exception as e:
            print(f"Bybit K線抓取發生系統錯誤: {e}")
            return pd.DataFrame()    
    
    # ==========================================
    #  改成從 DB 讀取的方法
    # ==========================================

    def get_google_trends_from_db(self, limit=1):
        """ 從 DB 讀取最新的 Google Trends """
        return self.db.load_external_data(
            symbol='GLOBAL', 
            metric='google_trends', 
            limit=limit
        )

    def get_fear_and_greed_from_db(self, limit=1):
        """ 從 DB 讀取恐慌指數 """
        return self.db.load_external_data(
            symbol='GLOBAL', 
            metric='fear_greed', 
            limit=limit
        )

    def get_macro_data_from_db(self, limit=1):
        """ 
        從 DB 讀取總經數據 
        因為 metric 有很多種，這裡可以一次讀出來
        """
        metrics = ['fed_assets', 'yield_10y', 'yield_2y']
        results = {}
        
        for m in metrics:
            df = self.db.load_external_data(symbol='US_MACRO', metric=m, limit=limit)
            if not df.empty:
                results[m] = df.iloc[-1]['value'] # 取最新一筆
            else:
                results[m] = 0
        return results

    def get_qqq_klines_from_db(self, limit=100):
        """ 從 market_data 表讀取 QQQ """
        return self.db.load_market_data(symbol='QQQ', interval='1d', limit=limit)

