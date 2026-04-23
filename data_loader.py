import pandas as pd
from binance.um_futures import UMFutures
from utils.database import DatabaseHandler
from pybit.unified_trading import HTTP as BybitHTTP  # 引入 Bybit V5 SDK

class DataLoader:
    def __init__(self, client: UMFutures, db: DatabaseHandler = None):
        self.client = client
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
        
    def get_orderbook_depth(self, symbol, limit=5):
        """
        獲取幣安合約 L2 訂單簿深度數據 (包含 Top 5 Bid/Ask 價格與掛單量)
        """
        try:
            # 呼叫 UMFutures SDK 的 depth 方法，設定 limit 來限制檔位數量
            depth_data = self.client.depth(symbol=symbol, limit=limit)
            
            # 幣安回傳的 bids 與 asks 格式皆為: [[價格字串, 數量字串], [價格字串, 數量字串]...]
            bids = depth_data.get('bids', [])
            asks = depth_data.get('asks', [])
            
            # 取得撮合引擎時間戳 (毫秒)，方便後續與 K 線對齊
            timestamp = depth_data.get('T') 
            
            # 將資料轉為浮點數，並整理成 DataFrame
            df_depth = pd.DataFrame({
                'timestamp': timestamp,
                'level': range(1, len(bids) + 1),               # 檔位 1~5
                'bid_price': [float(b[0]) for b in bids],       # 買價
                'bid_qty': [float(b[1]) for b in bids],         # 買單量
                'ask_price': [float(a[0]) for a in asks],       # 賣價
                'ask_qty': [float(a[1]) for a in asks]          # 賣單量
            })
            
            return df_depth

        except Exception as e:
            print(f"幣安深度數據抓取失敗: {e}")
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

