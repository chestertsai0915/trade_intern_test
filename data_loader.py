import pandas as pd
from binance.um_futures import UMFutures
from utils.database import DatabaseHandler
from pybit.unified_trading import HTTP as BybitHTTP  # 引入 Bybit V5 SDK

class DataLoader:
    def __init__(self, client: UMFutures,  bybit_client: BybitHTTP = None, db: DatabaseHandler = None):
        self.client = client
        self.db_handler = db
        self.bybit_client = bybit_client
        
    def get_binance_klines(self, symbol, interval, limit=100):
        """ 
        幣安 K 線抓取，
        """
        try:
            klines = self.client.klines(symbol=symbol, interval=interval, limit=limit)
            df = pd.DataFrame(klines, columns=[
                'open_time', 'open', 'high', 'low', 'close', 'volume', 
                'close_time', 'q_vol', 'trades', 'taker_buy_vol', 'taker_buy_q_vol', 'ignore'
            ])
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            df[numeric_cols] = df[numeric_cols].astype(float)
            
            # 確保時間是整數
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
        
    def get_bybit_klines(self, symbol, interval, limit=100):
        """ 
        Bybit K 線抓取 
        Bybit interval 格式：1, 3, 5, 15, 60, D, W, M
        """
        try:
            # Bybit V5 獲取 K 線
            response = self.bybit_client.get_kline(
                category="linear", # U本位合約
                symbol=symbol,
                interval=str(interval), 
                limit=limit
            )
            
            # Bybit 回傳的資料在 result['list'] 中
            klines = response.get('result', {}).get('list', [])
            
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
            print(f"Bybit K線抓取失敗: {e}")
            return pd.DataFrame()

    def get_bybit_orderbook_depth(self, symbol, limit=5):
        """
        獲取 Bybit 合約 L2 訂單簿深度數據 
        """
        try:
            # 【關鍵】Bybit U本位深度 limit 只能是 1, 50, 200, 500。這裡請求 50 檔。
            response = self.bybit_client.get_orderbook(
                category="linear",
                symbol=symbol,
                limit=50
            )
            
            result = response.get('result', {})
            # 利用切片 [:limit] 只取出前 5 檔
            bids = result.get('b', [])[:limit]
            asks = result.get('a', [])[:limit]
            timestamp = result.get('ts')
            
            df_depth = pd.DataFrame({
                'timestamp': int(timestamp),
                'level': range(1, len(bids) + 1),
                'bid_price': [float(b[0]) for b in bids],
                'bid_qty': [float(b[1]) for b in bids],
                'ask_price': [float(a[0]) for a in asks],
                'ask_qty': [float(a[1]) for a in asks]
            })
            
            return df_depth

        except Exception as e:
            print(f"Bybit 深度數據抓取失敗: {e}")
            return pd.DataFrame()


