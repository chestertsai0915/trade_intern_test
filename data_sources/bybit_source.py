import pandas as pd
from pybit.unified_trading import HTTP
import os

class BybitExternalSource:
    def __init__(self, testnet=False, api_key=None, api_secret=None):
        """
        初始化 Bybit 外部資料源
        如果是抓取公開的 K 線與 Orderbook，不傳入 API Key 也可以正常運作。
        """
        if api_key is None:
            api_key = os.getenv('BYBIT_API_KEY')
            api_secret = os.getenv('BYBIT_SECRET_KEY')

        self.client = HTTP(
            testnet=testnet,
            api_key=api_key,
            api_secret=api_secret
        )

    def get_klines(self, symbol, interval, limit=100, startTime=None, endTime=None):
        """ 
        Bybit K 線抓取 
        Bybit interval 格式：1, 3, 5, 15, 60, D, W, M
        """
        bybit_interval = str(interval)
        if bybit_interval.endswith('m'):
            bybit_interval = bybit_interval[:-1]  # '1m' -> '1', '15m' -> '15'
        elif bybit_interval == '1h':
            bybit_interval = '60'                 # '1h' -> '60'
        elif bybit_interval == '1d':
            bybit_interval = 'D'                  # '1d' -> 'D'
        try:
            # 整理參數
            params = {
                "category": "linear", # U本位合約
                "symbol": symbol,
                "interval": str(interval), 
                "limit": limit
            }
            # Bybit 的時間參數名稱為 start 和 end
            if startTime is not None:
                params["start"] = int(startTime)
            if endTime is not None:
                params["end"] = int(endTime)

            # Bybit V5 獲取 K 線
            response = self.client.get_kline(**params)
            
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

    def get_orderbook_depth(self, symbol, limit=5):
        """
        獲取 Bybit 合約 L2 訂單簿深度數據 
        """
        try:
            # 【關鍵】Bybit U本位深度 limit 只能是 1, 50, 200, 500。這裡請求 50 檔。
            response = self.client.get_orderbook(
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