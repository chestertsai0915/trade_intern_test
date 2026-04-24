# funding_rate_bybit.py
from .base_source import BaseDataSource
import pandas as pd
import os
from pybit.unified_trading import HTTP

class BybitFundingRateFetcher(BaseDataSource):
    name = "funding_rate_bybit"
    
    def __init__(self):
        # 讀取 Bybit 的 API Key
        key = os.getenv('BYBIT_API_KEY')
        secret = os.getenv('BYBIT_SECRET_KEY')
        
        # 建立連線 (使用 Bybit V5 API)
        # 註: 如果只抓取公開數據(如資金費率)，其實可以不給 key 和 secret
        self.client = HTTP(
            testnet=False,
            api_key=key,
            api_secret=secret
        )

    def fetch_data(self, symbol="BTCUSDT", limit=200):
        try:
            # 1. 呼叫 API
            # Bybit V5 需要指定 category="linear" (U本位永續合約)
            response = self.client.get_funding_rate_history(
                category="linear",
                symbol=symbol,
                limit=limit
            )
            
            # 2. 轉成 DataFrame
            # Bybit 的資料陣列會放在 response['result']['list'] 裡面
            data_list = response.get('result', {}).get('list', [])
            df = pd.DataFrame(data_list)
            
            if df.empty: 
                return pd.DataFrame()
            
            # 3. 標準化欄位
            result_df = pd.DataFrame()
            # Bybit 的時間戳欄位名稱為 fundingRateTimestamp
            result_df['open_time'] = df['fundingRateTimestamp'] 
            result_df['symbol'] = df['symbol']
            result_df['metric'] = 'funding_rate_bybit'
            # Bybit 回傳的 fundingRate 是字串格式，需要轉成 float
            result_df['value'] = df['fundingRate'].astype(float)
            
            return result_df
            
        except Exception as e:
            print(f"[BybitFundingRate] Error: {e}")
            return pd.DataFrame()