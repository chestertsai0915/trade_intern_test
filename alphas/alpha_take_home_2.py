import numpy as np
import pandas as pd
from alphas.base import BaseAlpha
import alphas.alpha_tools as tls

class Strategy(BaseAlpha):

    requirements = BaseAlpha.requirements + [
        "close",
        "vol_adj_mom_20_close_v1",  
        "vol_adj_mom_25_close_v1",
        "vol_adj_mom_50_close_v1",
        "vol_adj_mom_65_close_v1",
        "vol_adj_mom_70_close_v1",
        "vol_adj_mom_100_close_v1",
        "vol_adj_mom_150_close_v1",
        "vol_adj_mom_200_close_v1",
        "vol_adj_mom_300_close_v1",
        "vol_adj_mom_600_close_v1"
    ]
    
    default_params = {
        "factor_x": "vol_adj_mom_100_close_v1", 
        "mom_smooth": 30,           
        "zscore_window": 1440,      
        
        "trend_fast": 240,          
        "trend_slow": 1440,         
        
        "entry_z": 1.0,             
        "exit_z": -0.5,             
        
        # 停利停損 (稍微放寬，避免被 1 分鐘雜訊洗掉)
        "take_profit": 0.015,       # 停利：1.5%
        "stop_loss": 0.005,         # 停損：0.5%
    }

    def prepare_features(self, df):
        df = tls.add_ewm_sma(df, column=self.params["factor_x"], window=self.params["mom_smooth"], out_name='mom_smoothed')
        df = tls.add_zscore(df, column='mom_smoothed', window=self.params["zscore_window"], out_name='mom_z')
        
        df = tls.add_sma(df, column='close', window=self.params["trend_fast"], out_name='ma_fast')
        df = tls.add_sma(df, column='close', window=self.params["trend_slow"], out_name='ma_slow')
        df['trend_regime'] = np.where(df['ma_fast'] > df['ma_slow'], 1, -1)
        
        return df

    def generate_target_position(self, row, account):
        mom_z = row.get('mom_z', 0)
        trend = row.get('trend_regime', 0)
        current_price = row.get('close', 0)
        
        if np.isnan(mom_z) or np.isnan(trend) or current_price == 0:
            return None 

        # ==========================================
        # 🔒 全新防禦：冷卻鎖定系統 (防無限重入)
        # ==========================================
        if not hasattr(self, 'locked_direction'):
            self.locked_direction = 0  # 0: 無鎖, 1: 鎖死多單, -1: 鎖死空單

        # 解鎖條件：只要動能回到中性區間 (-0.5 ~ +0.5)，代表這波行情結束，解除鎖定！
        if abs(mom_z) < 0.5:
            self.locked_direction = 0

        entry_z = self.params["entry_z"]
        exit_z = self.params["exit_z"] 
        tp_pct = self.params["take_profit"]
        sl_pct = self.params["stop_loss"]

        # ==========================================
        # 🛡️ 絕對風控層：強制停利與停損 + 上鎖
        # ==========================================
        if account.position != 0 and account.avg_price > 0:
            unrealized_pct = 0.0
            if account.position > 0:
                unrealized_pct = (current_price - account.avg_price) / account.avg_price
            elif account.position < 0:
                unrealized_pct = (account.avg_price - current_price) / account.avg_price
                
            # 觸發停利或停損
            if unrealized_pct >= tp_pct or unrealized_pct <= -sl_pct:
                # 【關鍵】記住我們是因為停利/停損出場的，把該方向鎖死！
                self.locked_direction = 1 if account.position > 0 else -1
                return 0.0 

        # ==========================================
        # 📈 策略訊號層：進場與指標出場
        # ==========================================
        
        # 0. 大趨勢反轉逃命 (這種是自然換向，不用上鎖)
        if account.position > 0 and trend == -1: return 0.0
        if account.position < 0 and trend == 1: return 0.0

        # 1. 多頭邏輯 (必須確認多單沒有被鎖死！)
        if trend == 1:
            if mom_z > entry_z and self.locked_direction != 1:
                return 0.99             
            elif account.position > 0 and mom_z < exit_z:
                return 0.0              

        # 2. 空頭邏輯 (必須確認空單沒有被鎖死！)
        elif trend == -1:
            if mom_z < -entry_z and self.locked_direction != -1:
                return -0.99            
            elif account.position < 0 and mom_z > -exit_z: 
                return 0.0              
                
        # 3. 抱單等待
        return None