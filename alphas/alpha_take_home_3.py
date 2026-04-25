import numpy as np
import pandas as pd
from alphas.base import BaseAlpha
import alphas.alpha_tools as tls

class Strategy(BaseAlpha):

    requirements = BaseAlpha.requirements + [
        "close",
        "oim_lvl1_v1"
    ]
    
    default_params = {
        "oim_smooth": 10,           # 1. 訊號平滑窗口
        "zscore_window": 120,       # 2. 歷史基準窗口
        "dead_zone": 0.2            # 3. 死區 (訊號小於此值直接歸零)
    }

    def prepare_features(self, df):
        
        
            
        # 步驟一：最單純的均線平滑 (濾除高頻雜訊)
        df = tls.add_ewm_sma(df, column='oim_lvl1_v1', window=self.params["oim_smooth"], out_name='oim_smoothed')
        
        # 步驟二：Z-Score 標準化 
        # (因為我們不知道 OIM 的絕對數值有多大，必須強制把它壓縮到 -3 ~ +3 之間，餵給 tanh 才會有平滑漸進的效果)
        df = tls.add_zscore(df, column='oim_smoothed', window=self.params["zscore_window"], out_name='oim_z')
        
        return df

    def generate_target_position(self, row, account):
        oim_z = row.get('oim_z', 0)
        print(f"oim_z: {oim_z}")  # Debug: 印出 oim_z 的值，看看是不是有正常計算出來
        if np.isnan(oim_z):
            return None 

        # ==========================================
        # 最純粹的數學轉換：Z-Score 直接對應倉位大小
        # ==========================================
        # tanh 會完美地將 -3 ~ +3 的數值，映射成 -0.99 ~ +0.99 的倉位
        target_pos = np.tanh(oim_z)

        # 死區：如果算出來的目標倉位太小 (例如只建議開 15% 倉)，乾脆空手，省手續費
        if abs(target_pos) < self.params["dead_zone"]:
            return 0.0

        # 【關鍵小技巧】四捨五入到小數點後第一位 (例如 0.52 變成 0.5)
        # 這能避免引擎為了一點點小數點的變動 (從 52% 倉位變成 53% 倉位) 就跑去交易繳手續費
        final_pos = round(target_pos, 1)

        return final_pos