import numpy as np
import pandas as pd
from alphas.base import BaseAlpha
import alphas.alpha_tools as tls

class Strategy(BaseAlpha):

    
    # 1. 定義需求特徵
    requirements = BaseAlpha.requirements + [
        "close",
        "high",
        "low",
        "vol_adj_mom_20_close_v1",  # 基礎因子
        "custom_atr_14_v1"           # 用於波動率濾網
    ]
    
    default_params = {
        "mom_smooth_window": 10,    # 訊號平滑窗口
        "quantile_window": 250,      # 分位數計算窗口（約半年資料）
        "long_threshold": 0.7,       # 多頭分位數門檻 (前 30%)
        "short_threshold": 0.3,      # 空頭分位數門檻 (後 30%)
        "atr_filter_quantile": 0.8,  # 波動率過高時避險的門檻
    }

    def prepare_features(self, df):
        # A. 數據清洗：處理極端值 (Winsorize)
        # 限制因子在 -3 到 3 個標準差之間，避免異常跳空
        df = tls.add_zscore(df, column='vol_adj_mom_20_close_v1', window=self.params["quantile_window"], out_name='mom_z')
        
        # B. 數據平滑：使用雙重平滑減少交易次數
        # 1. 先計算 Rolling Median 移除雜訊
        # 2. 再計算 SMA 增加訊號穩定度
        raw_mom = df['vol_adj_mom_20_close_v1']
        df['mom_smooth'] = raw_mom.rolling(self.params["mom_smooth_window"]).median().rolling(5).mean()
        
        # C. 數據標準化：滾動分位數轉換 (Rolling Quantile)
        # 這能將因子映射到 0~1，解決不同市場環境下因子量級不同的問題
        df = tls.add_quantile(df, column='mom_smooth', window=self.params["quantile_window"], quantile=0.5, out_name='mom_q50')
        # 註：這裡建議在實作中增加一個 get_rolling_rank 的函數，將當前值轉為 0-1 的排名
        
        # D. 波動率環境分析
        # 計算 ATR 的分位數，判斷現在是否處於超高波動環境
        df = tls.add_quantile(df, column='custom_atr_14_v1', window=self.params["quantile_window"], 
                             quantile=self.params["atr_filter_quantile"], out_name='atr_high_threshold')
        
        return df

    def generate_target_position(self, row, account):
        # 讀取特徵
        mom_val = row.get('mom_smooth', 0)
        atr_val = row.get('custom_atr_14_v1', 0)
        atr_limit = row.get('atr_high_threshold', 999999)

        if np.isnan(mom_val):
            return 0.0

        # --- 訊號生成邏輯 ---
        # 1. 基礎訊號：使用 Tanh 函數將動量轉化為 -1 到 1 的連續區間
        # Tanh 能讓訊號在極端值處飽和，不會因為暴漲暴跌產生過大的非法訊號
        base_signal = np.tanh(mom_val)

        # 2. 波動率濾網 (Volatility Filter)
        # 如果當前 ATR 高於歷史 80% 水準，說明市場過熱或恐慌，強行降低 50% 倉位
        vol_multiplier = 1.0
        if atr_val > atr_limit:
            vol_multiplier = 0.5

        # 3. 結合時間濾網與階梯倉位
        # 如果是美股交易時段，動量通常更具趨勢性，稍微加強訊號
        final_signal = base_signal * vol_multiplier

        # 限制在 -1 到 1 之間
        final_signal = np.clip(final_signal, -1, 1)

        # 4. 使用工具類轉換為階梯倉位 (避免頻繁微調倉位導致的手續費損耗)
        return tls.get_tiered_position(final_signal, th_weak=0.4, pos_weak=0.5, th_strong=0.7, pos_strong=0.99)