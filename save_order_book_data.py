import os
import glob
import json
import pandas as pd
import zipfile

def extract_top5_from_bybit_jsonlines_safe(folder_path, output_csv="processed_l2_top5.csv"):
    file_pattern = os.path.join(folder_path, "*.zip")
    file_list = glob.glob(file_pattern)
    
    if not file_list:
        print(f"在 {folder_path} 找不到任何 .zip 壓縮檔！")
        return
        
    print(f"找到 {len(file_list)} 個壓縮檔，準備使用【低記憶體安全模式】處理...")
    
    # 執行前先刪除舊的同名檔案，避免重複接續寫入
    if os.path.exists(output_csv):
        os.remove(output_csv)
        
    total_rows = 0
    
    for idx, file_path in enumerate(file_list):
        print(f"[{idx+1}/{len(file_list)}] 處理中: {os.path.basename(file_path)}")
        parsed_data = [] 
        
        try:
            with zipfile.ZipFile(file_path, 'r') as z:
                target_filename = z.namelist()[0]
                with z.open(target_filename) as f:
                    for line in f:
                        line_str = line.decode('utf-8').strip()
                        if not line_str: continue
                        
                        try:
                            record = json.loads(line_str)
                        except:
                            continue
                            
                        ts = record.get('ts')
                        bids = record.get('data', {}).get('b', [])
                        asks = record.get('data', {}).get('a', [])
                        
                        if not ts or (not bids and not asks): continue
                            
                        row = {'timestamp': int(ts)}
                        for i in range(5):
                            level = i + 1
                            row[f'bid_price_{level}'] = float(bids[i][0]) if len(bids) > i else None
                            row[f'bid_qty_{level}']   = float(bids[i][1]) if len(bids) > i else None
                            row[f'ask_price_{level}'] = float(asks[i][0]) if len(asks) > i else None
                            row[f'ask_qty_{level}']   = float(asks[i][1]) if len(asks) > i else None
                        parsed_data.append(row)
            
            if parsed_data:
                df = pd.DataFrame(parsed_data)
                
                # ==========================================
                # 【關鍵修改】處理完一天，立刻寫入 CSV！
                # ==========================================
                # 如果是第一天 (total_rows == 0)，就寫入欄位名稱 (header=True)
                # 如果是第二天之後，就不寫入欄位名稱 (header=False)，並且用 mode='a' 接在檔案後面
                write_header = (total_rows == 0)
                df.to_csv(output_csv, mode='a', index=False, header=write_header)
                
                total_rows += len(df)
                print(f"  └─ 成功寫入 {len(df)} 筆。 (累積: {total_rows} 筆)\n")
                
                # 釋放記憶體
                del df 
                del parsed_data
                
        except Exception as e:
            print(f"發生錯誤: {e}\n")

    print(f" 任務大功告成！共計寫入 {total_rows} 筆資料至 {output_csv}")

if __name__ == "__main__":
    TARGET_FOLDER = "./bybit_raw_data" 
    extract_top5_from_bybit_jsonlines_safe(folder_path=TARGET_FOLDER)