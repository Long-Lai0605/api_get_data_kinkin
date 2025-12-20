import requests
import pandas as pd
import utils
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import time

def call_1office_api(method, url, token, from_date=None, to_date=None):
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    all_data = []
    page = 1; limit = 100
    base_params = {'limit': limit}
    if from_date: base_params['from_date'] = from_date
    if to_date: base_params['to_date'] = to_date

    try:
        while True:
            params = base_params.copy(); params['page'] = page
            if method.upper() == "POST":
                resp = requests.post(url, headers=headers, json=params, timeout=30)
            else:
                resp = requests.request(method.upper(), url, headers=headers, params=params, timeout=30)

            if resp.status_code == 401: return None, "Hết hạn API"
            if resp.status_code != 200: return None, f"HTTP Error {resp.status_code}"

            data = resp.json()
            items = data.get('data', []) if isinstance(data, dict) else data
            if not items: break
            all_data.extend(items)
            if len(items) < limit: break
            page += 1
        return pd.DataFrame(all_data), "Thành công"
    except Exception as e: return None, str(e)

def process_sync(row_config, block_name):
    # --- CẬP NHẬT TÊN CỘT MỚI TẠI ĐÂY ---
    url = str(row_config.get('API URL', '')).strip()
    real_token = utils.get_real_token(block_name, url)
    
    if not real_token: return False, "Thiếu Token", 0
    
    method = str(row_config.get('Method', 'GET')).strip()
    target_link = str(row_config.get('Link Đích', '')).strip()
    sheet_name = str(row_config.get('Tên sheet dữ liệu dịch', 'Sheet1')).strip()
    
    # Check ngày chốt/Tháng (nếu cần xử lý thêm logic ngày thì thêm ở đây)
    
    # Fetch Data
    df, msg = call_1office_api(method, url, real_token)
    if df is None: return False, msg, 0
    if df.empty: return True, "Không có dữ liệu mới", 0

    # Transform
    df['Link file nguồn'] = url
    df['Sheet nguồn'] = "1Office"
    df['Tháng chốt'] = row_config.get('Tháng', '')
    df['Luồng'] = block_name

    # Sync
    try:
        creds = utils.get_creds()
        gc = utils.gspread.authorize(creds)
        sh = gc.open_by_url(target_link)
        try: wks = sh.worksheet(sheet_name)
        except: wks = sh.add_worksheet(sheet_name, 1000, 20)
        
        existing = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        if 'Link file nguồn' in existing.columns:
            existing = existing[existing['Link file nguồn'] != url]
        
        final_df = pd.concat([existing, df], ignore_index=True)
        wks.clear()
        set_with_dataframe(wks, final_df)
        
        return True, "Thành công", len(final_df)
    except Exception as e:
        return False, f"Lỗi Ghi Sheet: {str(e)}", 0
