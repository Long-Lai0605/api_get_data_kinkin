import requests
import pandas as pd
import utils
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import time
from datetime import datetime

# Hàm gọi API có báo cáo trạng thái
def call_1office_api(method, url, token, from_date=None, to_date=None, callback=None):
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    all_data = []
    page = 1; limit = 100
    base_params = {'limit': limit}
    
    if from_date and from_date not in ['nan', 'None', '']: base_params['from_date'] = from_date
    if to_date and to_date not in ['nan', 'None', '']: base_params['to_date'] = to_date

    if callback: callback(f"📡 Bắt đầu gọi API: {url} (Method: {method})")
    
    try:
        while True:
            params = base_params.copy(); params['page'] = page
            
            # Gửi Request
            try:
                if method.upper() == "POST":
                    resp = requests.post(url, headers=headers, json=params, timeout=45)
                else:
                    resp = requests.request(method.upper(), url, headers=headers, params=params, timeout=45)
            except Exception as e: return None, f"Lỗi mạng: {e}"

            # Check HTTP Code
            if resp.status_code == 401: return None, "⛔ Hết hạn API hoặc Token sai (401)"
            if resp.status_code != 200: return None, f"⛔ HTTP Error {resp.status_code}: {resp.text[:100]}"

            # Parse JSON
            try:
                data_json = resp.json()
            except: return None, "⛔ API trả về dữ liệu không phải JSON"

            # DEBUG: In ra console để check kỹ nếu cần
            print(f"DEBUG Page {page}: {str(data_json)[:200]}")

            # Lấy items
            items = []
            if isinstance(data_json, dict):
                # Check lỗi logic từ 1Office (ví dụ: success=False)
                if data_json.get('status') == 'error':
                    err_msg = data_json.get('message', 'Lỗi không xác định')
                    return None, f"⛔ API báo lỗi: {err_msg}"
                
                items = data_json.get('data', [])
                if items is None: items = []
            elif isinstance(data_json, list):
                items = data_json

            # Báo cáo tiến độ
            if not items:
                if callback: callback(f"🏁 Trang {page} rỗng -> Kết thúc API.")
                break
            
            count_items = len(items)
            all_data.extend(items)
            if callback: callback(f"✅ Trang {page}: Lấy được {count_items} dòng (Tổng: {len(all_data)})")
            
            if count_items < limit:
                if callback: callback("🏁 Đã đến trang cuối.")
                break
            
            page += 1
            time.sleep(0.2)
            
        return pd.DataFrame(all_data), "Thành công"

    except Exception as e:
        return None, f"⛔ Lỗi Code Backend: {str(e)}"

# Hàm xử lý chính có Callback
def process_sync(row_config, block_name, callback=None):
    # 1. Lấy Token
    if callback: callback("🔑 Đang lấy Token bảo mật...")
    url = str(row_config.get('API URL', '')).strip()
    real_token = utils.get_real_token(block_name, url)
    if not real_token: return False, "Thiếu Token", 0
    
    method = str(row_config.get('Method', 'GET')).strip()
    target_link = str(row_config.get('Link Đích', '')).strip()
    sheet_name = str(row_config.get('Tên sheet dữ liệu dịch', 'Sheet1')).strip()
    f_d = str(row_config.get('Ngày bắt đầu', '')); t_d = str(row_config.get('Ngày kết thúc', ''))

    # 2. Gọi API
    df, msg = call_1office_api(method, url, real_token, f_d, t_d, callback=callback)
    
    if df is None: return False, msg, 0
    if df.empty: return True, "⚠️ API trả về 0 dòng (Check lại quyền/param)", 0

    # 3. Ghi Sheet
    if callback: callback(f"⚙️ Đang xử lý {len(df)} dòng dữ liệu...")
    df = df.astype(str).replace(['nan', 'None'], '')
    df['Link file nguồn'] = url; df['Sheet nguồn'] = "1Office"; df['Tháng chốt'] = datetime.now().strftime("%m/%Y"); df['Luồng'] = block_name

    try:
        if callback: callback("📑 Đang kết nối Google Sheet...")
        creds = utils.get_creds(); gc = utils.gspread.authorize(creds)
        sh = gc.open_by_url(target_link)
        
        try: wks = sh.worksheet(sheet_name)
        except: 
            if callback: callback(f"📑 Tạo sheet mới: {sheet_name}...")
            wks = sh.add_worksheet(sheet_name, 1000, 20)
        
        if callback: callback("🧹 Đang lọc và xóa dữ liệu cũ...")
        existing = get_as_dataframe(wks, evaluate_formulas=True, dtype=str).dropna(how='all')
        if 'Link file nguồn' in existing.columns: existing = existing[existing['Link file nguồn'] != url]
        
        if callback: callback("✍️ Đang ghi dữ liệu mới...")
        final_df = pd.concat([existing, df], ignore_index=True)
        wks.clear(); set_with_dataframe(wks, final_df)
        
        return True, "Thành công", len(df)
    except Exception as e: return False, f"⛔ Lỗi Ghi Sheet: {str(e)}", 0
