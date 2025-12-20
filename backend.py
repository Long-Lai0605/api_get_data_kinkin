import requests
import pandas as pd
import utils
import time
import math

# Hàm gọi API theo logic: Probe Total -> Loop 1..N
def call_1office_api(method, url, token, from_date=None, to_date=None, callback=None):
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    all_data = []
    limit = 100
    base_params = {'limit': limit}
    
    if from_date and from_date not in ['nan', 'None', '']: base_params['from_date'] = from_date
    if to_date and to_date not in ['nan', 'None', '']: base_params['to_date'] = to_date

    if callback: callback(f"📡 Kết nối API: {url} (Method: {method})")
    
    # =========================================================
    # BƯỚC 1: PROBE (THĂM DÒ) - CHỈ ĐỂ LẤY TOTAL
    # =========================================================
    total_items = 0
    total_pages = 0
    
    try:
        # Gọi thử trang 1
        params = base_params.copy(); params['page'] = 1
        if callback: callback("🔍 Đang thăm dò tổng số dữ liệu (Probe)...")

        if method.upper() == "POST":
            resp = requests.post(url, headers=headers, json=params, timeout=45)
        else:
            resp = requests.request(method.upper(), url, headers=headers, params=params, timeout=45)
            
        if resp.status_code != 200: 
            return None, f"⛔ HTTP Error {resp.status_code}: {resp.text[:100]}"
        
        try: data_json = resp.json()
        except: return None, "⛔ API lỗi format JSON"

        # Lấy Total
        if isinstance(data_json, dict):
             # Check lỗi từ 1Office
            if data_json.get('status') == 'error':
                 return None, f"⛔ API báo lỗi: {data_json.get('message')}"
            
            # Ưu tiên lấy 'total' hoặc 'total_item'
            total_items = data_json.get('total') or data_json.get('total_item') or 0
        
        # Nếu không lấy được Total (API trả về list hoặc thiếu key) -> Fallback: đếm thủ công trang 1
        if total_items == 0 and isinstance(data_json, dict) and data_json.get('data'):
             # Nếu API lởm không trả total nhưng có data, ta giả định ít nhất có 1 trang
             total_items = len(data_json.get('data'))
             if callback: callback("⚠️ API không trả 'total', tạm tính theo dữ liệu tìm thấy.")

    except Exception as e: return None, f"⛔ Lỗi Probe: {e}"

    # =========================================================
    # BƯỚC 2: TÍNH TOÁN SỐ TRANG (PLANNING)
    # =========================================================
    if int(total_items) == 0:
        if callback: callback("🏁 Total = 0. Không có dữ liệu để chạy.")
        return pd.DataFrame(), "Thành công"

    total_pages = math.ceil(int(total_items) / limit)
    if callback: callback(f"📊 Tìm thấy {total_items} dòng -> Kế hoạch: Quét {total_pages} trang.")

    # =========================================================
    # BƯỚC 3: LOOP 1..N (THỰC THI)
    # =========================================================
    for page in range(1, total_pages + 1):
        params['page'] = page
        
        # Retry cơ bản (nếu mạng lag thì thử lại 1 lần)
        for retry in range(2):
            try:
                if method.upper() == "POST":
                    r = requests.post(url, headers=headers, json=params, timeout=45)
                else:
                    r = requests.request(method.upper(), url, headers=headers, params=params, timeout=45)
                
                if r.status_code == 200:
                    d_json = r.json()
                    p_items = d_json.get('data', []) if isinstance(d_json, dict) else []
                    
                    if p_items:
                        all_data.extend(p_items)
                        if callback: callback(f"✅ Trang {page}/{total_pages}: Lấy được {len(p_items)} dòng")
                    else:
                        if callback: callback(f"⚠️ Trang {page} rỗng (Dù kế hoạch tính là có).")
                    
                    break # Thành công thì thoát retry
                else:
                    if callback: callback(f"❌ Trang {page} HTTP {r.status_code}. Thử lại...")
                    time.sleep(1)
            except Exception as e:
                if callback: callback(f"❌ Lỗi trang {page}: {e}")
                time.sleep(1)
        
        time.sleep(0.1) # Nghỉ chút để server không chặn

    return pd.DataFrame(all_data), "Thành công"

# Hàm process_sync GIỮ NGUYÊN (Không đổi logic ghi sheet)
def process_sync(row_config, block_name, callback=None):
    if callback: callback("🔑 Đang lấy Token...")
    url = str(row_config.get('API URL', '')).strip()
    real_token = utils.get_real_token(block_name, url)
    if not real_token: return False, "Thiếu Token", 0
    
    method = str(row_config.get('Method', 'GET')).strip()
    target_link = str(row_config.get('Link Đích', '')).strip()
    sheet_name = str(row_config.get('Tên sheet dữ liệu dịch', 'Sheet1')).strip()
    f_d = str(row_config.get('Ngày bắt đầu', '')); t_d = str(row_config.get('Ngày kết thúc', ''))

    # Gọi API
    df, msg = call_1office_api(method, url, real_token, f_d, t_d, callback=callback)
    
    if df is None: return False, msg, 0
    if df.empty: return True, "0 dòng", 0

    # Ghi Sheet
    if callback: callback(f"⚙️ Đang xử lý {len(df)} dòng dữ liệu...")
    df = df.astype(str).replace(['nan', 'None'], '')
    df['Link file nguồn'] = url; df['Sheet nguồn'] = "1Office"; df['Tháng chốt'] = time.strftime("%m/%Y"); df['Luồng'] = block_name

    try:
        if callback: callback("📑 Đang ghi vào Google Sheet...")
        creds = utils.get_creds(); gc = utils.gspread.authorize(creds)
        sh = gc.open_by_url(target_link)
        try: wks = sh.worksheet(sheet_name)
        except: wks = sh.add_worksheet(sheet_name, 1000, 20)
        
        existing = get_as_dataframe(wks, evaluate_formulas=True, dtype=str).dropna(how='all')
        if 'Link file nguồn' in existing.columns: existing = existing[existing['Link file nguồn'] != url]
        
        final_df = pd.concat([existing, df], ignore_index=True)
        wks.clear(); set_with_dataframe(wks, final_df)
        return True, "Thành công", len(df)
    except Exception as e: return False, f"⛔ Lỗi Ghi Sheet: {str(e)}", 0
