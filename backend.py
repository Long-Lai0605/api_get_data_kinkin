import requests
import pandas as pd
import utils
import time
import math

# Hàm gọi API theo logic: Lấy Total -> Tính Page -> Loop
def call_1office_api(method, url, token, from_date=None, to_date=None, callback=None):
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    all_data = []
    limit = 100
    base_params = {'limit': limit}
    
    # Xử lý tham số ngày
    if from_date and from_date not in ['nan', 'None', '']: base_params['from_date'] = from_date
    if to_date and to_date not in ['nan', 'None', '']: base_params['to_date'] = to_date

    if callback: callback(f"📡 Kết nối API: {url} (Method: {method})")
    
    # =========================================================
    # BƯỚC 1: GỌI TRANG 1 ĐỂ THĂM DÒ (Lấy Data + Total)
    # =========================================================
    try:
        params = base_params.copy(); params['page'] = 1
        
        # Gửi Request Page 1
        if method.upper() == "POST":
            resp = requests.post(url, headers=headers, json=params, timeout=45)
        else:
            resp = requests.request(method.upper(), url, headers=headers, params=params, timeout=45)
            
        if resp.status_code != 200: 
            return None, f"⛔ HTTP Error {resp.status_code}: {resp.text[:100]}"
        
        try:
            data_json = resp.json()
        except: return None, "⛔ API lỗi format JSON"

        # Khai thác dữ liệu Page 1
        items_p1 = []
        total_items = 0
        
        if isinstance(data_json, dict):
            # Check lỗi logic
            if data_json.get('status') == 'error':
                 return None, f"⛔ API báo lỗi: {data_json.get('message')}"
            
            items_p1 = data_json.get('data', [])
            if items_p1 is None: items_p1 = []
            
            # --- QUAN TRỌNG: LẤY TOTAL ---
            # 1Office thường trả về 'total' hoặc 'total_item'
            total_items = data_json.get('total') or data_json.get('total_item') or 0
            
        elif isinstance(data_json, list):
            items_p1 = data_json
            
        # Lưu dữ liệu trang 1 ngay lập tức
        if items_p1:
            all_data.extend(items_p1)
            if callback: callback(f"✅ Trang 1: Lấy được {len(items_p1)} dòng.")
        else:
            # Nếu trang 1 rỗng -> Dừng luôn
            if callback: callback(f"🏁 Trang 1 rỗng (Total: {total_items}) -> Kết thúc.")
            return pd.DataFrame(), "Thành công"

    except Exception as e: return None, f"⛔ Lỗi Trang 1: {e}"

    # =========================================================
    # BƯỚC 2: QUYẾT ĐỊNH CHIẾN THUẬT LOOP
    # =========================================================
    
    # CHIẾN THUẬT A: NẾU CÓ TOTAL (Logic chuẩn bạn yêu cầu - Nhanh nhất)
    if total_items and int(total_items) > 0:
        total_items = int(total_items)
        total_pages = math.ceil(total_items / limit)
        
        if callback: callback(f"📊 Tìm thấy Total: {total_items} dòng -> Cần quét {total_pages} trang.")
        
        # Nếu chỉ có 1 trang thì xong luôn
        if total_pages <= 1:
            return pd.DataFrame(all_data), "Thành công"
            
        # Loop từ trang 2 đến trang cuối (Total Pages)
        for page in range(2, total_pages + 1):
            params['page'] = page
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
                        if callback: callback(f"✅ Trang {page}/{total_pages}: +{len(p_items)} dòng")
                    else:
                        if callback: callback(f"⚠️ Trang {page} rỗng bất thường (Dữ liệu bị lệch).")
                else:
                    if callback: callback(f"❌ Trang {page} lỗi HTTP {r.status_code}")
                
                time.sleep(0.1) # Delay nhẹ để server thở
            except Exception as e:
                if callback: callback(f"❌ Lỗi trang {page}: {e}")

    # CHIẾN THUẬT B: NẾU KHÔNG CÓ TOTAL (Dự phòng - Deep Scan)
    else:
        if callback: callback("⚠️ API không trả về 'total' -> Chuyển sang chế độ dò từng trang (Deep Scan)...")
        page = 2
        empty_streak = 0
        
        while True:
            params['page'] = page
            try:
                if method.upper() == "POST":
                    r = requests.post(url, headers=headers, json=params, timeout=45)
                else:
                    r = requests.request(method.upper(), url, headers=headers, params=params, timeout=45)
                
                if r.status_code != 200: break
                
                d_json = r.json()
                p_items = d_json.get('data', []) if isinstance(d_json, dict) else []
                
                if not p_items:
                    empty_streak += 1
                    if callback: callback(f"⚠️ Trang {page} rỗng ({empty_streak}/5)...")
                    if empty_streak >= 5: break # Dừng nếu 5 trang liên tiếp rỗng
                else:
                    empty_streak = 0
                    all_data.extend(p_items)
                    if callback: callback(f"✅ Trang {page}: +{len(p_items)} dòng")
                    if len(p_items) < limit: break # Hết trang
                
                page += 1
                time.sleep(0.15)
                
            except: break

    return pd.DataFrame(all_data), "Thành công"

# Hàm process_sync giữ nguyên logic kết nối Sheet
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
    if df.empty: return True, "0 dòng (API trả về rỗng)", 0

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
