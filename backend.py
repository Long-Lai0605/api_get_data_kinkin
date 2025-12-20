import requests
import pandas as pd
import utils
import time
import math

def call_1office_api(method, url, token, from_date=None, to_date=None, callback=None):
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    all_data = []
    limit = 100
    base_params = {'limit': limit}
    
    if from_date and from_date not in ['nan', 'None', '']: base_params['from_date'] = from_date
    if to_date and to_date not in ['nan', 'None', '']: base_params['to_date'] = to_date

    if callback: callback(f"📡 Kết nối API: {url} (Method: {method})")
    
    # Biến cờ để quyết định chiến thuật
    use_deep_scan = False
    
    # =========================================================
    # BƯỚC 1: GỌI TRANG 1 ĐỂ THĂM DÒ
    # =========================================================
    try:
        params = base_params.copy(); params['page'] = 1
        
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
            if data_json.get('status') == 'error':
                 return None, f"⛔ API báo lỗi: {data_json.get('message')}"
            
            items_p1 = data_json.get('data', [])
            if items_p1 is None: items_p1 = []
            
            # Lấy Total
            total_items = data_json.get('total') or data_json.get('total_item') or 0
            
        elif isinstance(data_json, list):
            items_p1 = data_json
            
        # Lưu dữ liệu trang 1
        if items_p1:
            all_data.extend(items_p1)
            if callback: callback(f"✅ Trang 1: Lấy được {len(items_p1)} dòng.")
        else:
            # --- SỬA LOGIC TẠI ĐÂY ---
            # Thay vì dừng, ta kích hoạt chế độ Deep Scan để thử vận may ở trang sau
            if callback: callback(f"⚠️ Trang 1 rỗng (Total: {total_items}). Chuyển sang quét sâu (Deep Scan)...")
            use_deep_scan = True

    except Exception as e: return None, f"⛔ Lỗi Trang 1: {e}"

    # =========================================================
    # BƯỚC 2: QUYẾT ĐỊNH CHIẾN THUẬT LOOP
    # =========================================================
    
    # CHIẾN THUẬT A: NẾU CÓ DATA & TOTAL -> TÍNH TOÁN CHUẨN (Nhanh nhất)
    if not use_deep_scan and total_items and int(total_items) > 0:
        total_items = int(total_items)
        total_pages = math.ceil(total_items / limit)
        
        if callback: callback(f"📊 Tìm thấy Total: {total_items} dòng -> Quét {total_pages} trang.")
        
        if total_pages > 1:
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
                            if callback: callback(f"⚠️ Trang {page} rỗng bất thường.")
                    time.sleep(0.1)
                except Exception as e:
                    if callback: callback(f"❌ Lỗi trang {page}: {e}")

    # CHIẾN THUẬT B: DEEP SCAN (Dò tìm mù)
    # Kích hoạt khi: Không có Total HOẶC Trang 1 bị rỗng
    else:
        page = 2
        empty_streak = 0
        MAX_EMPTY = 5 # Cho phép 5 trang rỗng liên tiếp mới dừng
        
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
                    if callback: callback(f"⚠️ Trang {page} rỗng ({empty_streak}/{MAX_EMPTY})...")
                    if empty_streak >= MAX_EMPTY: 
                        if callback: callback("🏁 Dừng sau 5 trang rỗng liên tiếp.")
                        break
                else:
                    empty_streak = 0
                    all_data.extend(p_items)
                    if callback: callback(f"✅ Trang {page}: +{len(p_items)} dòng")
                    # Nếu có data nhưng ít hơn limit -> Có thể là trang cuối, nhưng vẫn thử tiếp 1 chút cho chắc
                    if len(p_items) < limit: 
                         # Logic an toàn: Nếu lấy được ít hơn limit, thử thêm 1 trang nữa rồi dừng
                         pass 
                
                page += 1
                time.sleep(0.15)
                if page > 500: break # Safety break
                
            except: break

    return pd.DataFrame(all_data), "Thành công"

# Hàm process_sync GIỮ NGUYÊN logic kết nối Sheet
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
    if df.empty: return True, "0 dòng (Đã quét hết)", 0

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
