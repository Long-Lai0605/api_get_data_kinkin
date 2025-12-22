import gspread
import requests
import pandas as pd
import math
import time
import toml
import json  # <--- Bắt buộc import json
from datetime import datetime
from google.oauth2.service_account import Credentials
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CẤU HÌNH ---
SCOPE = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

# ... (Các hàm load_secrets, get_connection, init_database GIỮ NGUYÊN) ...

# --- HÀM GỌI API ĐƠN LẺ (ĐÃ UPDATE PARAMS) ---
def fetch_single_page(url, params, method, page_num):
    p = params.copy()
    p["page"] = page_num
    try:
        if method.upper() == "POST":
            r = requests.post(url, params=p, json={}, timeout=30)
        else:
            r = requests.get(url, params=p, timeout=30)
        if r.status_code == 200:
            d = r.json()
            return d.get("data", d.get("items", []))
    except: pass
    return []

# --- HÀM FETCH THÔNG MINH (SERVER-SIDE FILTERING) ---
def fetch_1office_data_smart(url, token, method="GET", 
                             filter_key=None, date_start=None, date_end=None, 
                             status_callback=None):
    all_data = []
    limit = 100
    clean_token = str(token).strip()
    
    # 1. Base Params
    params = {
        "access_token": clean_token,
        "limit": limit
    }

    # 2. [CỐT LÕI] TẠO BỘ LỌC SERVER-SIDE
    # Thay vì tải hết, ta ép API chỉ trả dữ liệu trong khoảng ngày
    if filter_key:
        filters_dict = {}
        has_filter = False
        
        # 1Office thường dùng format dd/mm/yyyy cho filter
        if date_start:
            filters_dict[f"{filter_key}_from"] = date_start.strftime("%d/%m/%Y")
            has_filter = True
        if date_end:
            filters_dict[f"{filter_key}_to"] = date_end.strftime("%d/%m/%Y")
            has_filter = True
            
        if has_filter:
            # Chuyển dict thành JSON string theo đúng chuẩn file mẫu dòng 40
            params["filters"] = json.dumps(filters_dict)
            if status_callback:
                status_callback(f"🎯 Đang gửi lệnh lọc lên Server: {filters_dict}")
    
    # BƯỚC 1: LẤY PAGE 1 (Để xem Server trả về bao nhiêu kết quả sau khi lọc)
    if status_callback: status_callback("📡 Đang gọi Page 1...")

    try:
        if method.upper() == "POST":
            res = requests.post(url, params={**params, "page": 1}, json={}, timeout=30)
        else:
            res = requests.get(url, params={**params, "page": 1}, timeout=30)
            
        if res.status_code != 200: return None, f"HTTP {res.status_code}"
        d = res.json()
        if d.get("code") == "token_not_valid": return None, "Hết hạn API"
        
        # total_item lúc này chỉ là số lượng bản ghi ĐÃ LỌC (Rất ít)
        total_items = d.get("total_item", 0)
        items = d.get("data", d.get("items", []))
        if items: all_data.extend(items)
        
        if total_items == 0: 
            return [], "Success (0 kết quả khớp bộ lọc)"

        # BƯỚC 2: TÍNH TOÁN SỐ TRANG
        # Ví dụ: Tổng 100k, nhưng lọc tháng này chỉ còn 200 dòng -> total_pages = 2
        total_pages = math.ceil(total_items / limit)
        
        if total_pages > 1:
            if status_callback: 
                status_callback(f"🚀 Server báo có {total_items} dòng ({total_pages} trang) khớp điều kiện. Đang tải...")
            
            with ThreadPoolExecutor(max_workers=10) as executor:
                # Truyền params (đã chứa filters) vào các luồng con
                futures = {executor.submit(fetch_single_page, url, params, method, p): p for p in range(2, total_pages + 1)}
                
                for future in as_completed(futures):
                    page_items = future.result()
                    if page_items:
                        all_data.extend(page_items)
                    
        return all_data, "Success"
        
    except Exception as e:
        return None, str(e)

# --- [QUAN TRỌNG] HÀM GHI SHEET (KIỂM TRA HEADER & APPEND) ---
def write_to_sheet_range(secrets_dict, block_conf, data):
    if not data: return "0", "No Data"
    
    try:
        creds = Credentials.from_service_account_info(secrets_dict["gcp_service_account"], scopes=SCOPE)
        gc = gspread.authorize(creds)
        dest_ss = gc.open_by_url(block_conf['Link Đích'])
        wks_name = block_conf['Sheet Đích']
        
        try: wks = dest_ss.worksheet(wks_name)
        except: wks = dest_ss.add_worksheet(wks_name, 1000, 20)

        # 1. KIỂM TRA HEADER (Chỉ đọc dòng 1 để tiết kiệm băng thông)
        first_row_vals = wks.row_values(1)
        has_header = len(first_row_vals) > 0
        
        rows_to_write = []
        
        # 2. TẠO HEADER NẾU CHƯA CÓ
        if not has_header:
            first_item = data[0]
            api_headers = list(first_item.keys())
            # Thêm cột hệ thống
            system_headers = ["Link Nguồn", "Sheet Nguồn", "Tháng Chốt", "Luồng (Block)"]
            rows_to_write.append(api_headers + system_headers)

        # 3. CHUẨN BỊ DATA
        month = datetime.now().strftime("%m/%Y")
        b_name = block_conf['Block Name']
        
        for item in data:
            # Logic map dữ liệu khớp header
            if not has_header:
                r = [item.get(k, "") for k in api_headers]
            else:
                # Nếu sheet cũ, dùng values (chấp nhận rủi ro đổi cấu trúc để đổi lấy tốc độ)
                r = list(item.values())

            r = [str(x) if isinstance(x, (dict, list)) else x for x in r]
            r.extend([block_conf['Link Đích'], wks_name, month, b_name])
            rows_to_write.append(r)
            
        # 4. GHI APPEND
        wks.append_rows(rows_to_write)
        
        range_str = f"+{len(data)} dòng mới"
        update_master_status(secrets_dict, b_name, range_str)
        
        return range_str, "Success"
        
    except Exception as e:
        return "0", f"Write Error: {e}"

def update_master_status(secrets_dict, block_name, range_str):
    try:
        sh, _ = get_connection(secrets_dict)
        wks = sh.worksheet("luu_cau_hinh")
        cell = wks.find(block_name)
        if cell:
            now = datetime.now().strftime("%H:%M %d/%m")
            wks.update_cell(cell.row, 8, now)
            wks.update_cell(cell.row, 9, range_str)
    except: pass

def get_active_blocks(secrets_dict):
    sh, _ = get_connection(secrets_dict)
    if not sh: return []
    try:
        c = pd.DataFrame(sh.worksheet("luu_cau_hinh").get_all_records())
        s = pd.DataFrame(sh.worksheet("log_api_1office").get_all_records())
        if c.empty or s.empty: return []
        
        c.columns = [x.strip() for x in c.columns]
        s.columns = [x.strip() for x in s.columns]
        if "Filter Key" not in c.columns: c["Filter Key"] = ""

        full = pd.merge(c, s, on="Block Name", how="left")
        display_cols = ["Block Name", "Trạng thái", "Method", "API URL", "Access Token (Encrypted)", 
                        "Link Đích", "Sheet Đích", "Ngày bắt đầu", "Ngày kết thúc", "Filter Key",
                        "Total Rows", "Last Run"]
        final_cols = [col for col in display_cols if col in full.columns]
        return full[final_cols].fillna("").to_dict('records')
    except: return []

def add_new_block(secrets_dict, name, method, url, token, link, sheet, start, end, filter_key):
    sh, _ = get_connection(secrets_dict)
    if not sh: return False
    sh.worksheet("luu_cau_hinh").append_row([
        name, "Chưa chốt & đang cập nhật", str(start), str(end), filter_key, link, sheet, "", ""
    ])
    sh.worksheet("log_api_1office").append_row([name, method, url, token.strip()])
    return True
