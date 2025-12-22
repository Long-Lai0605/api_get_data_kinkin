import gspread
import requests
import pandas as pd
import math
import time
import toml
import json
from datetime import datetime
from google.oauth2.service_account import Credentials
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode, quote # <--- Import thêm để xử lý URL chuẩn

# --- CẤU HÌNH ---
SCOPE = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

def load_secrets_headless():
    try: return toml.load(".streamlit/secrets.toml")
    except: return None

def get_connection(secrets_dict):
    try:
        if not secrets_dict: return None, "Secrets is empty"
        creds = Credentials.from_service_account_info(secrets_dict["gcp_service_account"], scopes=SCOPE)
        gc = gspread.authorize(creds)
        master_id = secrets_dict["system"]["master_sheet_id"]
        return gc.open_by_key(master_id), "Success"
    except Exception as e: return None, str(e)

def init_database(secrets_dict):
    sh, msg = get_connection(secrets_dict)
    if not sh: return
    
    schemas = {
        "luu_cau_hinh": ["Block Name", "Trạng thái", "Ngày bắt đầu", "Ngày kết thúc", "Filter Key", "Link Đích", "Sheet Đích", "Last Run", "Total Rows"],
        "log_api_1office": ["Block Name", "Method", "API URL", "Access Token (Encrypted)"],
        "log_chay_auto_github": ["Run ID", "Thời gian", "Status", "Message"]
    }
    
    existing = [s.title for s in sh.worksheets()]
    for name, cols in schemas.items():
        if name not in existing:
            try:
                wks = sh.add_worksheet(name, 100, 20)
                wks.append_row(cols)
            except: pass

# --- [FIX QUAN TRỌNG] HÀM GỌI API CHUẨN JSON FILTER ---
def make_request_with_filter(url, params, method):
    """
    Hàm này tự đóng gói URL để đảm bảo filters không bị mã hóa sai.
    Nguyên lý: filters phải là chuỗi JSON nguyên bản được URL Encode.
    """
    try:
        # Tách filters ra xử lý riêng
        filters_json = params.pop("filters", None)
        
        # 1. Nếu là GET: Đóng gói vào URL Query Params
        if method.upper() != "POST":
            # Tạo query string cơ bản
            query_string = urlencode(params)
            
            # Nếu có filters, nối thủ công vào để đảm bảo đúng format
            if filters_json:
                # quote() sẽ chuyển {"k":"v"} thành %7B%22k%22%3A%22v%22%7D (Chuẩn 1Office)
                filter_query = f"filters={quote(filters_json)}"
                full_url = f"{url}?{query_string}&{filter_query}"
            else:
                full_url = f"{url}?{query_string}"
            
            r = requests.get(full_url, timeout=30)
            
        # 2. Nếu là POST: 1Office thường nhận params ở URL kể cả POST
        else:
            # POST vẫn cần filters trên URL (theo tài liệu mẫu dòng 41: buildUrlWithQuery_)
            query_string = urlencode(params)
            if filters_json:
                filter_query = f"filters={quote(filters_json)}"
                full_url = f"{url}?{query_string}&{filter_query}"
            else:
                full_url = f"{url}?{query_string}"
                
            r = requests.post(full_url, json={}, timeout=30)

        if r.status_code == 200:
            d = r.json()
            # Xử lý các trường hợp trả về khác nhau của API
            return d, d.get("data", d.get("items", []))
        return None, []
    except Exception as e:
        return None, []

def fetch_single_page(url, base_params, method, page_num):
    # Copy params để không ảnh hưởng luồng chính
    p = base_params.copy()
    p["page"] = page_num
    
    # Gọi hàm request chuẩn
    _, items = make_request_with_filter(url, p, method)
    return items

def fetch_1office_data_smart(url, token, method="GET", filter_key=None, date_start=None, date_end=None, status_callback=None):
    all_data = []
    limit = 100
    clean_token = str(token).strip()
    
    # Base Params
    params = {
        "access_token": clean_token,
        "limit": limit
    }

    # [FIX] Tạo chuỗi JSON cho Filters
    if filter_key and (date_start or date_end):
        filters_dict = {}
        if date_start: filters_dict[f"{filter_key}_from"] = date_start.strftime("%d/%m/%Y")
        if date_end: filters_dict[f"{filter_key}_to"] = date_end.strftime("%d/%m/%Y")
        
        # CHUYỂN THÀNH JSON STRING NGAY TẠI ĐÂY
        params["filters"] = json.dumps(filters_dict)
        
        if status_callback: 
            status_callback(f"🎯 Đang gửi lệnh lọc Server: {params['filters']}")

    if status_callback: status_callback("📡 Gọi Page 1 kiểm tra số lượng...")

    # BƯỚC 1: LẤY PAGE 1
    d_meta, items = make_request_with_filter(url, {**params, "page": 1}, method)
    
    if d_meta is None: return None, "Lỗi HTTP hoặc Kết nối"
    if d_meta.get("code") == "token_not_valid": return None, "Hết hạn API"
    
    total_items = d_meta.get("total_item", 0)
    
    # Nếu có items ở trang 1, thêm vào list
    if items: all_data.extend(items)
    
    if total_items == 0: return [], "Success (0 KQ)"

    # BƯỚC 2: TÍNH TOÁN
    total_pages = math.ceil(total_items / limit)
    
    if total_pages > 1:
        if status_callback: status_callback(f"🚀 Server báo {total_items} dòng ({total_pages} trang). Tải song song...")
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Truyền params đã có chuỗi json filters
            futures = {executor.submit(fetch_single_page, url, params, method, p): p for p in range(2, total_pages + 1)}
            for future in as_completed(futures):
                page_items = future.result()
                if page_items: all_data.extend(page_items)
                
    return all_data, "Success"

# --- HÀM GHI SHEET (GIỮ NGUYÊN) ---
def write_to_sheet_range(secrets_dict, block_conf, data):
    if not data: return "0", "No Data"
    try:
        creds = Credentials.from_service_account_info(secrets_dict["gcp_service_account"], scopes=SCOPE)
        gc = gspread.authorize(creds)
        dest_ss = gc.open_by_url(block_conf['Link Đích'])
        wks_name = block_conf['Sheet Đích']
        try: wks = dest_ss.worksheet(wks_name)
        except: wks = dest_ss.add_worksheet(wks_name, 1000, 20)

        first_row_vals = wks.row_values(1)
        has_header = len(first_row_vals) > 0
        
        rows_to_write = []
        if not has_header:
            first_item = data[0]
            api_headers = list(first_item.keys())
            system_headers = ["Link Nguồn", "Sheet Nguồn", "Tháng Chốt", "Luồng (Block)"]
            rows_to_write.append(api_headers + system_headers)

        month = datetime.now().strftime("%m/%Y")
        b_name = block_conf['Block Name']
        
        for item in data:
            if not has_header:
                r = [item.get(k, "") for k in api_headers]
            else:
                r = list(item.values())

            r = [str(x) if isinstance(x, (dict, list)) else x for x in r]
            r.extend([block_conf['Link Đích'], wks_name, month, b_name])
            rows_to_write.append(r)
            
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
