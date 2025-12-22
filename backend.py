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
from urllib.parse import urlencode, quote

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

# --- [FIX QUAN TRỌNG] HÀM XỬ LÝ NGÀY THÁNG ĐA DẠNG ---
def parse_date_val(date_str):
    """
    Xử lý mọi định dạng ngày tháng:
    - 20/11/2025 17:00:00
    - 07/11/2025
    - 2025-11-20
    """
    if not date_str: return None
    s = str(date_str).strip()
    
    # Danh sách các format phổ biến của 1Office
    formats = [
        "%d/%m/%Y %H:%M:%S", # Dạng có giờ: 20/11/2025 17:00:00
        "%d/%m/%Y",          # Dạng ngắn: 07/11/2025
        "%Y-%m-%d %H:%M:%S", # Dạng chuẩn DB: 2025-11-20 17:00:00
        "%Y-%m-%d",          # Dạng chuẩn ngắn: 2025-11-20
        "%d-%m-%Y"           # Dạng gạch ngang: 20-11-2025
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except: continue
        
    # Nếu thử các kiểu trên vẫn lỗi, thử cắt chuỗi lấy phần đầu (dành cho các case lạ)
    try:
        # Cố gắng cứu dữ liệu bằng cách lấy phần ngày trước dấu cách
        return datetime.strptime(s.split(' ')[0], "%d/%m/%Y")
    except: pass
    
    return None

def filter_chunk_client_side(items, filter_key, date_start, date_end):
    """
    Lớp bảo vệ 2: Lọc lại dữ liệu.
    QUAN TRỌNG: Nếu không parse được ngày -> Mặc định GIỮ LẠI (Fail-open) để tránh mất dữ liệu.
    """
    if not filter_key or (not date_start and not date_end):
        return items
        
    filtered = []
    # date_start/end ở đây là object date, cần chuyển sang datetime để so sánh
    d_start = datetime.combine(date_start, datetime.min.time()) if date_start else None
    d_end = datetime.combine(date_end, datetime.max.time()) if date_end else None # 23:59:59

    for item in items:
        val_str = item.get(filter_key)
        
        # Nếu không có giá trị key này, coi như không thỏa mãn -> Bỏ qua
        if not val_str: 
            continue

        val_date = parse_date_val(val_str)
        
        # [AN TOÀN] Nếu có dữ liệu ngày nhưng format lạ quá không đọc được
        # -> GIỮ LẠI để người dùng kiểm tra thủ công, thà thừa hơn thiếu.
        if not val_date: 
            filtered.append(item)
            continue

        # So sánh logic
        if d_start and val_date < d_start: continue
        if d_end and val_date > d_end: continue
        
        filtered.append(item)
    return filtered

# --- HÀM DỰNG URL ---
def build_manual_url(base_url, access_token, limit, page, filters_dict=None):
    params = {
        "access_token": access_token.strip(),
        "limit": limit,
        "page": page
    }
    query_string = urlencode(params)
    filter_part = ""
    if filters_dict:
        json_str = json.dumps(filters_dict, separators=(',', ':'))
        encoded_json = quote(json_str)
        filter_part = f"&filters={encoded_json}"
    return f"{base_url}?{query_string}{filter_part}"

def fetch_single_page_manual(full_url, method):
    try:
        if method.upper() == "POST":
            r = requests.post(full_url, json={}, timeout=30)
        else:
            r = requests.get(full_url, timeout=30)
        if r.status_code == 200:
            d = r.json()
            return d.get("data", d.get("items", []))
    except: pass
    return []

# --- HÀM FETCH THÔNG MINH (KÍCH HOẠT LẠI LỌC CLIENT) ---
def fetch_1office_data_smart(url, token, method="GET", filter_key=None, date_start=None, date_end=None, status_callback=None):
    all_data = []
    limit = 100
    
    # 1. Bộ lọc Server (Vẫn gửi để hi vọng Server lọc bớt được tí nào hay tí đó)
    filters_dict = None
    if filter_key and (date_start or date_end):
        filters_dict = {}
        if date_start: filters_dict[f"{filter_key}_from"] = date_start.strftime("%Y-%m-%d")
        if date_end: filters_dict[f"{filter_key}_to"] = date_end.strftime("%Y-%m-%d")
        
        if status_callback:
            status_callback(f"🎯 Filter: {json.dumps(filters_dict)}")

    if status_callback: status_callback("📡 Gọi Page 1...")

    page1_url = build_manual_url(url, token, limit, 1, filters_dict)
    
    try:
        if method.upper() == "POST":
            res = requests.post(page1_url, json={}, timeout=30)
        else:
            res = requests.get(page1_url, timeout=30)
            
        if res.status_code != 200: return None, f"HTTP {res.status_code}"
        d = res.json()
        if d.get("code") == "token_not_valid": return None, "Hết hạn API"
        
        total_items = d.get("total_item", 0)
        items = d.get("data", d.get("items", []))
        
        # [KÍCH HOẠT LẠI] Lọc Client-side với hàm parse thông minh
        if items:
            clean_items = filter_chunk_client_side(items, filter_key, date_start, date_end)
            all_data.extend(clean_items)
        
        if total_items == 0: return [], "Success (0 KQ)"

        total_pages = math.ceil(total_items / limit)
        
        if total_pages > 1:
            if status_callback: 
                status_callback(f"🚀 Server trả {total_items} dòng. Đang tải & Lọc kỹ...")
            
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {}
                for p in range(2, total_pages + 1):
                    p_url = build_manual_url(url, token, limit, p, filters_dict)
                    futures[executor.submit(fetch_single_page_manual, p_url, method)] = p
                    
                for future in as_completed(futures):
                    page_items = future.result()
                    if page_items:
                        # [KÍCH HOẠT LẠI] Lọc từng trang con
                        clean_chunk = filter_chunk_client_side(page_items, filter_key, date_start, date_end)
                        all_data.extend(clean_chunk)
                    
        return all_data, "Success"
        
    except Exception as e:
        return None, str(e)

# --- HÀM GHI SHEET (GHI ĐÈ / OVERWRITE) ---
def write_to_sheet_range(secrets_dict, block_conf, data):
    if not data: return "0", "No Data"
    try:
        creds = Credentials.from_service_account_info(secrets_dict["gcp_service_account"], scopes=SCOPE)
        gc = gspread.authorize(creds)
        dest_ss = gc.open_by_url(block_conf['Link Đích'])
        wks_name = block_conf['Sheet Đích']
        try: wks = dest_ss.worksheet(wks_name)
        except: wks = dest_ss.add_worksheet(wks_name, 1000, 20)

        # 1. XÓA DỮ LIỆU CŨ
        wks.clear()

        rows_to_write = []
        
        # 2. TẠO HEADER
        first_item = data[0]
        api_headers = list(first_item.keys())
        system_headers = ["Link Nguồn", "Sheet Nguồn", "Tháng Chốt", "Luồng (Block)"]
        rows_to_write.append(api_headers + system_headers)

        month = datetime.now().strftime("%m/%Y")
        b_name = block_conf['Block Name']
        
        # 3. CHUẨN BỊ DATA
        for item in data:
            r = [item.get(k, "") for k in api_headers]
            r = [str(x) if isinstance(x, (dict, list)) else x for x in r]
            r.extend([block_conf['Link Đích'], wks_name, month, b_name])
            rows_to_write.append(r)
            
        # 4. GHI MỚI
        wks.update(values=rows_to_write, range_name='A1')
        
        range_str = f"Làm mới {len(data)} dòng"
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
