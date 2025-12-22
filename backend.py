import gspread
import requests
import pandas as pd
import math
import time
import toml
import json
import uuid
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode, quote

# --- CẤU HÌNH ---
SCOPE = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

def get_connection(secrets_dict):
    try:
        if not secrets_dict: return None, "Secrets is empty"
        creds = Credentials.from_service_account_info(secrets_dict["gcp_service_account"], scopes=SCOPE)
        gc = gspread.authorize(creds)
        master_id = secrets_dict["system"]["master_sheet_id"]
        return gc.open_by_key(master_id), "Success"
    except Exception as e: return None, str(e)

# --- HELPER: ĐỌC DỮ LIỆU AN TOÀN (QUAN TRỌNG) ---
def safe_get_records(wks):
    """
    Thay thế get_all_records() bằng get_all_values() để tránh lỗi APIError
    khi sheet bị rỗng hoặc lỗi header.
    """
    try:
        data = wks.get_all_values()
        if not data: return [] # Sheet rỗng
        
        header = data[0]
        rows = data[1:]
        
        result = []
        for row in rows:
            # Map header với value thủ công
            item = {}
            for i, col_name in enumerate(header):
                val = row[i] if i < len(row) else ""
                item[col_name] = val
            result.append(item)
        return result
    except Exception as e:
        print(f"Read Error: {e}")
        return []

# --- MIGRATION ---
def migrate_old_data(sh):
    try:
        try: wks_old = sh.worksheet("luu_cau_hinh")
        except: return 
        header = wks_old.row_values(1)
        if "Block ID" in header: return 

        old_data = wks_old.get_all_records()
        if not old_data: return
        wks_blocks = sh.worksheet("manager_blocks")
        wks_links = sh.worksheet("manager_links")
        for row in old_data:
            b_name = row.get("Block Name", "Khối Cũ")
            if not b_name: continue
            b_id = str(uuid.uuid4())[:8]
            wks_blocks.append_row([b_id, b_name, "Thủ công", "{}", "Active", row.get("Last Run", "")])
            l_id = str(uuid.uuid4())[:8]
            wks_links.append_row([
                l_id, b_id, "GET", "", "", row.get("Link Đích", ""),
                row.get("Sheet Đích", ""), row.get("Filter Key", ""),
                str(row.get("Ngày bắt đầu", "")), str(row.get("Ngày kết thúc", "")), "Chưa chốt & đang cập nhật"
            ])
        wks_old.update_title("luu_cau_hinh_OLD_BACKUP")
        return True
    except: return False

def init_database(secrets_dict):
    sh, msg = get_connection(secrets_dict)
    if not sh: return
    
    schemas = {
        "manager_blocks": ["Block ID", "Block Name", "Schedule Type", "Schedule Config", "Status", "Last Run"],
        "manager_links": ["Link ID", "Block ID", "Method", "API URL", "Access Token", "Link Sheet", "Sheet Name", "Filter Key", "Date Start", "Date End", "Status"],
        "log_system": ["Time", "Block", "Message", "Type"]
    }
    
    existing = [s.title for s in sh.worksheets()]
    for name, cols in schemas.items():
        if name not in existing:
            try:
                wks = sh.add_worksheet(name, 100, 20)
                wks.append_row(cols)
            except: pass
        else:
            # [FIX] Kiểm tra nếu sheet tồn tại nhưng RỖNG (mất header) thì điền lại
            try:
                wks = sh.worksheet(name)
                if not wks.row_values(1): # Dòng 1 trống
                     wks.append_row(cols)
            except: pass

    migrate_old_data(sh)

# --- CRUD ---
def create_block(secrets_dict, block_name):
    sh, _ = get_connection(secrets_dict)
    wks = sh.worksheet("manager_blocks")
    block_id = str(uuid.uuid4())[:8]
    wks.append_row([block_id, block_name, "Thủ công", "{}", "Active", ""])
    return True

def delete_block(secrets_dict, block_id):
    sh, _ = get_connection(secrets_dict)
    # Xóa Block
    wks_b = sh.worksheet("manager_blocks")
    cells = wks_b.findall(block_id)
    # Xóa ngược từ dưới lên để ko lệch index
    rows_to_del = sorted([c.row for c in cells], reverse=True)
    for r in rows_to_del: wks_b.delete_rows(r)
    
    # Xóa Links
    wks_l = sh.worksheet("manager_links")
    cells_l = wks_l.findall(block_id)
    rows_l_to_del = sorted([c.row for c in cells_l], reverse=True)
    for r in rows_l_to_del: wks_l.delete_rows(r)
    return True

def get_all_blocks(secrets_dict):
    sh, _ = get_connection(secrets_dict)
    if not sh: return []
    # [FIX] Dùng safe_get_records thay vì get_all_records
    return safe_get_records(sh.worksheet("manager_blocks"))

def get_links_by_block(secrets_dict, block_id):
    sh, _ = get_connection(secrets_dict)
    if not sh: return []
    # [FIX] Dùng safe_get_records thay vì get_all_records
    all_links = safe_get_records(sh.worksheet("manager_links"))
    return [l for l in all_links if str(l.get("Block ID")) == str(block_id)]

def update_block_config(secrets_dict, block_id, schedule_type, schedule_config):
    sh, _ = get_connection(secrets_dict)
    wks = sh.worksheet("manager_blocks")
    cell = wks.find(block_id)
    if cell:
        wks.update_cell(cell.row, 3, schedule_type)
        wks.update_cell(cell.row, 4, json.dumps(schedule_config, ensure_ascii=False))
        return True
    return False

def save_links_bulk(secrets_dict, block_id, df_links):
    sh, _ = get_connection(secrets_dict)
    wks = sh.worksheet("manager_links")
    all_vals = wks.get_all_values()
    if not all_vals: return False
    
    # [FIX] Cẩn thận khi lọc dữ liệu cũ
    kept_rows = [all_vals[0]] # Giữ Header
    for r in all_vals[1:]:
        # Cột Block ID là cột thứ 2 (index 1)
        if len(r) > 1 and str(r[1]) != str(block_id):
            kept_rows.append(r)

    new_rows = []
    for _, row in df_links.iterrows():
        d_s = row.get("Date Start", "")
        if isinstance(d_s, (pd.Timestamp, datetime)): d_s = d_s.strftime("%Y-%m-%d")
        d_e = row.get("Date End", "")
        if isinstance(d_e, (pd.Timestamp, datetime)): d_e = d_e.strftime("%Y-%m-%d")

        r = [
            row.get("Link ID", str(uuid.uuid4())[:8]),
            str(block_id),
            "GET", 
            row.get("API URL", ""),
            row.get("Access Token", ""),
            row.get("Link Sheet", ""),
            row.get("Sheet Name", ""),
            row.get("Filter Key", ""),
            str(d_s),
            str(d_e),
            row.get("Status", "Chưa chốt & đang cập nhật")
        ]
        new_rows.append(r)
    
    wks.clear()
    wks.update(kept_rows + new_rows)
    return True

# --- FETCH LOGIC (Tail Chaser + Day+1) ---
def build_manual_url(base_url, access_token, limit, page, filters_list=None):
    params = {"access_token": str(access_token).strip(), "limit": limit, "page": page, "sort_by": "id", "sort_type": "desc"}
    query_string = urlencode(params)
    filter_part = ""
    if filters_list:
        json_str = json.dumps(filters_list, separators=(',', ':'))
        filter_part = f"&filters={quote(json_str)}"
    return f"{base_url}?{query_string}{filter_part}"

def fetch_single_page_manual(full_url, method):
    try:
        if method.upper() == "POST": r = requests.post(full_url, json={}, timeout=30)
        else: r = requests.get(full_url, timeout=30)
        if r.status_code == 200:
            d = r.json()
            return d.get("data", d.get("items", []))
    except: pass
    return []

def filter_chunk_client_side(items, filter_key, date_start, date_end):
    if not filter_key or (not date_start and not date_end): return items
    filtered = []
    d_start = datetime.combine(date_start, datetime.min.time()) if date_start else None
    d_end = datetime.combine(date_end, datetime.max.time()) if date_end else None
    
    def parse_d(d):
        if not d: return None
        s = str(d).strip()
        fmt = ["%d/%m/%Y %H:%M:%S", "%d/%m/%Y", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S"]
        for f in fmt:
            try: return datetime.strptime(s, f)
            except: continue
        try: return datetime.strptime(s.split(' ')[0], "%d/%m/%Y")
        except: pass
        return None

    for item in items:
        val_str = item.get(filter_key)
        if not val_str: continue 
        val_date = parse_d(val_str)
        if not val_date: 
            filtered.append(item)
            continue
        if d_start and val_date < d_start: continue
        if d_end and val_date > d_end: continue
        filtered.append(item)
    return filtered

def fetch_1office_data_smart(url, token, method="GET", filter_key=None, date_start=None, date_end=None, status_callback=None):
    all_data = []
    limit = 50 
    filters_list = None
    if filter_key and (date_start or date_end):
        f_obj = {}
        if date_start: f_obj[f"{filter_key}_from"] = date_start.strftime("%d/%m/%Y")
        if date_end: 
            server_end_date = date_end + timedelta(days=1)
            f_obj[f"{filter_key}_to"] = server_end_date.strftime("%d/%m/%Y")
        filters_list = [f_obj]

    if status_callback: status_callback("📡 Gọi Page 1...")
    
    page1_url = build_manual_url(url, token, limit, 1, filters_list)
    try:
        if method.upper() == "POST": res = requests.post(page1_url, json={}, timeout=30)
        else: res = requests.get(page1_url, timeout=30)
        
        if res.status_code != 200: return None, f"HTTP {res.status_code}"
        d = res.json()
        if d.get("code") == "token_not_valid": return None, "Hết hạn API"
        
        total_items = d.get("total_item", 0)
        items = d.get("data", d.get("items", []))
        
        if items:
            clean = filter_chunk_client_side(items, filter_key, date_start, date_end)
            all_data.extend(clean)
        
        if total_items == 0 and not items: return [], "Success (0 KQ)"

        estimated_pages = math.ceil(total_items / limit)
        if estimated_pages > 1:
            if status_callback: status_callback(f"🚀 Tải {estimated_pages} trang...")
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(fetch_single_page_manual, build_manual_url(url, token, limit, p, filters_list), method): p for p in range(2, estimated_pages + 1)}
                for future in as_completed(futures):
                    p_items = future.result()
                    if p_items:
                        clean = filter_chunk_client_side(p_items, filter_key, date_start, date_end)
                        all_data.extend(clean)

        current_page = estimated_pages + 1
        max_safety = 20
        while max_safety > 0:
            extra = fetch_single_page_manual(build_manual_url(url, token, limit, current_page, filters_list), method)
            if extra:
                clean = filter_chunk_client_side(extra, filter_key, date_start, date_end)
                all_data.extend(clean)
                current_page += 1
                max_safety -= 1
            else: break
            
        return all_data, "Success"
    except Exception as e: return None, str(e)

def write_to_sheet_range(secrets_dict, link_sheet, sheet_name, block_name, data):
    if not data: return "0", "No Data"
    try:
        creds = Credentials.from_service_account_info(secrets_dict["gcp_service_account"], scopes=SCOPE)
        gc = gspread.authorize(creds)
        dest_ss = gc.open_by_url(link_sheet)
        try: wks = dest_ss.worksheet(sheet_name)
        except: wks = dest_ss.add_worksheet(sheet_name, 1000, 20)
        
        wks.clear()
        if not data: return "0", "Empty Data"

        rows = [list(data[0].keys()) + ["Link Nguồn", "Sheet Nguồn", "Tháng Chốt", "Luồng (Block)"]]
        month = datetime.now().strftime("%m/%Y")
        for item in data:
            r = list(item.values())
            r = [str(x) if isinstance(x, (dict, list)) else x for x in r]
            r.extend([link_sheet, sheet_name, month, block_name])
            rows.append(r)
        wks.update(values=rows, range_name='A1')
        return f"Dòng 2 -> {len(rows)}", "Success"
    except Exception as e: return "0", str(e)
