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

# --- HELPER ---
def safe_get_records(wks):
    try:
        data = wks.get_all_values()
        if not data: return []
        header = [str(h).strip() for h in data[0]]
        rows = data[1:]
        result = []
        for row in rows:
            item = {}
            for i, col_name in enumerate(header):
                val = row[i] if i < len(row) else ""
                item[col_name] = val
            result.append(item)
        return result
    except: return []

# --- INIT DATABASE (CẬP NHẬT SHEET MỚI) ---
def init_database(secrets_dict):
    sh, msg = get_connection(secrets_dict)
    if not sh: return
    
    schemas = {
        "manager_blocks": ["Block ID", "Block Name", "Schedule Type", "Schedule Config", "Status", "Last Run"],
        "manager_links": ["Link ID", "Block ID", "Method", "API URL", "Access Token", "Link Sheet", "Sheet Name", "Filter Key", "Date Start", "Date End", "Status"],
        "log_system": ["Time", "Block", "Message", "Type"],
        # [MỚI] Bảng chứa cấu hình lịch cho Bot đọc
        "lich_chay_tu_dong": ["Block ID", "Block Name", "Frequency", "Config JSON", "Last Updated"],
        # [MỚI] Bảng log lịch sử chạy (thủ công + tự động)
        "log_lan_thuc_thi": ["Run Time", "Block Name", "Trigger Type", "Status", "Details"]
    }
    
    existing = [s.title for s in sh.worksheets()]
    for name, cols in schemas.items():
        if name not in existing:
            try:
                wks = sh.add_worksheet(name, 100, 20)
                wks.append_row(cols)
            except: pass
        else:
            try:
                wks = sh.worksheet(name)
                if not wks.row_values(1): wks.append_row(cols)
            except: pass

# --- CÁC HÀM CRUD ---
def create_block(secrets_dict, block_name):
    sh, _ = get_connection(secrets_dict)
    if not sh: return False
    wks = sh.worksheet("manager_blocks")
    block_id = str(uuid.uuid4())[:8]
    wks.append_row([block_id, block_name, "Thủ công", "{}", "Active", ""])
    return True

def delete_block(secrets_dict, block_id):
    sh, _ = get_connection(secrets_dict)
    if not sh: return False
    
    # 1. Xóa trong manager_blocks
    wks_b = sh.worksheet("manager_blocks")
    cells = wks_b.findall(block_id)
    for r in sorted([c.row for c in cells], reverse=True): wks_b.delete_rows(r)
    
    # 2. Xóa trong manager_links
    wks_l = sh.worksheet("manager_links")
    all_vals = wks_l.get_all_values()
    if all_vals:
        rows_to_keep = [all_vals[0]]
        for row in all_vals[1:]:
            if len(row) > 1 and str(row[1]).strip() != str(block_id).strip():
                rows_to_keep.append(row)
        wks_l.clear()
        wks_l.update(rows_to_keep)

    # 3. [MỚI] Xóa luôn trong lich_chay_tu_dong
    try:
        wks_s = sh.worksheet("lich_chay_tu_dong")
        cells_s = wks_s.findall(block_id)
        for r in sorted([c.row for c in cells_s], reverse=True): wks_s.delete_rows(r)
    except: pass
    
    return True

def get_all_blocks(secrets_dict):
    sh, _ = get_connection(secrets_dict)
    if not sh: return []
    return safe_get_records(sh.worksheet("manager_blocks"))

def get_links_by_block(secrets_dict, block_id):
    sh, _ = get_connection(secrets_dict)
    if not sh: return []
    all_links = safe_get_records(sh.worksheet("manager_links"))
    target_id = str(block_id).strip()
    return [l for l in all_links if str(l.get("Block ID", "")).strip() == target_id]

# --- [CẬP NHẬT] LƯU CẤU HÌNH LỊCH (ĐỒNG BỘ 2 BẢNG) ---
def update_block_config_and_schedule(secrets_dict, block_id, block_name, schedule_type, schedule_config):
    """
    Vừa cập nhật vào manager_blocks (cho App hiển thị)
    Vừa cập nhật vào lich_chay_tu_dong (cho Bot chạy)
    """
    sh, _ = get_connection(secrets_dict)
    if not sh: return False
    
    json_config = json.dumps(schedule_config, ensure_ascii=False)
    now_str = (datetime.utcnow() + timedelta(hours=7)).strftime("%H:%M %d/%m/%Y")

    # 1. Update manager_blocks
    wks_b = sh.worksheet("manager_blocks")
    cell = wks_b.find(block_id)
    if cell:
        wks_b.update_cell(cell.row, 3, schedule_type)
        wks_b.update_cell(cell.row, 4, json_config)

    # 2. Update lich_chay_tu_dong (Upsert: Có rồi thì sửa, chưa có thì thêm)
    wks_s = sh.worksheet("lich_chay_tu_dong")
    cell_s = wks_s.find(block_id)
    
    if cell_s:
        # Update dòng cũ
        wks_s.update_cell(cell_s.row, 2, block_name)
        wks_s.update_cell(cell_s.row, 3, schedule_type)
        wks_s.update_cell(cell_s.row, 4, json_config)
        wks_s.update_cell(cell_s.row, 5, now_str)
    else:
        # Thêm dòng mới
        wks_s.append_row([block_id, block_name, schedule_type, json_config, now_str])
        
    return True

# --- [MỚI] GHI LOG THỰC THI ---
def log_execution_history(secrets_dict, block_name, trigger_type, status, details):
    """
    Ghi lại lịch sử chạy vào sheet log_lan_thuc_thi
    trigger_type: 'Manual' (Bấm tay) hoặc 'Auto' (Bot chạy)
    """
    try:
        sh, _ = get_connection(secrets_dict)
        wks = sh.worksheet("log_lan_thuc_thi")
        now_str = (datetime.utcnow() + timedelta(hours=7)).strftime("%H:%M:%S %d/%m/%Y")
        wks.append_row([now_str, block_name, trigger_type, status, details])
    except: pass

def save_links_bulk(secrets_dict, block_id, df_links):
    sh, _ = get_connection(secrets_dict)
    if not sh: return False
    
    wks = sh.worksheet("manager_links")
    all_vals = wks.get_all_values()
    
    if not all_vals: 
        kept_rows = [["Link ID", "Block ID", "Method", "API URL", "Access Token", "Link Sheet", "Sheet Name", "Filter Key", "Date Start", "Date End", "Status"]]
    else:
        target_block_id = str(block_id).strip()
        kept_rows = [all_vals[0]]
        for r in all_vals[1:]:
            if len(r) > 1 and str(r[1]).strip() != target_block_id:
                kept_rows.append(r)

    new_rows = []
    for i, (_, row) in enumerate(df_links.iterrows(), start=1):
        d_s = row.get("Date Start", "")
        if isinstance(d_s, (pd.Timestamp, datetime)): d_s = d_s.strftime("%Y-%m-%d")
        d_e = row.get("Date End", "")
        if isinstance(d_e, (pd.Timestamp, datetime)): d_e = d_e.strftime("%Y-%m-%d")

        r = [
            str(i),
            str(block_id).strip(),
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

# --- FETCH & WRITE LOGIC (Giữ nguyên) ---
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
