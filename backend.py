import gspread
import requests
import pandas as pd
import math
import time
import json
import uuid
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode, quote
from gspread_dataframe import set_with_dataframe, get_as_dataframe

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
        return wks.get_all_records()
    except: return []

# --- INIT DATABASE ---
def init_database(secrets_dict):
    sh, msg = get_connection(secrets_dict)
    if not sh: return
    
    schemas = {
        "manager_blocks": ["Block ID", "Block Name", "Schedule Type", "Schedule Config", "Status", "Last Run"],
        "manager_links": ["Link ID", "Block ID", "Method", "API URL", "Access Token", "Link Sheet", "Sheet Name", "Filter Key", "Date Start", "Date End", "Status", "Last Range"],
        "log_system": ["Time", "Block", "Message", "Type"],
        "lich_chay_tu_dong": ["Block ID", "Block Name", "Frequency", "Config JSON", "Last Updated"],
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
            if name == "manager_links":
                try:
                    wks = sh.worksheet(name)
                    headers = wks.row_values(1)
                    if "Last Range" not in headers:
                        wks.update_cell(1, len(headers) + 1, "Last Range")
                except: pass

# --- QUYỀN TRUY CẬP ---
def check_sheet_access(secrets_dict, sheet_url):
    try:
        if not sheet_url or len(sheet_url) < 10: return False, "Link không hợp lệ", ""
        creds_info = secrets_dict["gcp_service_account"]
        bot_email = creds_info.get("client_email", "unknown")
        creds = Credentials.from_service_account_info(creds_info, scopes=SCOPE)
        gc = gspread.authorize(creds)
        gc.open_by_url(sheet_url)
        return True, "✅ Đã có quyền truy cập", bot_email
    except Exception as e: return False, f"⚠️ Lỗi: {str(e)}", ""

# --- CRUD BLOCK ---
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
    wks_b = sh.worksheet("manager_blocks")
    cells = wks_b.findall(block_id)
    for r in sorted([c.row for c in cells], reverse=True): wks_b.delete_rows(r)
    return True

def get_all_blocks(secrets_dict):
    sh, _ = get_connection(secrets_dict)
    if not sh: return []
    return safe_get_records(sh.worksheet("manager_blocks"))

def get_links_by_block(secrets_dict, block_id):
    sh, _ = get_connection(secrets_dict)
    if not sh: return []
    try:
        wks = sh.worksheet("manager_links")
        data = wks.get_all_records()
        target_id = str(block_id).strip()
        # Chuyển đổi Block ID sang string để so sánh
        return [l for l in data if str(l.get("Block ID", "")).strip() == target_id]
    except: return []

# --- UPDATE CONFIG & SCHEDULE ---
def update_block_config_and_schedule(secrets_dict, block_id, block_name, schedule_type, schedule_config):
    sh, _ = get_connection(secrets_dict)
    if not sh: return False
    json_config = json.dumps(schedule_config, ensure_ascii=False)
    now_str = (datetime.utcnow() + timedelta(hours=7)).strftime("%H:%M %d/%m/%Y")
    try:
        wks_b = sh.worksheet("manager_blocks")
        cell = wks_b.find(block_id)
        if cell:
            wks_b.update_cell(cell.row, 3, schedule_type)
            wks_b.update_cell(cell.row, 4, json_config)
    except: pass
    return True

# --- UPDATE REALTIME ---
def update_link_last_range(secrets_dict, link_id, block_id, range_val):
    try:
        sh, _ = get_connection(secrets_dict)
        wks = sh.worksheet("manager_links")
        
        # 1. Tìm vị trí cột "Last Range"
        header = wks.row_values(1)
        try: col_idx = header.index("Last Range") + 1
        except: col_idx = 12 

        # 2. Tìm dòng khớp cả Link ID và Block ID
        all_vals = wks.get_all_values()
        target_link = str(link_id).strip()
        target_block = str(block_id).strip()
        
        for i, row in enumerate(all_vals):
            # Row index trong gspread bắt đầu từ 1, header là dòng 1 => Data bắt đầu check từ dòng 2
            # enumerate bắt đầu từ 0. 
            # row[0] = Link ID, row[1] = Block ID (Giả định theo thứ tự cột)
            if len(row) >= 2:
                if str(row[0]).strip() == target_link and str(row[1]).strip() == target_block:
                    wks.update_cell(i + 1, col_idx, str(range_val))
                    return True
        return False
    except: return False

def log_execution_history(secrets_dict, block_name, trigger_type, status, details):
    try:
        sh, _ = get_connection(secrets_dict)
        wks = sh.worksheet("log_lan_thuc_thi")
        now_str = (datetime.utcnow() + timedelta(hours=7)).strftime("%H:%M:%S %d/%m/%Y")
        wks.append_row([now_str, block_name, trigger_type, status, details])
    except: pass

# --- SAVE LINKS (BULK SAVE) ---
def save_links_bulk(secrets_dict, block_id, df_links):
    sh, _ = get_connection(secrets_dict)
    if not sh: return False
    wks = sh.worksheet("manager_links")
    
    old_df = get_as_dataframe(wks, evaluate_formulas=True).dropna(how='all')
    
    if not old_df.empty and 'Block ID' in old_df.columns:
        old_df['Block ID'] = old_df['Block ID'].astype(str)
        other_blocks_df = old_df[old_df['Block ID'] != str(block_id)]
    else:
        other_blocks_df = pd.DataFrame()

    df_links['Block ID'] = str(block_id)
    final_df = pd.concat([other_blocks_df, df_links], ignore_index=True)
    
    wks.clear()
    set_with_dataframe(wks, final_df)
    return True

# --- FETCH API ---
def fetch_1office_data_smart(url, token, method="GET", filter_key=None, date_start=None, date_end=None, status_callback=None):
    all_data = []
    limit = 100
    filters_list = []
    if filter_key and (date_start or date_end):
        f_obj = {}
        if date_start: f_obj[f"{filter_key}_from"] = date_start.strftime("%d/%m/%Y")
        if date_end: f_obj[f"{filter_key}_to"] = date_end.strftime("%d/%m/%Y")
        if f_obj:
            filters_list.append(f_obj)
            if status_callback: status_callback(f"🎯 Server Filter: {json.dumps(f_obj)}")

    def fetch_page(p_idx):
        params = {"access_token": str(token).strip(), "limit": limit, "page": p_idx}
        if filters_list: params["filters"] = json.dumps(filters_list)
        try:
            full_query = urlencode(params)
            full_url = f"{url}?{full_query}"
            if method.upper() == "POST": r = requests.post(full_url, json={}, timeout=60)
            else: r = requests.get(full_url, timeout=60)
            if r.status_code == 200:
                d = r.json()
                return d.get("data", d.get("items", [])), d.get("total_item", 0)
            return [], 0
        except: return [], 0

    if status_callback: status_callback("📡 Gọi Server...")
    items, total_items = fetch_page(1)
    if items:
        all_data.extend(items)
        if total_items > limit:
            estimated_pages = math.ceil(total_items / limit)
            if status_callback: status_callback(f"🚀 Tải {estimated_pages} trang...")
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(fetch_page, p): p for p in range(2, estimated_pages + 1)}
                for future in as_completed(futures):
                    p_items, _ = future.result()
                    if p_items: all_data.extend(p_items)
    return all_data, "Success"

# --- XỬ LÝ DỮ LIỆU FINAL V4 (FIX LỖI) ---
def process_data_final_v4(secrets_dict, link_sheet_url, sheet_name, block_id, link_id_config, new_data, status_mode):
    if not new_data and status_mode != "Chưa chốt & đang cập nhật": 
        return "0", "No Data from API"
    
    try:
        # 1. KẾT NỐI
        creds = Credentials.from_service_account_info(secrets_dict["gcp_service_account"], scopes=SCOPE)
        gc = gspread.authorize(creds)
        dest_ss = gc.open_by_url(link_sheet_url)
        try: wks = dest_ss.worksheet(sheet_name)
        except: wks = dest_ss.add_worksheet(sheet_name, 1000, 20)
        
        # 2. ĐỌC DỮ LIỆU CŨ
        old_df = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        old_df = old_df.dropna(how='all').dropna(axis=1, how='all')
        
        # 3. CHUẨN BỊ
        meta_cols = ["Link Nguồn", "Sheet Nguồn", "Block ID", "Link ID Config", "Thời gian điền"]
        for col in meta_cols:
            if col not in old_df.columns: old_df[col] = ""

        # PHÂN VÙNG DỮ LIỆU
        target_block = str(block_id).strip()
        target_link = str(link_id_config).strip()
        
        # Lọc an toàn
        is_target = (old_df["Block ID"] == target_block) & (old_df["Link ID Config"] == target_link)
        
        safe_zone_df = old_df[~is_target]
        target_zone_df = old_df[is_target]
        
        if new_data:
            new_df = pd.DataFrame(new_data).astype(str)
            now_str = datetime.now().strftime("%H:%M:%S %d/%m/%Y")
            new_df["Link Nguồn"] = link_sheet_url
            new_df["Sheet Nguồn"] = sheet_name
            new_df["Block ID"] = target_block
            new_df["Link ID Config"] = target_link
            new_df["Thời gian điền"] = now_str
            pk = new_df.columns[0]
        else:
            new_df = pd.DataFrame()
            pk = None

        # 4. XỬ LÝ LOGIC
        result_df = pd.DataFrame()

        if status_mode == "Chưa chốt & đang cập nhật":
            result_df = new_df

        elif status_mode == "Cập nhật dữ liệu cũ":
            if target_zone_df.empty or new_df.empty:
                result_df = target_zone_df
            else:
                common_ids = set(target_zone_df[pk]).intersection(set(new_df[pk]))
                updated_rows = new_df[new_df[pk].isin(common_ids)]
                kept_history = target_zone_df[~target_zone_df[pk].isin(common_ids)]
                result_df = pd.concat([kept_history, updated_rows], ignore_index=True)

        elif status_mode == "Cập nhật dữ liệu mới":
            if target_zone_df.empty:
                result_df = new_df
            elif new_df.empty:
                result_df = target_zone_df
            else:
                existing_ids = set(target_zone_df[pk])
                pure_new_rows = new_df[~new_df[pk].isin(existing_ids)]
                result_df = pd.concat([target_zone_df, pure_new_rows], ignore_index=True)

        else: # "Đã chốt"
            result_df = target_zone_df

        # 5. GHI LẠI
        final_df = pd.concat([safe_zone_df, result_df], ignore_index=True)
        wks.clear()
        set_with_dataframe(wks, final_df)
        
        return f"{len(result_df)}", "Success"

    except Exception as e:
        return "0", str(e)
