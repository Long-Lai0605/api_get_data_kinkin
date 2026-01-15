import gspread
import requests
import pandas as pd
import math
import time
import json
import uuid
import re
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode, quote
from gspread_dataframe import set_with_dataframe, get_as_dataframe

# --- CẤU HÌNH ---
SCOPE = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

def get_connection(secrets_dict):
    try:
        creds = Credentials.from_service_account_info(secrets_dict["gcp_service_account"], scopes=SCOPE)
        gc = gspread.authorize(creds)
        master_id = secrets_dict["system"]["master_sheet_id"]
        return gc.open_by_key(master_id), "Success"
    except Exception as e: return None, str(e)

# --- HELPER ---
def clean_str_series(series):
    # Thêm .str.lstrip("'") để cắt bỏ dấu nháy đơn ở đầu nếu có
    return series.astype(str).str.strip().str.replace(r'\.0$', '', regex=True).str.lstrip("'")

def clean_str(val):
    if pd.isna(val) or val is None: return ""
    # Thêm .lstrip("'") để cắt bỏ dấu nháy đơn khi lấy giá trị đơn lẻ
    return str(val).strip().replace(".0", "").lstrip("'")

def safe_get_records(wks):
    try: return wks.get_all_records()
    except: return []

# --- INIT DATABASE (SCHEMA V20 - LOG CHI TIẾT) ---
def init_database(secrets_dict):
    sh, msg = get_connection(secrets_dict)
    if not sh: return
    schemas = {
        "manager_blocks": ["Block ID", "Block Name", "Schedule Type", "Schedule Config", "Status", "Last Run"],
        "manager_links": ["Link ID", "Block ID", "Method", "API URL", "Access Token", "Link Sheet", "Sheet Name", "Filter Key", "Date Start", "Date End", "Status", "Last Range"],
        "log_system": ["Time", "Block", "Message", "Type"],
        "lich_chay_tu_dong": ["Block ID", "Block Name", "Frequency", "Config JSON", "Last Updated"],
        "log_lan_thuc_thi": ["Time", "Block Name", "Sheet Name", "Trigger Type", "Status", "Updated Range", "Message"]
    }
    existing = [s.title for s in sh.worksheets()]
    for name, cols in schemas.items():
        if name not in existing:
            try: wks = sh.add_worksheet(name, 100, 20); wks.append_row(cols)
            except: pass

# --- LOG FUNCTION (7 CỘT) ---
def log_execution_history(secrets_dict, block_name, sheet_name, trigger_type, status, range_val, message):
    try:
        sh, _ = get_connection(secrets_dict)
        if not sh: return
        wks = sh.worksheet("log_lan_thuc_thi")
        now_str = (datetime.utcnow() + timedelta(hours=7)).strftime("%H:%M:%S %d/%m/%Y")
        wks.append_row([now_str, str(block_name), str(sheet_name), str(trigger_type), str(status), str(range_val), str(message)])
    except Exception as e: print(f"Log Error: {e}") 

# --- CORE FUNCTIONS ---
def check_sheet_access(secrets_dict, sheet_url):
    try:
        creds = Credentials.from_service_account_info(secrets_dict["gcp_service_account"], scopes=SCOPE)
        gspread.authorize(creds).open_by_url(sheet_url)
        # --- SỬA LỖI TẠI ĐÂY: Lấy email trực tiếp từ secrets_dict thay vì creds object ---
        bot_email = secrets_dict["gcp_service_account"]["client_email"]
        return True, "✅ OK", bot_email
    except Exception as e: return False, f"⚠️ Lỗi: {str(e)}", ""

def create_block(secrets_dict, block_name):
    sh, _ = get_connection(secrets_dict)
    if not sh: return False
    sh.worksheet("manager_blocks").append_row([str(uuid.uuid4())[:8], block_name, "Thủ công", "{}", "Active", ""])
    return True

def delete_block(secrets_dict, block_id):
    sh, _ = get_connection(secrets_dict)
    if not sh: return False
    wks = sh.worksheet("manager_blocks")
    cells = wks.findall(block_id)
    for r in sorted([c.row for c in cells], reverse=True): wks.delete_rows(r)
    return True

def get_all_blocks(secrets_dict):
    sh, _ = get_connection(secrets_dict)
    if not sh: return []
    return safe_get_records(sh.worksheet("manager_blocks"))

def get_links_by_block(secrets_dict, block_id):
    sh, _ = get_connection(secrets_dict)
    if not sh: return []
    try:
        data = sh.worksheet("manager_links").get_all_records()
        tid = clean_str(block_id)
        return [l for l in data if clean_str(l.get("Block ID", "")) == tid]
    except: return []

def update_block_config_and_schedule(secrets_dict, block_id, block_name, schedule_type, schedule_config):
    sh, _ = get_connection(secrets_dict)
    if not sh: return False
    try:
        wks = sh.worksheet("manager_blocks")
        cell = wks.find(block_id)
        if cell:
            wks.update_cell(cell.row, 3, schedule_type)
            wks.update_cell(cell.row, 4, json.dumps(schedule_config, ensure_ascii=False))
    except: pass
    return True

def update_link_last_range(secrets_dict, link_id, block_id, range_val):
    try:
        sh, _ = get_connection(secrets_dict)
        wks = sh.worksheet("manager_links")
        all_rows = wks.get_all_values()
        if not all_rows: return False
        try: h = all_rows[0]; idx_r = h.index("Last Range") + 1; idx_l = h.index("Link ID"); idx_b = h.index("Block ID")
        except: idx_r=12; idx_l=0; idx_b=1
        tl, tb = clean_str(link_id), clean_str(block_id)
        for i, row in enumerate(all_rows[1:], start=2):
            cl = clean_str(row[idx_l]) if len(row) > idx_l else ""
            cb = clean_str(row[idx_b]) if len(row) > idx_b else ""
            if cl == tl and cb == tb: wks.update_cell(i, idx_r, str(range_val)); return True
        return False
    except: return False

def save_links_bulk(secrets_dict, block_id, df_links):
    sh, _ = get_connection(secrets_dict)
    if not sh: return False
    wks = sh.worksheet("manager_links")
    
    # 1. Lấy dữ liệu cũ
    old_df = get_as_dataframe(wks, evaluate_formulas=True).dropna(how='all')
    
    # 2. Tách dữ liệu của Block khác ra (để giữ lại)
    tb = clean_str(block_id)
    if not old_df.empty and 'Block ID' in old_df.columns:
        # Dùng clean_str_series để đảm bảo ID so sánh chính xác
        other_df = old_df[clean_str_series(old_df['Block ID']) != tb]
    else: 
        other_df = pd.DataFrame()
    
    # 3. Xử lý dữ liệu mới cần lưu
    df_links['Block ID'] = tb
    df_links = df_links.astype(str) # Ép kiểu chuỗi toàn bộ để tránh lỗi định dạng
    
    # --- QUAN TRỌNG: Thêm dấu ' vào trước Access Token ---
    if 'Access Token' in df_links.columns:
        df_links['Access Token'] = df_links['Access Token'].apply(
            lambda x: "'" + x if x and str(x).strip() != "" and not x.startswith("'") else x
        )
    # -----------------------------------------------------

    # 4. Gộp lại và Ghi đè lên Sheet
    final_df = pd.concat([other_df, df_links], ignore_index=True)
    wks.clear()
    set_with_dataframe(wks, final_df)
    return True

def fetch_1office_data_smart(url, token, method="GET", filter_key=None, date_start=None, date_end=None, status_callback=None):
    all_data = []
    limit = 100
    filters = []
    if filter_key and (date_start or date_end):
        f = {}
        if date_start: f[f"{filter_key}_from"] = date_start.strftime("%d/%m/%Y")
        if date_end: f[f"{filter_key}_to"] = date_end.strftime("%d/%m/%Y")
        if f: filters.append(f)

    def fetch(p):
        prms = {"access_token": str(token).strip(), "limit": limit, "page": p}
        if filters: prms["filters"] = json.dumps(filters)
        try:
            u = f"{url}?{urlencode(prms)}"
            r = requests.post(u, json={}, timeout=60) if method.upper()=="POST" else requests.get(u, timeout=60)
            if r.status_code==200: d=r.json(); return d.get("data", d.get("items", [])), d.get("total_item", 0)
            return [], 0
        except: return [], 0

    if status_callback: status_callback("📡 Gọi Server...")
    items, total = fetch(1)
    if items:
        all_data.extend(items)
        if total > limit:
            with ThreadPoolExecutor(max_workers=5) as ex:
                futures = {ex.submit(fetch, p): p for p in range(2, math.ceil(total/limit)+1)}
                for f in as_completed(futures):
                    p_items, _ = f.result()
                    if p_items: all_data.extend(p_items)
    return all_data, "Success"

def process_data_final_v11(secrets_dict, link_sheet_url, sheet_name, block_id, link_id_config, new_data, status_mode):
    if not new_data and status_mode != "Chưa chốt & đang cập nhật": return "0", "No Data"
    try:
        creds = Credentials.from_service_account_info(secrets_dict["gcp_service_account"], scopes=SCOPE)
        gc = gspread.authorize(creds)
        dest_ss = gc.open_by_url(link_sheet_url)
        try: wks = dest_ss.worksheet(sheet_name)
        except: wks = dest_ss.add_worksheet(sheet_name, 1000, 20)
        
        old_df = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        old_df = old_df.dropna(how='all').dropna(axis=1, how='all')
        meta_cols = ["Link Nguồn", "Sheet Nguồn", "Block ID", "Link ID Config", "Thời gian điền"]
        for c in meta_cols: 
            if c not in old_df.columns: old_df[c] = ""

        if "Block ID" in old_df.columns: old_df["_cb"] = clean_str_series(old_df["Block ID"])
        else: old_df["_cb"] = ""
        if "Link ID Config" in old_df.columns: old_df["_cl"] = clean_str_series(old_df["Link ID Config"])
        else: old_df["_cl"] = ""

        tb, tl = clean_str(block_id), clean_str(link_id_config)
        mask = (old_df["_cb"] == tb) & (old_df["_cl"] == tl)
        safe_df = old_df[~mask].copy()
        target_df = old_df[mask].copy()
        safe_df = safe_df.drop(columns=["_cb", "_cl"])
        target_df = target_df.drop(columns=["_cb", "_cl"])

        if new_data:
            new_df = pd.DataFrame(new_data).astype(str)
            api_cols = [c for c in new_df.columns if c not in meta_cols]
            new_df["Link Nguồn"] = link_sheet_url
            new_df["Sheet Nguồn"] = sheet_name
            new_df["Block ID"] = tb
            new_df["Link ID Config"] = tl
            new_df["Thời gian điền"] = (datetime.utcnow() + timedelta(hours=7)).strftime("%H:%M:%S %d/%m/%Y")
            pk = api_cols[0] if api_cols else new_df.columns[0]
        else: new_df = pd.DataFrame(); pk = None

        res_df = pd.DataFrame()
        if status_mode == "Chưa chốt & đang cập nhật": res_df = new_df
        elif status_mode == "Cập nhật dữ liệu cũ":
            if target_df.empty or new_df.empty: res_df = target_df
            else:
                common = set(target_df[pk]).intersection(set(new_df[pk]))
                res_df = pd.concat([target_df[~target_df[pk].isin(common)], new_df[new_df[pk].isin(common)]], ignore_index=True)
        elif status_mode == "Cập nhật dữ liệu mới":
            if target_df.empty: res_df = new_df
            elif new_df.empty: res_df = target_df
            else: res_df = pd.concat([target_df, new_df[~new_df[pk].isin(set(target_df[pk]))]], ignore_index=True)
        else: res_df = target_df

        final_df = pd.concat([safe_df, res_df], ignore_index=True)
        final_df["_sort_id"] = pd.to_numeric(final_df["Link ID Config"], errors='coerce').fillna(999999)
        final_df = final_df.sort_values(by=["Block ID", "_sort_id"]).drop(columns=["_sort_id"])

        cols = list(final_df.columns)
        f_cols = [c for c in cols if c not in meta_cols] + meta_cols
        final_df = final_df[[c for c in f_cols if c in final_df.columns]]

        wks.clear(); set_with_dataframe(wks, final_df)
        final_df = final_df.reset_index(drop=True)
        clean_links = clean_str_series(final_df["Link ID Config"])
        clean_blocks = clean_str_series(final_df["Block ID"])
        match_idx = final_df.index[(clean_links == tl) & (clean_blocks == tb)].tolist()
        
        if match_idx: return f"{min(match_idx)+2} - {max(match_idx)+2}", "Success"
        return "No Data", "Success"
    except Exception as e: return "0", str(e)
