import requests
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from datetime import datetime
import streamlit as st
import time

# --- CẤU HÌNH TÊN TAB VÀ CỘT ---
SHEET_CONFIG = "luu_api_key"  # Tên tab cấu hình
SHEET_LOG = "sys_log"         # Tên tab lưu log hệ thống

# Tên các cột trong tab luu_api_key
COL_BLOCK = "Tên Khối"
COL_STATUS = "Trạng thái"
COL_METHOD = "Method"
COL_URL = "API URL"
COL_KEY = "API Key"           # Nơi lưu Token thật
COL_TARGET = "Tab Đích"       # Tên tab muốn đổ dữ liệu ra
COL_RESULT = "Kết quả"
COL_COUNT = "Số dòng"

def get_master_sheet(creds):
    """Mở file Sheet Master từ ID trong secrets"""
    gc = gspread.authorize(creds)
    sh_id = st.secrets["system"]["master_sheet_id"]
    return gc.open_by_key(sh_id)

def log_system(creds, msg):
    """Ghi log vào tab sys_log"""
    try:
        sh = get_master_sheet(creds)
        try: wks = sh.worksheet(SHEET_LOG)
        except: 
            wks = sh.add_worksheet(SHEET_LOG, 1000, 5)
            wks.append_row(["Thời gian", "Nội dung"])
        wks.append_row([datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg])
    except: pass

def fetch_api(method, url, token):
    """Gọi API 1Office có phân trang"""
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    all_data = []
    page = 1; limit = 100
    
    try:
        while True:
            params = {'page': page, 'limit': limit}
            if method.upper() == "POST":
                res = requests.post(url, headers=headers, json=params, timeout=30)
            else:
                res = requests.get(url, headers=headers, params=params, timeout=30)
            
            if res.status_code != 200: return None, f"HTTP {res.status_code}"
            
            data = res.json()
            # Lấy data từ key 'data' hoặc lấy trực tiếp nếu là list
            items = data.get('data', []) if isinstance(data, dict) else data
            
            if not items: break
            all_data.extend(items)
            if len(items) < limit: break
            page += 1
            
        return pd.DataFrame(all_data), "Thành công"
    except Exception as e: return None, f"Lỗi: {str(e)}"

def sync_data(creds, row_config):
    """Đồng bộ 1 dòng cấu hình: API -> Sheet"""
    try:
        # 1. Lấy thông tin (row_config chứa Token thật)
        token = str(row_config.get(COL_KEY, "")).strip()
        url = str(row_config.get(COL_URL, "")).strip()
        method = str(row_config.get(COL_METHOD, "GET")).strip()
        target_tab = str(row_config.get(COL_TARGET, "Data_Raw")).strip()
        
        if not token or len(token) < 5: return False, "Thiếu Token", 0
        if not url: return False, "Thiếu URL", 0

        # 2. Gọi API
        df, msg = fetch_api(method, url, token)
        if df is None: return False, msg, 0
        if df.empty: return True, "API trả về 0 dòng", 0

        # 3. Thêm cột hệ thống
        df['Link Nguồn'] = url
        df['Ngày cập nhật'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 4. Ghi vào Sheet Master
        sh = get_master_sheet(creds)
        try: wks = sh.worksheet(target_tab)
        except: wks = sh.add_worksheet(target_tab, 1000, 20)
        
        # Logic: Xóa dòng cũ của URL này -> Ghi mới
        old_df = get_as_dataframe(wks, evaluate_formulas=True, dtype=str)
        if 'Link Nguồn' in old_df.columns:
            old_df = old_df[old_df['Link Nguồn'] != url]
        
        final_df = pd.concat([old_df, df], ignore_index=True)
        wks.clear()
        set_with_dataframe(wks, final_df)
        
        return True, "Thành công", len(df) # Trả về số dòng mới lấy được
    except Exception as e:
        return False, f"Lỗi Ghi: {str(e)}", 0
