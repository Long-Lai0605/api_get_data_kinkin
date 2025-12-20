import streamlit as st
import gspread
from google.oauth2 import service_account
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas as pd
from datetime import datetime, timedelta

# --- TÊN SHEET HỆ THỐNG ---
SH_CONFIG = "luu_cau_hinh"      
SH_SECURE = "log_api_1office"   
SH_LOCK = "sys_lock"
SH_LOG = "log_lanthucthi"
SH_LOG_GH = "log_chay_auto_github"
SH_SCHED = "sys_config"

def get_creds():
    return service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], scopes=['https://www.googleapis.com/auth/spreadsheets']
    )

def get_master_sh():
    creds = get_creds()
    gc = gspread.authorize(creds)
    return gc.open_by_key(st.secrets["system"]["master_sheet_id"])

def init_db():
    """Khởi tạo database với Header chuẩn yêu cầu"""
    sh = get_master_sh()
    
    # 1. Sheet Cấu hình (Hiển thị)
    try: sh.worksheet(SH_CONFIG)
    except: 
        wks = sh.add_worksheet(SH_CONFIG, 100, 20)
        # --- CẬP NHẬT HEADER CHUẨN TẠI ĐÂY ---
        headers = [
            "Block_Name", "STT", "Trạng thái", "Ngày chốt", "Tháng", 
            "Method", "API URL", "Access Token", 
            "Link Đích", "Tên sheet dữ liệu dịch", "Kết quả", "Dòng dữ liệu"
        ]
        wks.append_row(headers)

    # 2. Sheet Bảo mật (Lưu Token thật)
    try: sh.worksheet(SH_SECURE)
    except:
        wks = sh.add_worksheet(SH_SECURE, 1000, 5)
        wks.append_row(["Block_Name", "API URL", "Real_Token", "Last_Updated"])

    # 3. Các sheet khác
    for name in [SH_LOCK, SH_LOG, SH_LOG_GH, SH_SCHED]:
        try: sh.worksheet(name)
        except: sh.add_worksheet(name, 100, 5)

# --- CƠ CHẾ BẢO MẬT TOKEN ---
def save_secure_token(block, url, token):
    if not token or token == "Đã lưu kho 🔒": return
    sh = get_master_sh()
    wks = sh.worksheet(SH_SECURE)
    df = get_as_dataframe(wks, dtype=str).dropna(how='all')
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    mask = (df['Block_Name'] == block) & (df['API URL'] == url)
    if mask.any():
        idx = df[mask].index[0]
        df.at[idx, 'Real_Token'] = token; df.at[idx, 'Last_Updated'] = now
    else:
        new_row = {"Block_Name": block, "API URL": url, "Real_Token": token, "Last_Updated": now}
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    set_with_dataframe(wks, df)

def get_real_token(block, url):
    try:
        sh = get_master_sh()
        wks = sh.worksheet(SH_SECURE)
        df = get_as_dataframe(wks, dtype=str)
        row = df[(df['Block_Name'] == block) & (df['API URL'] == url)]
        if not row.empty: return row.iloc[0]['Real_Token']
        return None
    except: return None

# --- UTILS KHÁC ---
def check_lock(user_id):
    sh = get_master_sh()
    wks = sh.worksheet(SH_LOCK)
    val = wks.acell('A2').value
    locker = wks.acell('B2').value
    time_str = wks.acell('C2').value
    if val == "TRUE":
        try:
            lock_time = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
            if datetime.now() - lock_time > timedelta(minutes=30): return False
        except: pass
        if locker != user_id: return True
    return False

def set_lock(user_id, status=True):
    sh = get_master_sh()
    wks = sh.worksheet(SH_LOCK)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    wks.update("A2:C2", [["TRUE" if status else "FALSE", user_id if status else "", now if status else ""]])

def write_log(msg, source="Manual"):
    sh = get_master_sh()
    target_sh = SH_LOG_GH if source == "GitHub" else SH_LOG
    wks = sh.worksheet(target_sh)
    wks.append_row([datetime.now().strftime("%Y-%m-%d %H:%M:%S"), source, msg])
