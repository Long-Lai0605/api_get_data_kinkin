import streamlit as st
import gspread
from google.oauth2 import service_account
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas as pd
from datetime import datetime

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
    sh = get_master_sh()
    try: sh.worksheet(SH_CONFIG)
    except: 
        wks = sh.add_worksheet(SH_CONFIG, 100, 20)
        headers = ["Block_Name", "STT", "Trạng thái", "Ngày bắt đầu", "Ngày kết thúc", "Method", "API URL", "Access Token", "Link Đích", "Tên sheet dữ liệu dịch", "Kết quả", "Dòng dữ liệu"]
        wks.append_row(headers)
    try: sh.worksheet(SH_SECURE)
    except:
        wks = sh.add_worksheet(SH_SECURE, 1000, 5)
        wks.append_row(["Block_Name", "API URL", "Real_Token", "Last_Updated"])
    for name in [SH_LOCK, SH_LOG, SH_LOG_GH, SH_SCHED]:
        try: sh.worksheet(name)
        except: sh.add_worksheet(name, 100, 5)

# ------------------------------------------------------------------
# [FIX] HÀM LƯU & LẤY TOKEN (LOGIC CHUẨN: BLOCK + URL + CLEANING)
# ------------------------------------------------------------------

def save_secure_token(block, url, token):
    if not token or token == "Đã lưu kho 🔒": return
    
    # 1. Làm sạch dữ liệu đầu vào
    block = str(block).strip()
    url = str(url).strip()
    token = str(token).strip()
    
    # Nếu user lỡ nhập "Bearer xyz...", cắt bỏ chữ Bearer đi
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    
    sh = get_master_sh()
    wks = sh.worksheet(SH_SECURE)
    df = get_as_dataframe(wks, dtype=str).dropna(how='all')
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 2. Tìm chính xác theo Block + URL để update
    if 'API URL' in df.columns and 'Block_Name' in df.columns:
        # So sánh chính xác (sau khi strip)
        mask = (df['Block_Name'].str.strip() == block) & (df['API URL'].str.strip() == url)
        
        if mask.any():
            # Nếu có nhiều dòng trùng, update dòng cuối cùng (mới nhất)
            idx = df[mask].index[-1] 
            df.at[idx, 'Real_Token'] = token
            df.at[idx, 'Last_Updated'] = now
        else:
            new_row = {"Block_Name": block, "API URL": url, "Real_Token": token, "Last_Updated": now}
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    else:
        new_row = {"Block_Name": block, "API URL": url, "Real_Token": token, "Last_Updated": now}
        df = pd.DataFrame([new_row])

    set_with_dataframe(wks, df)


def get_real_token(block, url):
    """
    Lấy Token theo đúng chuẩn: Block Name + URL
    + Thêm bước 'Làm sạch' để chống lỗi token_not_valid
    + Luôn lấy dòng mới nhất (Last Write Wins)
    """
    try:
        block = str(block).strip()
        url = str(url).strip()
        
        sh = get_master_sh()
        wks = sh.worksheet(SH_SECURE)
        df = get_as_dataframe(wks, dtype=str)
        
        if 'API URL' not in df.columns or 'Block_Name' not in df.columns:
            return None

        # 1. Lọc đúng chuẩn Block + URL
        mask = (df['Block_Name'].str.strip() == block) & (df['API URL'].str.strip() == url)
        rows = df[mask]
        
        if not rows.empty:
            # Lấy dòng mới nhất (dòng cuối cùng tìm thấy)
            raw_token = rows.iloc[-1]['Real_Token']
            
            if raw_token and str(raw_token).lower() != 'nan':
                # 2. LÀM SẠCH TOKEN KHI LẤY RA (Chốt chặn cuối cùng)
                clean_token = str(raw_token).strip()
                # Loại bỏ Bearer nếu có
                if clean_token.lower().startswith("bearer "):
                    clean_token = clean_token[7:].strip()
                
                return clean_token
                
        print(f"⚠️ Không tìm thấy Token cho Block: {block} - URL: {url}")
        return None
    except Exception as e:
        print(f"❌ Lỗi utils: {e}")
        return None

# --- GIỮ NGUYÊN CÁC HÀM KHÁC ---
def check_lock(user_id):
    try:
        sh = get_master_sh(); wks = sh.worksheet(SH_LOCK)
        val = wks.acell('A2').value; locker = wks.acell('B2').value
        if val == "TRUE" and locker != user_id: return True
        return False
    except: return False

def set_lock(user_id, status=True):
    try:
        sh = get_master_sh(); wks = sh.worksheet(SH_LOCK)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        wks.update("A2:C2", [["TRUE" if status else "FALSE", user_id if status else "", now if status else ""]])
    except: pass

def write_log(msg, source="Manual"):
    try:
        sh = get_master_sh()
        target = SH_LOG_GH if source == "GitHub" else SH_LOG
        wks = sh.worksheet(target)
        wks.append_row([datetime.now().strftime("%Y-%m-%d %H:%M:%S"), source, msg])
    except: pass
