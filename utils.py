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
    sh = get_master_sh()
    # (Giữ nguyên phần khởi tạo sheet như cũ)
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
# [FIX] HÀM LƯU & LẤY TOKEN (CỐT LÕI)
# ------------------------------------------------------------------

def save_secure_token(block, url, token):
    """Lưu Token chính xác vào kho"""
    if not token or token == "Đã lưu kho 🔒": return
    
    # Chuẩn hóa đầu vào để tránh lỗi do khoảng trắng
    url = url.strip()
    token = token.strip()
    
    sh = get_master_sh()
    wks = sh.worksheet(SH_SECURE)
    df = get_as_dataframe(wks, dtype=str).dropna(how='all')
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Tìm xem URL này đã tồn tại chưa (Bất kể Block nào)
    # Vì 1 URL chỉ nên có 1 Token duy nhất
    if 'API URL' in df.columns:
        mask = df['API URL'] == url
        if mask.any():
            idx = df[mask].index[0]
            df.at[idx, 'Real_Token'] = token
            df.at[idx, 'Last_Updated'] = now
            # Cập nhật luôn Block Name mới nhất nếu có đổi
            df.at[idx, 'Block_Name'] = block 
        else:
            new_row = {"Block_Name": block, "API URL": url, "Real_Token": token, "Last_Updated": now}
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    else:
        # Trường hợp sheet rỗng chưa có header
        new_row = {"Block_Name": block, "API URL": url, "Real_Token": token, "Last_Updated": now}
        df = pd.DataFrame([new_row])

    set_with_dataframe(wks, df)


def get_real_token(block, url):
    """
    [FIXED] Lấy Token thông minh hơn:
    1. Chỉ so khớp URL (Chuẩn nhất).
    2. Loại bỏ khoảng trắng thừa.
    """
    try:
        url = str(url).strip()
        sh = get_master_sh()
        wks = sh.worksheet(SH_SECURE)
        df = get_as_dataframe(wks, dtype=str)
        
        # Đảm bảo cột tồn tại
        if 'API URL' not in df.columns or 'Real_Token' not in df.columns:
            return None

        # Tìm dòng có URL khớp (Bỏ qua Block Name để tránh lỗi lệch tên)
        # Sử dụng str.strip() để so sánh chính xác tuyệt đối
        row = df[df['API URL'].str.strip() == url]
        
        if not row.empty:
            token = row.iloc[0]['Real_Token']
            # Kiểm tra token rỗng
            if token and str(token).lower() != 'nan':
                return str(token).strip()
                
        return None
    except Exception as e:
        print(f"Lỗi lấy Token: {e}") # In ra log để debug
        return None

# --- CÁC HÀM LOCK/LOG GIỮ NGUYÊN ---
def check_lock(user_id):
    # (Giữ nguyên code cũ)
    try:
        sh = get_master_sh(); wks = sh.worksheet(SH_LOCK)
        val = wks.acell('A2').value; locker = wks.acell('B2').value
        if val == "TRUE" and locker != user_id: return True
        return False
    except: return False

def set_lock(user_id, status=True):
    # (Giữ nguyên code cũ)
    try:
        sh = get_master_sh(); wks = sh.worksheet(SH_LOCK)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        wks.update("A2:C2", [["TRUE" if status else "FALSE", user_id if status else "", now if status else ""]])
    except: pass

def write_log(msg, source="Manual"):
    # (Giữ nguyên code cũ)
    try:
        sh = get_master_sh()
        target = SH_LOG_GH if source == "GitHub" else SH_LOG
        wks = sh.worksheet(target)
        wks.append_row([datetime.now().strftime("%Y-%m-%d %H:%M:%S"), source, msg])
    except: pass
