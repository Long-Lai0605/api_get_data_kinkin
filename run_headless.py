import pandas as pd
import utils
import backend
from gspread_dataframe import set_with_dataframe, get_as_dataframe

print("🚀 GITHUB AUTO SYNC STARTED...")
try:
    utils.init_db()
    if utils.check_lock("GitHub"):
        print("🔒 System Locked. Exiting.")
        exit()
    utils.set_lock("GitHub", True)

    sh = utils.get_master_sh()
    wks_config = sh.worksheet(utils.SH_CONFIG)
    df_config = get_as_dataframe(wks_config, dtype=str).dropna(how='all')
    
    job_count = 0
    
    # Quét tất cả dòng chưa chốt
    for idx, row in df_config.iterrows():
        if row.get("Trạng thái") == "Chưa chốt & đang cập nhật":
            print(f"Running: {row.get('Block_Name')} -> {row.get('API URL')}")
            # Backend tự lấy token thật từ kho
            ok, msg, count = backend.process_sync(row, row.get('Block_Name'))
            
            # Cập nhật trạng thái
            df_config.at[idx, "Kết quả"] = msg
            df_config.at[idx, "Dòng dữ liệu"] = count
            if ok: job_count += 1

    wks_config.clear()
    set_with_dataframe(wks_config, df_config)
    
    utils.write_log(f"GitHub Auto Run: {job_count} jobs completed.", "GitHub")
    print("✅ DONE.")

except Exception as e:
    print(f"❌ ERROR: {e}")
    utils.write_log(f"GitHub Error: {e}", "GitHub")
finally:
    utils.set_lock("GitHub", False)
