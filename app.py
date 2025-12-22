import streamlit as st
import backend as be
import pandas as pd
import time
import json
from datetime import time as dt_time

st.set_page_config(page_title="KINKIN ENGINE", layout="wide", page_icon="⚡")
st.markdown("""<style>.stButton>button { width: 100%; font-weight: bold; }</style>""", unsafe_allow_html=True)

with st.spinner("Kết nối Database..."):
    be.init_database(st.secrets)

st.title("⚡ 1OFFICE PARALLEL ENGINE")
tab1, tab2, tab3 = st.tabs(["🚀 Dashboard & Chạy", "⏰ Cài đặt Lịch chạy", "➕ Thêm Khối Mới"])

# --- TAB 1: DASHBOARD (EDITABLE) ---
with tab1:
    blocks = be.get_active_blocks(st.secrets)
    
    if not blocks:
        st.info("Chưa có cấu hình nào.")
    else:
        df = pd.DataFrame(blocks)
        
        # --- [BƯỚC 1] TÁCH DỮ LIỆU ẨN (TOKEN, URL) ---
        # Để tránh lộ Token trên bảng, ta tách ra và sẽ ghép lại khi bấm Lưu
        hidden_cols = ['Access Token (Encrypted)', 'Method', 'API URL']
        # Tạo từ điển map: { "Tên Block": {Token: ..., URL: ...} }
        hidden_map = {}
        if not df.empty and 'Block Name' in df.columns:
            # Lưu lại dữ liệu ẩn trước khi xóa khỏi df hiển thị
            for index, row in df.iterrows():
                b_name = row['Block Name']
                hidden_map[b_name] = {col: row.get(col, '') for col in hidden_cols}
        
        # Xóa cột ẩn khỏi bảng hiển thị
        df_display = df.drop(columns=[c for c in hidden_cols if c in df.columns], errors='ignore')

        # --- [FIX LỖI QUAN TRỌNG] CHUYỂN ĐỔI KIỂU DỮ LIỆU NGÀY ---
        # Chuyển string ("2025-11-01") thành datetime object để st.data_editor hiểu
        date_cols = ["Ngày bắt đầu", "Ngày kết thúc"]
        for col in date_cols:
            if col in df_display.columns:
                df_display[col] = pd.to_datetime(df_display[col], errors='coerce')

        # --- [BƯỚC 2] HIỂN THỊ BẢNG CHỈNH SỬA ---
        edited_df = st.data_editor(
            df_display,
            column_config={
                "Block Name": st.column_config.TextColumn("Tên Khối", disabled=True), # Khóa tên để không mất link với Token
                "Trạng thái": st.column_config.SelectboxColumn(
                    "Trạng thái",
                    options=["Chưa chốt & đang cập nhật", "Đã chốt"],
                    required=True,
                ),
                "Ngày bắt đầu": st.column_config.DateColumn("Ngày bắt đầu", format="DD/MM/YYYY"),
                "Ngày kết thúc": st.column_config.DateColumn("Ngày kết thúc", format="DD/MM/YYYY"),
                "Link Đích": st.column_config.LinkColumn("Link Sheet"),
            },
            use_container_width=True,
            hide_index=True,
            key="editor"
        )
        
        # --- [BƯỚC 3] NÚT LƯU CẤU HÌNH ---
        if st.button("💾 LƯU CẤU HÌNH (Link, Ngày, Trạng thái...)", type="primary"):
            with st.spinner("Đang lưu..."):
                try:
                    # 1. Chuyển ngày tháng từ object về string (YYYY-MM-DD) để lưu vào Sheet
                    df_to_save = edited_df.copy()
                    for col in date_cols:
                        if col in df_to_save.columns:
                            # Nếu là NaT (trống) thì để chuỗi rỗng, ngược lại format YYYY-MM-DD
                            df_to_save[col] = df_to_save[col].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else "")

                    # 2. Ghép lại cột Token/URL đã ẩn
                    # Duyệt qua từng dòng để lấy lại Token từ hidden_map
                    restored_rows = []
                    for index, row in df_to_save.iterrows():
                        r_data = row.to_dict()
                        b_name = r_data.get('Block Name')
                        # Lấy lại thông tin ẩn nếu có
                        if b_name in hidden_map:
                            r_data.update(hidden_map[b_name])
                        restored_rows.append(r_data)
                    
                    final_df = pd.DataFrame(restored_rows)

                    # 3. Gọi hàm lưu
                    status, msg = be.save_configurations(st.secrets, final_df)
                    
                    if status:
                        st.success(msg)
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error(msg)
                except Exception as e:
                    st.error(f"Lỗi xử lý dữ liệu: {e}")
        
        st.divider()

        # --- NÚT CHẠY ---
        # Lọc danh sách cần chạy từ edited_df (dữ liệu đang hiển thị)
        blocks_to_run = edited_df[edited_df["Trạng thái"] == "Chưa chốt & đang cập nhật"]
        count_run = len(blocks_to_run)
        
        btn_label = f"▶️ CHẠY {count_run} BLOCK (Đang cập nhật)" if count_run > 0 else "▶️ KHÔNG CÓ BLOCK CẦN CHẠY"
        
        if st.button(btn_label, type="primary", disabled=(count_run == 0)):
            # Lấy lại full info (để có token mới nhất)
            full_blocks = be.get_active_blocks(st.secrets)
            df_full = pd.DataFrame(full_blocks)
            
            # Chỉ lấy những block có tên nằm trong danh sách cần chạy
            targets = df_full[df_full["Block Name"].isin(blocks_to_run["Block Name"])]
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            for i, (_, block) in enumerate(targets.iterrows()):
                b_name = block['Block Name']
                status_text.write(f"⏳ Đang chạy: **{b_name}**...")
                progress_bar.progress((i) / count_run)
                
                with st.status(f"🔄 **{b_name}**", expanded=True) as status:
                    def update_text(t): status.write(t)
                    
                    # Convert ngày
                    d_s_str = str(block.get("Ngày bắt đầu", ""))
                    d_e_str = str(block.get("Ngày kết thúc", ""))
                    d_s_obj = pd.to_datetime(d_s_str, dayfirst=False).date() if d_s_str and d_s_str != 'NaT' and d_s_str != '' else None
                    d_e_obj = pd.to_datetime(d_e_str, dayfirst=False).date() if d_e_str and d_e_str != 'NaT' and d_e_str != '' else None
                    
                    data, msg = be.fetch_1office_data_smart(
                        url=block.get('API URL', ''), 
                        token=block.get('Access Token (Encrypted)', ''), 
                        method=block.get('Method', 'GET'), 
                        filter_key=block.get("Filter Key", ""),
                        date_start=d_s_obj,
                        date_end=d_e_obj,
                        status_callback=update_text
                    )

                    if msg.startswith("Success") and data is not None:
                        status.write(f"✅ Tải {len(data)} dòng. Đang ghi Sheet...")
                        range_str, w_msg = be.write_to_sheet_range(st.secrets, block, data)
                        if "Error" not in w_msg:
                            status.update(label=f"✅ {b_name}: Xong! ({range_str})", state="complete", expanded=False)
                        else:
                            status.update(label=f"❌ {b_name}: Lỗi ghi", state="error")
                            st.error(w_msg)
                    else:
                        status.update(label=f"⚠️ {b_name}: {msg}", state="error")
            
            progress_bar.progress(100)
            status_text.write("🎉 Đã hoàn tất toàn bộ quy trình!")
            st.toast("Đã chạy xong!")
            time.sleep(2)
            st.rerun()

# --- TAB 2: HẸN GIỜ (SCHEDULER UI) ---
with tab2:
    st.header("⏰ Cấu hình Lịch chạy Tự động")
    st.info("Cài đặt này sẽ được Bot sử dụng để biết khi nào cần kích hoạt.")
    
    freq = st.radio("Tần suất lặp lại", ["Hàng ngày", "Hàng tuần", "Hàng tháng"], horizontal=True)
    
    schedule_data = {}
    
    if freq == "Hàng ngày":
        mode = st.selectbox("Chế độ", ["Cố định 1 lần/ngày", "Lấy liên tục (Loop)"])
        if mode == "Cố định 1 lần/ngày":
            t = st.time_input("Chọn giờ chạy", dt_time(8, 0))
            schedule_data = {"type": "daily_fixed", "time": str(t)}
        else:
            m = st.number_input("Chạy lại sau mỗi (phút)", min_value=5, value=60)
            schedule_data = {"type": "daily_loop", "interval_minutes": m}
            
    elif freq == "Hàng tuần":
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Lần 1**")
            d1 = st.selectbox("Thứ", ["Thứ 2", "Thứ 3", "Thứ 4", "Thứ 5", "Thứ 6", "Thứ 7", "Chủ Nhật"], key="w1")
            t1 = st.time_input("Giờ", dt_time(8, 0), key="t1")
        with c2:
            st.markdown("**Lần 2 (Tùy chọn)**")
            en2 = st.checkbox("Kích hoạt lần 2")
            if en2:
                d2 = st.selectbox("Thứ", ["Thứ 2", "Thứ 3", "Thứ 4", "Thứ 5", "Thứ 6", "Thứ 7", "Chủ Nhật"], key="w2")
                t2 = st.time_input("Giờ", dt_time(17, 0), key="t2")
                schedule_data = {"type": "weekly", "run1": {"day": d1, "time": str(t1)}, "run2": {"day": d2, "time": str(t2)}}
            else:
                schedule_data = {"type": "weekly", "run1": {"day": d1, "time": str(t1)}}

    elif freq == "Hàng tháng":
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Lần 1**")
            d1 = st.number_input("Ngày (1-31)", 1, 31, 1, key="m1")
            t1 = st.time_input("Giờ", dt_time(8, 0), key="mt1")
        with c2:
            st.markdown("**Lần 2 (Tùy chọn)**")
            en2 = st.checkbox("Kích hoạt lần 2", key="men2")
            if en2:
                d2 = st.number_input("Ngày (1-31)", 1, 31, 15, key="m2")
                t2 = st.time_input("Giờ", dt_time(8, 0), key="mt2")
                schedule_data = {"type": "monthly", "run1": {"day": d1, "time": str(t1)}, "run2": {"day": d2, "time": str(t2)}}
            else:
                schedule_data = {"type": "monthly", "run1": {"day": d1, "time": str(t1)}}

    # Nút lưu giống hệt Tab 1
    if st.button("💾 LƯU LỊCH CHẠY", type="primary"):
        ok = be.save_schedule_settings(st.secrets, freq, schedule_data)
        if ok: 
            st.success("✅ Đã lưu cấu hình lịch chạy thành công!")
            time.sleep(1)
            st.rerun()
        else: st.error("Lỗi khi lưu lịch chạy.")

# --- TAB 3: THÊM MỚI (GIỮ NGUYÊN) ---
with tab3:
    st.markdown("### Cấu hình Khối mới")
    with st.form("add_form", clear_on_submit=True):
        c1, c2 = st.columns(2)
        name = c1.text_input("Tên Khối (Block Name) *")
        method = c2.selectbox("Method", ["GET", "POST"])
        url = st.text_input("API URL *")
        token = st.text_input("Token *", type="password")
        c3, c4 = st.columns(2)
        link = c3.text_input("Link Sheet Đích *")
        sheet = c4.text_input("Tên Sheet Đích *")
        st.divider()
        filter_key = st.text_input("Key Lọc (VD: end_plan)", value="end_plan")
        col_d1, col_d2 = st.columns(2)
        start = col_d1.date_input("Ngày bắt đầu")
        end = col_d2.date_input("Ngày kết thúc")
        
        if st.form_submit_button("Lưu & Cập nhật Dashboard"):
            if not name or not url or not token: st.error("Thiếu thông tin!")
            else:
                ok = be.add_new_block(st.secrets, name, method, url, token, link, sheet, start, end, filter_key)
                if ok: 
                    st.toast("✅ Đã thêm!")
                    time.sleep(1)
                    st.rerun()
