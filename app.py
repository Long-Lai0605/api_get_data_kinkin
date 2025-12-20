# ... (Phần trên giữ nguyên) ...

# ---------------------------------------------------------
# CẬP NHẬT PHẦN NÚT CHẠY ĐỂ HIỂN THỊ LOG CHI TIẾT
# ---------------------------------------------------------
if st.button("▶️ CHẠY KHỐI NÀY", type="primary"):
    if utils.check_lock("User"): st.error("Hệ thống đang bận!"); st.stop()
    utils.set_lock("User", True)
    
    # Tạo Container trạng thái
    status_box = st.status("🚀 Đang khởi động...", expanded=True)
    
    try:
        # Hàm callback để update giao diện từ Backend
        def ui_logger(msg):
            status_box.write(msg)
            time.sleep(0.05) # Delay xíu cho đẹp

        # Lấy data config mới nhất
        df_latest = get_as_dataframe(wks_config, dtype=str).dropna(how='all')
        rows_run = df_latest[(df_latest["Block_Name"] == selected_block) & 
                             (df_latest["Trạng thái"] == "Chưa chốt & đang cập nhật")]
        
        total_rows = 0; start = time.time()
        
        if rows_run.empty:
            status_box.update(label="⚠️ Không có dòng nào 'Chưa chốt' để chạy!", state="error")
            st.warning("Vui lòng kiểm tra lại trạng thái các dòng cấu hình.")
        else:
            for idx, row in rows_run.iterrows():
                api_url = row.get('API URL', 'Unknown URL')
                status_box.write(f"🔵 **Đang xử lý nguồn:** `{api_url}`")
                
                # Gọi Backend và truyền hàm ui_logger vào
                ok, msg, count = backend.process_sync(row, selected_block, callback=ui_logger)
                
                # Update kết quả
                if ok:
                    status_box.write(f"✅ **Xong nguồn này:** +{count} dòng.")
                    total_rows += count
                else:
                    status_box.write(f"❌ **Lỗi:** {msg}")
                
                # Lưu vào DB
                real_idx = df_latest.index[df_latest['API URL'] == api_url].tolist()[0]
                df_latest.at[real_idx, "Kết quả"] = msg
                df_latest.at[real_idx, "Dòng dữ liệu"] = count
            
            # Lưu config cuối cùng
            wks_config.clear()
            set_with_dataframe(wks_config, df_latest)
            
            elapsed = round(time.time() - start, 2)
            status_box.update(label="🎉 Hoàn tất quy trình!", state="complete", expanded=False)
            
            if total_rows > 0:
                st.success(f"📊 Tổng kết: Thêm mới {total_rows} dòng | Thời gian: {elapsed}s")
            else:
                st.warning(f"⚠️ Chạy xong nhưng không có dữ liệu nào được thêm. (Thời gian: {elapsed}s)")

    except Exception as e:
        st.error(f"🔥 Lỗi nghiêm trọng: {e}")
    finally:
        utils.set_lock("User", False)
