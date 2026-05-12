import datetime as dt

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from dashboard_core.metrics import (
    calculate_daily_leaderboard,
    calculate_metrics,
    calculate_ranking_scores,
    calculate_rolling_timeseries,
    get_7d_ranking_history,
    get_detailed_ranking_history,
    get_monthly_pct_change,
    get_period_performance_ranking,
)

from .common import bind_context


def render_compare(tab_compare, ctx):
    bound = bind_context(ctx)
    all_data = bound['all_data']
    ana_end_dt = bound['ana_end_dt']
    ana_start_dt = bound['ana_start_dt']
    benchmark_series_full = bound['benchmark_series_full']
    benchmark_series_sliced = bound['benchmark_series_sliced']
    cutoff_days = bound['cutoff_days']
    date_option = bound['date_option']
    df_all_instruments = bound['df_all_instruments']
    df_all_instruments_full = bound['df_all_instruments_full']
    df_bond = bound['df_bond']
    df_bond_full = bound['df_bond_full']
    df_equity = bound['df_equity']
    df_equity_full = bound['df_equity_full']
    df_gov_bonds_price = bound['df_gov_bonds_price']
    df_gov_bonds_price_full = bound['df_gov_bonds_price_full']
    df_gov_bonds_yield = bound['df_gov_bonds_yield']
    df_gov_bonds_yield_full = bound['df_gov_bonds_yield_full']
    df_index = bound['df_index']
    df_index_full = bound['df_index_full']
    df_index_vol = bound['df_index_vol']
    df_index_vol_full = bound['df_index_vol_full']
    df_komoditas = bound['df_komoditas']
    df_komoditas_full = bound['df_komoditas_full']
    df_mata_uang = bound['df_mata_uang']
    df_mata_uang_full = bound['df_mata_uang_full']
    df_suku_bunga = bound['df_suku_bunga']
    df_suku_bunga_full = bound['df_suku_bunga_full']
    end_date_str = bound['end_date_str']
    full_dfs_dict = bound['full_dfs_dict']
    get_benchmark_series = bound['get_benchmark_series']
    latest_dates = bound['latest_dates']
    latest_update = bound['latest_update']
    metrics_all = bound['metrics_all']
    metrics_bond = bound['metrics_bond']
    metrics_equity = bound['metrics_equity']
    ranked_products_all = bound['ranked_products_all']
    ranked_products_bond = bound['ranked_products_bond']
    ranked_products_equity = bound['ranked_products_equity']
    risk_free_rate = bound['risk_free_rate']
    safe_slice = bound['safe_slice']
    scoring_mode = bound['scoring_mode']
    selected_bench_label = bound['selected_bench_label']
    selected_benchmark_label = bound['selected_benchmark_label']
    selected_benchmark_ticker = bound['selected_benchmark_ticker']
    start_date_str = bound['start_date_str']
    weights_dict = bound['weights_dict']
    young_all = bound['young_all']
    young_bonds = bound['young_bonds']
    young_equities = bound['young_equities']
    # ==================== TAB 5: PERBANDINGAN HISTORIS ====================
    with tab_compare:
        st.header("Perbandingan Historis & Analisis Volatilitas")
        st.info(f"""**Panduan Analisis Grafik ({date_option} | {start_date_str} s/d {end_date_str}):**
        - **Kinerja Absolut & Relatif:** Melacak tren Harga (NAV) aktual, akumulasi keuntungan (Return Kumulatif), dan risiko penurunan terdalam dari titik puncak (Drawdown).
        - **Volatility Bands (Standard Deviation Bands):** Memvisualisasikan area kewajaran harga. Harga yang menyentuh pita atas (+2 atau +3 SD) mengindikasikan area jenuh beli (*Overbought*/Mahal), sedangkan sentuhan di pita bawah (-2 atau -3 SD) menunjukkan jenuh jual (*Oversold*/Murah).
        - **Pergerakan Metrik Harian (Rolling):** Memantau tren perubahan metrik **Alpha, Beta (terhadap {selected_benchmark_label}), Sharpe Ratio, dan Volatilitas** secara dinamis dari hari ke hari, berguna untuk melihat apakah kinerja manajer investasi konsisten atau hanya kebetulan di satu waktu.""")

        # --- Filter Grup Produk & Manajer Investasi (MI) ---
        col_hist_f1, col_hist_f2 = st.columns([1, 2])
        with col_hist_f1:
            grup_produk_hist = st.selectbox("Pilih Grup Produk:", options=["Semua", "Equity", "Fixed Income"], key="hist_grup_produk")

        if grup_produk_hist == "Equity":
            all_instruments_list = df_equity.columns.tolist()
        elif grup_produk_hist == "Fixed Income":
            all_instruments_list = df_bond.columns.tolist()
        else:
            all_instruments_list = df_all_instruments.columns.tolist()

        mi_set = set()
        for name in all_instruments_list:
            if name.startswith("BNP Paribas"): mi_set.add("BNP Paribas")
            elif name.startswith("Eastspring"): mi_set.add("Eastspring")
            elif name.startswith("TRIM") or name.startswith("Trimegah"): mi_set.add("Trimegah")
            else: mi_set.add(name.split()[0])

        mi_list_filter = sorted(list(mi_set))

        with col_hist_f2:
            selected_mi_filters = st.multiselect(
                "🔍 Filter Berdasarkan Manajer Investasi (Kosongkan untuk tampilkan semua MI):",
                options=mi_list_filter,
                key="hist_mi_filter"
            )

        if selected_mi_filters:
            def match_mi(c, selected_filters):
                for mi in selected_filters:
                    if mi == "Trimegah" and (c.startswith("Trimegah") or c.startswith("TRIM")): return True
                    elif c.startswith(mi): return True
                return False

            available_instruments = [c for c in all_instruments_list if match_mi(c, selected_mi_filters)]
            default_selection = available_instruments # Semua produk dari MI tersebut langsung terpilih
        else:
            available_instruments = all_instruments_list
            default_selection = available_instruments[:min(2, len(available_instruments))] if available_instruments else []

        # Kunci dinamis agar Streamlit me-reset widget dan mengambil default baru setiap kali filter berubah
        dynamic_key = f"compare_multiselect_{hash(tuple(selected_mi_filters))}"
        selected_instruments = st.multiselect(
            "📈 Pilih Instrumen untuk Dibandingkan",
            options=available_instruments,
            default=default_selection,
            key=dynamic_key
        )

        # Syarat diubah menjadi minimal 1 instrumen agar analisis volatilitas tunggal dapat dilakukan
        if len(selected_instruments) >= 1:
            df_compare = df_all_instruments[selected_instruments].copy()
            df_compare = df_compare.ffill()

            legend_layout = dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5, title=None)

            # --- 1. Kinerja Absolut & Relatif ---
            st.subheader("Kinerja Absolut & Relatif")

            fig_prices = px.line(df_compare, x=df_compare.index, y=df_compare.columns, title="Harga Historis Aktual (NAV)")
            fig_prices.update_layout(xaxis_title="Tanggal", yaxis_title="Harga", legend=legend_layout, height=900)
            st.plotly_chart(fig_prices, use_container_width=True)

            # Kalkulasi Return Kumulatif
            df_returns_pct = pd.DataFrame(index=df_compare.index)

            for col in df_compare.columns:
                # Cari tanggal pertama di mana produk ini punya harga
                first_valid_idx = df_compare[col].first_valid_index()
                if first_valid_idx is not None:
                    base_price = df_compare.loc[first_valid_idx, col]
                    # Hitung persentase kenaikan dari titik rilis tersebut
                    df_returns_pct[col] = ((df_compare[col] / base_price) - 1) * 100

            fig_returns = px.line(df_returns_pct, x=df_returns_pct.index, y=df_returns_pct.columns, title="Return Kumulatif (%)")
            fig_returns.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)

            # --- Ekstrak warna otomatis dari Plotly Express ---
            line_colors = {}
            for trace in fig_returns.data:
                line_colors[trace.name] = trace.line.color

            # --- Tambahkan anotasi angka (persentase) berlatar warna ---
            if not df_returns_pct.empty:
                last_date = df_returns_pct.index[-1]
                for col in df_returns_pct.columns:
                    last_val = df_returns_pct[col].iloc[-1]

                    # Ambil warna garis, gunakan abu-abu sebagai cadangan jika tidak ditemukan
                    bg_color = line_colors.get(col, "gray")

                    fig_returns.add_annotation(
                        x=last_date,
                        y=last_val,
                        text=f"<b>{last_val:.2f}%</b>",
                        showarrow=False,
                        xanchor="left",
                        xshift=8,
                        font=dict(size=11, color="white"), # Ubah font menjadi putih agar terbaca
                        bgcolor=bg_color,                  # Latar belakang mengikuti warna garis
                        borderpad=3,                       # Jarak antara teks dan tepi kotak warna
                        opacity=0.9                        # Sedikit transparansi agar elegan
                    )

            fig_returns.update_layout(
                xaxis_title="Tanggal",
                yaxis_title="Return (%)",
                legend=legend_layout,
                margin=dict(r=70), # Margin diperlebar sedikit lagi untuk ruang kotak warna
                height=900
            )
            st.plotly_chart(fig_returns, use_container_width=True)

            running_max = df_compare.expanding().max()
            drawdown = (df_compare - running_max) / running_max * 100
            fig_dd = px.line(drawdown, x=drawdown.index, y=drawdown.columns, title="Drawdown dari Nilai Tertinggi (%)")
            fig_dd.update_layout(xaxis_title="Tanggal", yaxis_title="Drawdown (%)", yaxis_tickformat='.1f', legend=legend_layout, height=900)
            st.plotly_chart(fig_dd, use_container_width=True)

            st.divider()

           # ==========================================================
            # 2. Analisis Volatilitas Dinamis (Volatility Bands - Terpisah)
            # ==========================================================
            st.divider()
            st.subheader("Volatility Bands NAV (Standard Deviation)")
            st.caption("Pita volatilitas ini diatur secara independen untuk visualisasi titik ekstrem, tidak memengaruhi skor komposit.")

            total_days_full = len(df_all_instruments_full)

            col_vb1, col_vb2 = st.columns(2)
            with col_vb1:
                band_target_window = st.number_input(
                    "Interval Rolling Grafik (Hari Bursa):",
                    min_value=5, max_value=1260, value=252, step=1, key="vol_band_period"
                )
                # Fitur pemilihan SD Dinamis (Tabel Pengaturan)
                st.markdown("<p style='margin-bottom: 5px;'>Level Standard Deviation:</p>", unsafe_allow_html=True)
                selected_sd = []
                for i, default_val in enumerate([1.0, 2.0, 3.0]):
                    cc1, cc2 = st.columns([1, 2])
                    with cc1:
                        st.markdown(f"<p style='margin-top: 8px;'>Garis Risk {i+1}</p>", unsafe_allow_html=True)
                    with cc2:
                        val = st.number_input(f"SD {i+1}", value=default_val, step=0.1, label_visibility="collapsed", key=f"sd_{i}")
                        if val > 0:
                            selected_sd.append(val)
                # Hilangkan duplikat dan urutkan
                selected_sd = sorted(list(set(selected_sd)))
            with col_vb2:
                chart_theme = st.radio("Tema Visual Grafik:", ["Dark Theme", "Light Theme"], horizontal=True, key="band_theme_radio")

            if band_target_window >= total_days_full - 5:
                band_dynamic_window = max(22, total_days_full // 3)
                st.warning(f"Data historis terbatas. Pita disesuaikan ke {band_dynamic_window} hari.")
            else:
                band_dynamic_window = band_target_window

            for inst in selected_instruments:
                inst_nav_full = df_all_instruments_full[inst].ffill().bfill()
                roll_mean_full = inst_nav_full.rolling(window=band_dynamic_window).mean()
                roll_std_full = inst_nav_full.rolling(window=band_dynamic_window).std()

                inst_nav = safe_slice(inst_nav_full, ana_start_dt, ana_end_dt)
                roll_mean = safe_slice(roll_mean_full, ana_start_dt, ana_end_dt)

                fig_band = go.Figure()

                # Pengaturan Warna Tema
                if chart_theme == "Dark Theme":
                    nav_color, mean_color = 'white', 'cyan'
                    template_style = "plotly_dark"
                    # Palet warna untuk 6 level SD agar kontras
                    sd_colors = {
                        1: 'rgba(0, 255, 127, 0.8)',  # Spring Green
                        2: 'rgba(255, 215, 0, 0.8)',  # Gold
                        3: 'rgba(255, 69, 0, 0.8)',   # Red Orange
                        4: 'rgba(173, 216, 230, 0.8)', # Light Blue
                        5: 'rgba(238, 130, 238, 0.8)', # Violet
                        6: 'rgba(211, 211, 211, 0.6)'  # Light Grey
                    }
                else:
                    nav_color, mean_color = 'black', 'blue'
                    template_style = "plotly_white"
                    sd_colors = {
                        1: 'rgba(44, 160, 44, 0.6)',
                        2: 'rgba(255, 127, 14, 0.6)',
                        3: 'rgba(214, 39, 40, 0.6)',
                        4: 'rgba(31, 119, 180, 0.6)',
                        5: 'rgba(148, 103, 189, 0.6)',
                        6: 'rgba(127, 127, 127, 0.6)'
                    }

                fig_band.add_trace(go.Scatter(x=inst_nav.index, y=inst_nav, mode='lines', name='NAV Aktual', line=dict(color=nav_color, width=2.5)))
                fig_band.add_trace(go.Scatter(x=roll_mean.index, y=roll_mean, mode='lines', name=f'Mean ({band_dynamic_window}d)', line=dict(color=mean_color, width=1.5, dash='dot')))

                # Looping untuk merender SD yang dipilih saja
                for sd in sorted(selected_sd):
                    u_full = roll_mean_full + (sd * roll_std_full)
                    l_full = roll_mean_full - (sd * roll_std_full)

                    upper = safe_slice(u_full, ana_start_dt, ana_end_dt)
                    lower = safe_slice(l_full, ana_start_dt, ana_end_dt)

                    # Gunakan round() untuk pendekatan integer terdekat ke warna yang tersedia
                    color = sd_colors.get(round(sd), 'gray')

                    fig_band.add_trace(go.Scatter(x=upper.index, y=upper, mode='lines', name=f'+{sd} SD', line=dict(color=color, width=1, dash='dash')))
                    fig_band.add_trace(go.Scatter(x=lower.index, y=lower, mode='lines', name=f'-{sd} SD', line=dict(color=color, width=1, dash='dash')))

                fig_band.update_layout(
                    title=f"Distribusi Harga & Volatility Bands: {inst}",
                    xaxis_title="Tanggal", yaxis_title="NAV / Harga",
                    legend=legend_layout, hovermode="x unified",
                    template=template_style, height=900
                )
                st.plotly_chart(fig_band, use_container_width=True, theme=None)

            st.divider()

            # --- 3. Pergerakan Metrik Harian ---
            st.subheader(f"Grafik Pergerakan Metrik Harian (Rolling {date_option})")

            # Definisikan ulang dynamic_window murni berdasarkan rentang di sidebar
            if date_option == "1 Bulan": target_window = 22
            elif date_option == "3 Bulan": target_window = 63
            elif date_option == "6 Bulan": target_window = 126
            elif date_option == "1 Tahun": target_window = 252
            else: target_window = 252

            if target_window >= len(df_all_instruments_full) - 5:
                dynamic_window = max(22, len(df_all_instruments_full) // 3)
            else:
                dynamic_window = target_window

            # Kalkulasi metrik rolling menggunakan dynamic_window yang sudah dikalibrasi
            df_selected_full = df_all_instruments_full[selected_instruments]
            dynamic_ts = calculate_rolling_timeseries(df_selected_full, benchmark_series_full, risk_free_rate, window=dynamic_window, bench_ticker=selected_benchmark_ticker)
            sliced_ts_dict = {k: safe_slice(v, ana_start_dt, ana_end_dt) for k, v in dynamic_ts.items()}

            ts_data = {}
            for metric_name, ts_df in sliced_ts_dict.items():
                available_cols = [col for col in selected_instruments if col in ts_df.columns]
                if available_cols:
                    ts_data[metric_name] = ts_df[available_cols]

            if ts_data:
                if 'Alpha' in ts_data and not ts_data['Alpha'].empty:
                    fig_alpha = px.line(ts_data['Alpha'], title=f"Pergerakan Alpha dengan Benchmark {selected_bench_label} ({dynamic_window} Hari)")
                    fig_alpha.update_layout(xaxis_title="Tanggal", yaxis_title="Alpha", legend=legend_layout, height=800)
                    st.plotly_chart(fig_alpha, use_container_width=True)

                if 'Beta' in ts_data and not ts_data['Beta'].empty:
                    fig_beta = px.line(ts_data['Beta'], title=f"Pergerakan Beta dengan Benchmark {selected_bench_label} ({dynamic_window} Hari)")
                    fig_beta.update_layout(xaxis_title="Tanggal", yaxis_title="Beta", legend=legend_layout, height=800)
                    st.plotly_chart(fig_beta, use_container_width=True)

                if 'Sharpe_Ratio' in ts_data and not ts_data['Sharpe_Ratio'].empty:
                    fig_sharpe = px.line(ts_data['Sharpe_Ratio'], title=f"Pergerakan Sharpe Ratio dengan Benchmark {selected_bench_label} ({dynamic_window} Hari)")
                    fig_sharpe.update_layout(xaxis_title="Tanggal", yaxis_title="Sharpe Ratio", legend=legend_layout, height=800)
                    st.plotly_chart(fig_sharpe, use_container_width=True)

                if 'Volatility' in ts_data and not ts_data['Volatility'].empty:
                    fig_vol = px.line(ts_data['Volatility'], title=f"Pergerakan Risk (Std Dev, {dynamic_window} Hari)")
                    fig_vol.update_layout(xaxis_title="Tanggal", yaxis_title="Volatility", legend=legend_layout, height=800)
                    st.plotly_chart(fig_vol, use_container_width=True)
            else:
                st.info("Tidak ada data metrik time-series untuk instrumen yang dipilih.")

            st.divider()

            # =====================================================================
            # 5. ANALISIS REZIM PASAR (2-PILAR: STRUKTUR & RSI + BoS DETECTOR)
            # =====================================================================
            st.divider()
            st.subheader("🧭 Analisis Rezim & Detektor BoS (NAV vs RSI)")
            st.caption("Mendeteksi fase pasar menggunakan konfluensi Struktur (BoS) dan Momentum (RSI).")

            # Input Parameter
            col_b1, col_b2 = st.columns(2)
            with col_b1:
                regime_target = st.selectbox("Pilih Produk Utama (Grafik):", options=selected_instruments, key="reg_target_v5")
            with col_b2:
                # Menggunakan multiselect agar bisa diketik dan dipilih banyak sekaligus (maksimal 60)
                bos_lengths = st.multiselect(
                    "Rentang Pivot BoS (Hari):",
                    options=list(range(1, 61)),
                    default=[5, 7, 10],
                    key="bos_len_v5",
                    help="Ketik angka (maksimal 60) atau pilih dari daftar. Bisa memilih lebih dari satu rentang."
                )

            if selected_instruments and bos_lengths:
                summary_data = []
                notif_data = []

                # --- 1. PROSES SEMUA INSTRUMEN UNTUK TABEL RINGKASAN & NOTIFIKASI ---
                for inst in selected_instruments:
                    df_b = pd.DataFrame({'Close': df_compare[inst].ffill()})

                    # Kalkulasi RSI
                    delta = df_b['Close'].diff()
                    gain = delta.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
                    loss = (-1 * delta.clip(upper=0)).ewm(alpha=1/14, adjust=False).mean()
                    rs = np.where(loss == 0, 100, gain / loss)
                    df_b['RSI'] = np.where(loss == 0, 100, 100 - (100 / (1 + rs)))

                    # Variabel penampung BoS Multi-Length
                    is_bull_nav, is_bear_nav = False, False
                    is_bull_rsi, is_bear_rsi = False, False

                    df_b_close_last = df_b['Close'].iloc[-1]
                    df_b_rsi_last = df_b['RSI'].iloc[-1]

                    # Logika BoS (Cukup salah satu rentang tembus, maka dianggap Valid)
                    for l in bos_lengths:
                        hi_nav = df_b['Close'].rolling(l).max().shift(1).iloc[-1]
                        lo_nav = df_b['Close'].rolling(l).min().shift(1).iloc[-1]
                        if df_b_close_last > hi_nav: is_bull_nav = True
                        if df_b_close_last < lo_nav: is_bear_nav = True

                        hi_rsi = df_b['RSI'].rolling(l).max().shift(1).iloc[-1]
                        lo_rsi = df_b['RSI'].rolling(l).min().shift(1).iloc[-1]
                        if df_b_rsi_last > hi_rsi: is_bull_rsi = True
                        if df_b_rsi_last < lo_rsi: is_bear_rsi = True

                    status_nav = "Bullish BoS 📈" if is_bull_nav else ("Bearish BoS 📉" if is_bear_nav else "-")
                    status_rsi = "Bullish BoS 📈" if is_bull_rsi else ("Bearish BoS 📉" if is_bear_rsi else "-")

                    # Logika Penentuan Kondisi
                    if status_nav == "-" and status_rsi == "-":
                        kondisi_akhir = "Stagnan ➖"
                    elif status_nav == status_rsi:
                        kondisi_akhir = "Sinkron ✅"
                    else:
                        kondisi_akhir = "Divergensi ⚠️"

                    # Masukkan SEMUA instrumen ke Tabel Ringkasan (Tanpa Filter)
                    summary_data.append({
                        "Produk": inst,
                        "Status NAV": status_nav,
                        "Status RSI": status_rsi,
                        "Kondisi": kondisi_akhir
                    })

                    # Filter Notifikasi Konflik Tetap Dipertahankan (Hanya yang Divergensi)
                    if (status_nav != "-" or status_rsi != "-") and status_nav != status_rsi:
                        notif_data.append({"Produk": inst, "Detail": f"NAV: {status_nav} | RSI: {status_rsi}"})
                # Tampilan Tabel Notifikasi & Ringkasan
                col_t1, col_t2 = st.columns([2, 1])
                with col_t1:
                    st.markdown("**📋 Ringkasan Struktur BoS Terkini**")
                    if summary_data:
                        st.dataframe(pd.DataFrame(summary_data), use_container_width=True, hide_index=True)
                    else:
                        st.info("Tidak ada penembusan struktur (BoS) pada instrumen terpilih hari ini.")
                with col_t2:
                    st.markdown("**⚠️ Notifikasi Divergensi**")
                    if notif_data:
                        st.warning("Ditemukan ketidaksesuaian struktur antara Harga dan Momentum!")
                        st.dataframe(pd.DataFrame(notif_data), use_container_width=True, hide_index=True)
                    else:
                        st.success("Semua pergerakan tersinkronisasi dengan baik.")

                # --- 2. GRAFIK DETAIL UNTUK PRODUK FOKUS (2-PILAR LINE CHART) ---
                # --- 2. GRAFIK DETAIL UNTUK PRODUK FOKUS (2-PILAR LINE CHART) ---
                if regime_target:
                    st.divider()
                    st.markdown(f"### Analisis Detail: {regime_target}")
                    with st.spinner("Mengkalkulasi grafik rezim garis..."):
                        try:
                            # Ambil data historis lengkap untuk grafik
                            df_f = pd.DataFrame({'Close': df_compare[regime_target].ffill()})

                            # Re-kalkulasi RSI
                            delta_f = df_f['Close'].diff()
                            gain_f = delta_f.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
                            loss_f = (-1 * delta_f.clip(upper=0)).ewm(alpha=1/14, adjust=False).mean()
                            rs_f = np.where(loss_f == 0, 100, gain_f / loss_f)
                            df_f['RSI'] = np.where(loss_f == 0, 100, 100 - (100 / (1 + rs_f)))

                            # Kalkulasi Agregat MA Tren & Pengumpulan Marker untuk Multi-Length
                            df_f['agg_market_trend'] = 0.0
                            bull_dates = set()
                            bear_dates = set()
                            bull_rsi_dates = set()
                            bear_rsi_dates = set()

                            for l in bos_lengths:
                                # Skoring Rata-Rata
                                df_f['agg_market_trend'] += np.where(df_f['Close'] > df_f['Close'].rolling(l).mean(), 1, -1)

                                # 1. Marker NAV
                                hi_nav_s = df_f['Close'].rolling(l).max().shift(1)
                                lo_nav_s = df_f['Close'].rolling(l).min().shift(1)
                                bull_dates.update(df_f[(df_f['Close'] > hi_nav_s) & (df_f['Close'].shift(1) <= hi_nav_s.shift(1))].index)
                                bear_dates.update(df_f[(df_f['Close'] < lo_nav_s) & (df_f['Close'].shift(1) >= lo_nav_s.shift(1))].index)

                                # 2. Marker RSI
                                hi_rsi_s = df_f['RSI'].rolling(l).max().shift(1)
                                lo_rsi_s = df_f['RSI'].rolling(l).min().shift(1)
                                bull_rsi_dates.update(df_f[(df_f['RSI'] > hi_rsi_s) & (df_f['RSI'].shift(1) <= hi_rsi_s.shift(1))].index)
                                bear_rsi_dates.update(df_f[(df_f['RSI'] < lo_rsi_s) & (df_f['RSI'].shift(1) >= lo_rsi_s.shift(1))].index)

                            # Skor Struktur (55 Poin)
                            df_f['score_struct'] = (df_f['agg_market_trend'] / len(bos_lengths)) * 55

                            # Skor RSI (45 Poin)
                            rsi_norm = ((df_f['RSI'] - 50) / 20).clip(-1, 1)
                            df_f['score_rsi'] = rsi_norm * 45

                            # Total Net Score
                            df_f['net_score'] = df_f['score_struct'] + df_f['score_rsi']

                            # Visualisasi 3-Panel
                            fig_res = make_subplots(rows=3, cols=1, shared_xaxes=True,
                                                   vertical_spacing=0.05,
                                                   row_heights=[0.5, 0.25, 0.25])

                            # Panel 1: NAV + Markers
                            fig_res.add_trace(go.Scatter(x=df_f.index, y=df_f['Close'], name='NAV', line=dict(color='orange', width=2)), row=1, col=1)
                            if bull_dates:
                                fig_res.add_trace(go.Scatter(x=sorted(list(bull_dates)), y=df_f.loc[sorted(list(bull_dates)), 'Close'], mode='markers',
                                                              marker=dict(symbol='triangle-up', size=12, color='lime'), name='Bull Break'), row=1, col=1)
                            if bear_dates:
                                fig_res.add_trace(go.Scatter(x=sorted(list(bear_dates)), y=df_f.loc[sorted(list(bear_dates)), 'Close'], mode='markers',
                                                              marker=dict(symbol='triangle-down', size=12, color='red'), name='Bear Break'), row=1, col=1)

                            # Panel 2: RSI
                            fig_res.add_trace(go.Scatter(x=df_f.index, y=df_f['RSI'], name='RSI', line=dict(color='#FF6D00'), fill='tozeroy'), row=2, col=1)

                            fig_res.add_hline(y=70, line_dash="dot", line_color="red", row=2, col=1)
                            fig_res.add_hline(y=30, line_dash="dot", line_color="green", row=2, col=1)

                            # Panel 3: Net Regime Score Line
                            fig_res.add_trace(go.Scatter(x=df_f.index, y=df_f['net_score'], name='Net Score', line=dict(color='white', width=2), fill='tozeroy'), row=3, col=1)
                            fig_res.add_hline(y=75, line_dash="dot", line_color="lime", row=3, col=1)
                            fig_res.add_hline(y=-75, line_dash="dot", line_color="red", row=3, col=1)

                            fig_res.update_layout(height=900, hovermode="x unified", template="plotly_dark")
                            st.plotly_chart(fig_res, use_container_width=True)

                        except Exception as e:
                            st.error(f"Gagal memuat grafik detail: {e}")

            elif not bos_lengths:
                st.warning("Silakan ketik atau pilih minimal satu rentang pivot.")

            # =====================================================================
            # 6. HEATMAP KINERJA BULANAN BENCHMARK
            # =====================================================================
            st.divider()
            st.subheader("📊 Heatmap Kinerja Bulanan Benchmark")
            st.caption("Menampilkan peta panas (heatmap) persentase return bulanan dan akumulasi tahunan untuk instrumen acuan yang dipilih.")

            benchmark_options_heat = {
                'IHSG (.JKSE)': '.JKSE', 'LQ45 (.JKLQ45)': '.JKLQ45', 'IDX30': '.JKIDX30',
                'IDX80': '.JKIDX80', 'NASDAQ (.IXIC)': '.IXIC', 'S&P 500 (.SPX)': '.SPX',
                'Dow Jones (.DJI)': '.DJI', 'Shanghai (.SSEC)': '.SSEC', 'DXY Index': '.DXY',
                'Kurs IDR': 'IDR=', 'Crude Oil (CLc1)': 'CLc1',
                'IDR 10Y Yield': 'ID10YT=RR', 'US 10Y Yield': 'US10YT=RR'
            }

            selected_bench_heat_label = st.selectbox("Pilih Benchmark untuk Heatmap:", list(benchmark_options_heat.keys()), key="heatmap_benchmark_select")
            selected_bench_heat_ticker = benchmark_options_heat[selected_bench_heat_label]

            # Ambil data benchmark full
            bench_heat_series = get_benchmark_series(selected_bench_heat_ticker, full_dfs_dict)

            if not bench_heat_series.empty:
                with st.spinner("Mengkalkulasi Heatmap..."):
                    try:
                        # Buat copy dan bersihkan index timezone jika ada
                        bh_series = bench_heat_series.dropna().copy()
                        bh_series.index = pd.to_datetime(bh_series.index).tz_localize(None)

                        # Resample bulanan
                        try:
                            monthly_prices = bh_series.resample('ME').last()
                        except:
                            monthly_prices = bh_series.resample('M').last()

                        monthly_prices = monthly_prices.dropna()
                        monthly_returns = monthly_prices.pct_change()

                        df_heat = pd.DataFrame({
                            'Year': monthly_returns.index.year,
                            'Month': monthly_returns.index.month,
                            'Return': monthly_returns.values
                        })

                        pivot_heat = df_heat.pivot(index='Year', columns='Month', values='Return')

                        month_names = {1: 'January', 2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June', 7: 'July', 8: 'August', 9: 'September', 10: 'October', 11: 'November', 12: 'December'}
                        pivot_heat = pivot_heat.rename(columns=month_names)

                        for m in month_names.values():
                            if m not in pivot_heat.columns:
                                pivot_heat[m] = np.nan

                        pivot_heat = pivot_heat[list(month_names.values())]

                        # Menghitung Total Return per tahun (YTD) menggunakan harga riil
                        total_ret_dict = {}
                        years = pivot_heat.index.unique()
                        for y in years:
                            prices_this_year = bh_series[bh_series.index.year <= y].dropna()
                            if len(prices_this_year) > 0:
                                last_price = prices_this_year.iloc[-1]

                                prices_prev_year = bh_series[bh_series.index.year < y].dropna()
                                if len(prices_prev_year) > 0:
                                    prev_price = prices_prev_year.iloc[-1]
                                    total_ret_dict[y] = (last_price / prev_price) - 1
                                else:
                                    # Tahun pertama
                                    prices_y = bh_series[bh_series.index.year == y].dropna()
                                    if len(prices_y) > 0:
                                        first_price = prices_y.iloc[0]
                                        total_ret_dict[y] = (last_price / first_price) - 1
                                    else:
                                        total_ret_dict[y] = np.nan
                            else:
                                total_ret_dict[y] = np.nan

                        pivot_heat['Total Return'] = pivot_heat.index.map(total_ret_dict)
                        pivot_heat = pivot_heat.dropna(how='all')

                        # Hitung Average, Positive %, Negative %
                        avg_row = pivot_heat.mean()

                        pos_count = (pivot_heat > 0).sum()
                        neg_count = (pivot_heat < 0).sum()
                        total_count = pivot_heat.notna().sum()

                        pos_pct = pos_count / total_count.replace(0, np.nan)
                        neg_pct = neg_count / total_count.replace(0, np.nan)

                        pivot_heat.loc['AVERAGE'] = avg_row
                        pivot_heat.loc['POSITIVE %'] = pos_pct
                        pivot_heat.loc['NEGATIVE %'] = neg_pct

                        def highlight_cells(x):
                            df_colors = pd.DataFrame('', index=x.index, columns=x.columns)
                            for row in x.index:
                                for col in x.columns:
                                    val = x.loc[row, col]
                                    if pd.isna(val):
                                        continue
                                    if row == 'AVERAGE':
                                        if val > 0:
                                            df_colors.loc[row, col] = 'background-color: #c8e6c9; color: #1b5e20; font-weight: bold;'
                                        elif val < 0:
                                            df_colors.loc[row, col] = 'background-color: #ffcdd2; color: #b71c1c; font-weight: bold;'
                                        else:
                                            df_colors.loc[row, col] = 'font-weight: bold;'
                                    elif row == 'POSITIVE %':
                                        df_colors.loc[row, col] = 'background-color: #ffe0b2; color: #e65100; font-weight: bold;'
                                    elif row == 'NEGATIVE %':
                                        df_colors.loc[row, col] = 'background-color: #ffcc80; color: #e65100; font-weight: bold;'
                                    else:
                                        if val > 0:
                                            df_colors.loc[row, col] = 'background-color: #c8e6c9; color: #1b5e20;'
                                        elif val < 0:
                                            df_colors.loc[row, col] = 'background-color: #ffcdd2; color: #b71c1c;'
                            return df_colors

                        def format_pct(x):
                            if pd.isna(x):
                                return "-"
                            return f"{x*100:.2f}%"

                        styled_heat = pivot_heat.style.apply(highlight_cells, axis=None).format(format_pct)
                        st.dataframe(styled_heat, use_container_width=True, height=600)

                    except Exception as e:
                        st.error(f"Gagal mengkalkulasi heatmap: {e}")
            else:
                st.warning("Data historis benchmark tidak tersedia.")

