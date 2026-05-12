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


def render_performance(tab_performance, ctx):
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
    # ==================== TAB 4: PERFORMA & RANKING (GABUNGAN) ====================
    with tab_performance:
        st.header("Performa, Metrik & Riwayat Peringkat")

        # Kalkulasi string tanggal untuk deskripsi dinamis
        start_date_str = ana_start_dt.strftime('%d %b %Y')
        end_date_str = ana_end_dt.strftime('%d %b %Y')

        st.info(f"""**Metodologi Metrik & Skor Komposit ({date_option} | {start_date_str} s/d {end_date_str}):**
        - **Momentum Return (3 Metrik):** Persentase profit 1W, 1M, dan 3M.
        - **Risk & Reward (4 Metrik):** Volatilitas, Sharpe Ratio, Alpha, dan Beta.
        - **Konsistensi Peringkat (12 Metrik):** Frekuensi dan *streak* produk bertahan di Top 5, 10, dan 20 pada berbagai rentang waktu.
        - **Akselerasi Tren / Climbers (4 Metrik):** Perubahan posisi peringkat harian saat ini dibandingkan posisi 1, 7, 14, dan 22 hari perdagangan sebelumnya.
        - **Skor Valuasi:** Tersedia sebagai mode fokus terpisah berdasarkan deviasi Z-Score.
        - **Total Skor:** Penjumlahan persentil berbobot sesuai mode skoring aktif.""")

        perf_type = st.radio("Pilih Kategori Aset", ["Equity", "Fixed Income"], horizontal=True, key="perf_type_radio")

        if perf_type == "Equity":
            df_perf = df_equity
            df_perf_full = df_equity_full # Tambahkan data utuh
            metrics_perf = metrics_equity
            ranked_perf = ranked_products_equity
            title = "Equity"
        else:
            df_perf = df_bond
            df_perf_full = df_bond_full # Tambahkan data utuh
            metrics_perf = metrics_bond
            ranked_perf = ranked_products_bond
            title = "Fixed Income"

        if not df_perf.empty and ranked_perf is not None and not ranked_perf.empty:

            # Gabungkan metrik dan seluruh kolom skor (_scaled) di awal agar mudah dipilah
            full_performance = metrics_perf.merge(ranked_perf, left_index=True, right_index=True, how='left')

            # BUANG PRODUK MUDA DARI LEADERBOARD TAB 4
            full_performance_clean = full_performance.drop(index=[x for x in young_all if x in full_performance.index], errors='ignore')

            # --- 1. DUA LEADERBOARD TOP 5 (ATAS) ---
            st.subheader(f"Leaderboards: Top 5 {title}")
            col_top1, col_top2 = st.columns(2)

            with col_top1:
                st.markdown(f"**Top 5 Composite Score** (Benchmark: {selected_bench_label})")
                top5_score = full_performance_clean.sort_values('Total_Score', ascending=False).head(5)
                df_show_score = top5_score[['Total_Score']].copy()
                df_show_score['Total_Score'] = df_show_score['Total_Score'].round(3)
                df_show_score = df_show_score.rename(columns={'Total_Score': 'Skor Komposit'})
                df_show_score.index.name = 'Nama Produk'
                st.dataframe(df_show_score, use_container_width=True)

            with col_top2:
                st.markdown(f"**Top 5 Performa (Return {date_option})**")
                top5_return = full_performance_clean.sort_values('Interval_Return', ascending=False).head(5)
                df_show_return = top5_return[['Interval_Return']].copy()
                df_show_return['Interval_Return'] = (df_show_return['Interval_Return'] * 100).round(2).astype(str) + '%'
                df_show_return = df_show_return.rename(columns={'Interval_Return': f'Return {date_option}'})
                df_show_return.index.name = 'Nama Produk'
                st.dataframe(df_show_return, use_container_width=True)

            st.divider()

            # --- 2. ANALISIS MENDALAM PRODUK ---
            st.subheader(f"Analisis Kekuatan & Kelemahan")
            st.caption(f"Perbandingan kinerja {date_option} terakhir terhadap rata-rata kategori.")

            # Menggunakan list dari Top 5 Skor Komposit sebagai opsi dropdown
            top_5_list = top5_score.index.tolist()
            if top_5_list:
                selected_top = st.selectbox(f"Pilih Produk {title} untuk dianalisis:", top_5_list, key="perf_select_top")
                if selected_top:
                    metrics_top = metrics_perf.loc[selected_top]

                    strengths = []
                    weaknesses = []

                    # Cek Metrik Return (Sesuai Interval)
                    if pd.notna(metrics_top['Interval_Return']):
                        avg_ret = metrics_perf['Interval_Return'].mean() * 100
                        val_ret = metrics_top['Interval_Return'] * 100
                        if val_ret > avg_ret:
                            strengths.append(f"• Return ({date_option}): Profit {val_ret:.2f}%, di atas rata-rata {avg_ret:.2f}%.")
                        elif val_ret < avg_ret:
                            weaknesses.append(f"• Return ({date_option}): Profit {val_ret:.2f}%, tertinggal dari rata-rata {avg_ret:.2f}%.")

                    # Cek Metrik Sharpe
                    if pd.notna(metrics_top['Sharpe_Ratio']):
                        avg_sharpe = metrics_perf['Sharpe_Ratio'].mean()
                        val_sharpe = metrics_top['Sharpe_Ratio']
                        if val_sharpe > avg_sharpe:
                            strengths.append(f"• Sharpe Ratio: Nilai {val_sharpe:.2f} lebih tinggi dari rata-rata {avg_sharpe:.2f}.")
                        elif val_sharpe < avg_sharpe:
                            weaknesses.append(f"• Sharpe Ratio: Nilai {val_sharpe:.2f} lebih rendah dari rata-rata {avg_sharpe:.2f}.")

                    # Cek Metrik Volatility (Desimal)
                    if pd.notna(metrics_top['Volatility']):
                        avg_vol = metrics_perf['Volatility'].mean()
                        val_vol = metrics_top['Volatility']
                        if val_vol < avg_vol:
                            strengths.append(f"• Volatility: Fluktuasi {val_vol:.4f} lebih stabil dibanding rata-rata {avg_vol:.4f}.")
                        elif val_vol > avg_vol:
                            weaknesses.append(f"• Volatility: Fluktuasi {val_vol:.4f} lebih berisiko dibanding rata-rata {avg_vol:.4f}.")

                    # Cek Metrik Alpha (Desimal)
                    if pd.notna(metrics_top['Alpha']):
                        avg_alpha = metrics_perf['Alpha'].mean()
                        val_alpha = metrics_top['Alpha']
                        if val_alpha > avg_alpha:
                            strengths.append(f"• Alpha (Benchmark: {selected_benchmark_label}): Nilai {val_alpha:.4f} mengungguli rata-rata {avg_alpha:.4f}.")
                        elif val_alpha < avg_alpha:
                            weaknesses.append(f"• Alpha (Benchmark: {selected_benchmark_label}): Nilai {val_alpha:.4f} di bawah rata-rata {avg_alpha:.4f}.")

                    # Tampilan UI 2 Kolom
                    col_str, col_weak = st.columns(2)
                    with col_str:
                        st.write("Kekuatan:")
                        if strengths:
                            for s in strengths:
                                st.write(s)
                        else:
                            st.write("Tidak ada keunggulan mencolok dibanding rata-rata.")

                    with col_weak:
                        st.write("Kelemahan:")
                        if weaknesses:
                            for w in weaknesses:
                                st.write(w)
                        else:
                            st.write("**Tidak ada kelemahan**")
                            st.write("Tidak ada metrik utama di bawah rata-rata.")

            st.divider()

            # --- 3. TABEL METRIK & SKOR LENGKAP ---
            st.subheader(f"Tabel Metrik & Skor Lengkap - {title} (Benchmark: {selected_bench_label})")
            st.caption(f"Fokus Evaluasi: **{scoring_mode}**. Tabel ini menampilkan nilai mentah (raw) dari masing-masing metrik untuk perbandingan langsung.")

            # [Perbaikan]: Menyalin langsung dari metrik mentah lalu menyuntikkan Total Skor agar tidak ada kolom yang terhapus
            full_performance_display = metrics_perf.copy()
            if ranked_perf is not None and 'Total_Score' in ranked_perf.columns:
                full_performance_display['Total_Score'] = ranked_perf['Total_Score']
            else:
                full_performance_display['Total_Score'] = 0

            full_performance_display = full_performance_display.sort_values('Total_Score', ascending=False, na_position='last')
            full_performance_display.index.name = 'Nama Produk'

            # 1. Klasifikasi Kelompok Metrik
            # return_metrics = ['Inception_Return', 'Interval_Return', 'Return_1W', 'Return_1M', 'Return_3M']
            return_metrics = ['Interval_Return', 'Return_1W', 'Return_1M', 'Return_3M']
            risk_metrics = ['Sharpe_Ratio', 'Alpha', 'Beta', 'Volatility']
            consist_metrics = [
                'Consist_1d_Top5', 'Consist_1d_Top10', 'Consist_1d_Top20',
                'Consist_7d_Top5', 'Consist_7d_Top10', 'Consist_7d_Top20',
                'Consist_14d_Top5', 'Consist_14d_Top10', 'Consist_14d_Top20',
                'Consist_21d_Top5', 'Consist_21d_Top10', 'Consist_21d_Top20'
            ]
            climb_metrics = ['Climb_1d', 'Climb_7d', 'Climb_14d', 'Climb_22d']
            val_metrics = ['Z_Score', 'Status_Valuasi', 'Skor_Valuasi']

            # 2. Filter Dinamis Sesuai Mode di Sidebar
            active_metrics = []
            if scoring_mode == "Balanced (Semua Metrik)":
                active_metrics = return_metrics + risk_metrics + consist_metrics + climb_metrics + val_metrics
            elif scoring_mode == "Fokus Return (Profit)":
                active_metrics = return_metrics
            elif scoring_mode == "Fokus Risiko & Rasio":
                active_metrics = risk_metrics
            elif scoring_mode == "Fokus Konsistensi":
                active_metrics = consist_metrics
            elif scoring_mode == "Fokus Momentum (Climbers)":
                active_metrics = climb_metrics
            elif scoring_mode == "Fokus Valuasi (Murah/Mahal)":
                active_metrics = val_metrics

            # 3. Bangun Urutan Kolom
            cols_order = ['Total_Score']
            for metric in active_metrics:
                if metric in full_performance_display.columns:
                    cols_order.append(metric)

            # 4. Potong Dataframe Hanya untuk Kolom Aktif
            full_performance_display = full_performance_display[cols_order]

            # 5. Eksekusi Formatting Tampilan
            cols_to_format = {
                #'Inception_Return': "{:.2%}",
                'Interval_Return': "{:.2%}", 'Return_1W': "{:.2%}", 'Return_1M': "{:.2%}", 'Return_3M': "{:.2%}",
                'Volatility': "{:.4f}",
                'Sharpe_Ratio': "{:.4f}",
                'Beta': "{:.4f}",
                'Alpha': "{:.4f}",
                'Z_Score': "{:+.2f} SD",
                'Skor_Valuasi': "{:.2f}",
                'Total_Score': "{:.3f}"
            }

            for col in full_performance_display.columns:
                if col.startswith('Consist_'):
                    cols_to_format[col] = "{:.0f}"
                elif col.startswith('Climb_'):
                    cols_to_format[col] = "{:+.0f}"

            final_format = {k: v for k, v in cols_to_format.items() if k in full_performance_display.columns}

            # --- HIGHLIGHT KUNING UNTUK REKSA DANA MUDA DI TABEL SKOR ---
            def highlight_perf_young(row):
                # Cek apakah nama index (nama produk) masuk daftar young_all
                if row.name in young_all:
                    return ['background-color: rgba(255, 215, 0, 0.2)'] * len(row)
                return [''] * len(row)

            styled_performance_table = full_performance_display.style.apply(highlight_perf_young, axis=1).format(final_format, na_rep="-")

            st.caption("*(Catatan: Baris yang di-highlight **Kuning** adalah instrumen berumur kurang dari 1 tahun)*")
            st.dataframe(styled_performance_table, use_container_width=True)

            st.divider()

            # --- 3. HEATMAP ANALISIS LANJUTAN (BAWAH) ---
            st.subheader(f"Heatmap Analisis Lanjutan - {title} (Benchmark: {selected_bench_label})")
            st.caption("Pilih interval lompatan waktu dan jumlah kolom evaluasi. Sistem akan memotong data murni berdasarkan urutan indeks array (Trading Days), menjamin jumlah kolom yang presisi.")

            # Panel Kendali Terpadu untuk Heatmap
            col_h1, col_h2, col_h3 = st.columns(3)
            with col_h1:
                period_map = {
                    "Harian": 1,
                    "Mingguan (5 Hari Bursa)": 5,
                    "2 Mingguan (10 Hari)": 10,
                    "3 Mingguan (15 Hari)": 15,
                    "Bulanan (21 Hari)": 21
                }
                selected_period_label = st.selectbox("Lompatan Interval:", list(period_map.keys()), index=1, key="heat_interval")
                trading_interval = period_map[selected_period_label]
            with col_h2:
                num_columns = st.selectbox("Jumlah Kolom (Periode):", [5, 10, 15, 20, 30], index=1, key="heat_columns")
            with col_h3:
                selected_top_n = st.selectbox("Target Peringkat (Highlights):", [5, 10, 20], index=0, key="heat_top_n")

            # Sistem Peringatan Jika Data Historis dari API Tidak Cukup Panjang
            required_days = num_columns * trading_interval
            if len(df_perf_full) < required_days:
                st.warning(f"Data yang ditarik dari API hanya mencakup {len(df_perf_full)} hari bursa. Untuk merender {num_columns} kolom dengan interval '{selected_period_label}', Anda harus mengubah 'Start Date' di sidebar menjadi lebih lama (minimal {(required_days/252):.1f} tahun ke belakang).")

            def get_custom_highlights(df_ranks, top_n=10):
                stats = []
                for inst, row in df_ranks.iterrows():
                    numeric_row = pd.to_numeric(row, errors='coerce')
                    is_top = (numeric_row <= top_n) & (numeric_row > 0)
                    total_in = is_top.sum()

                    streak, max_streak = 0, 0
                    for val in is_top:
                        if val:
                            streak += 1
                            if streak > max_streak: max_streak = streak
                        else:
                            streak = 0

                    if total_in > 0:
                        stats.append({
                            'Instrument': inst,
                            f'Total Masuk Top {top_n}': total_in,
                            'Streak Terlama': max_streak
                        })

                if stats:
                    df_stats = pd.DataFrame(stats).set_index('Instrument')
                    return df_stats.sort_values(by=[f'Total Masuk Top {top_n}', 'Streak Terlama'], ascending=[False, False]).head(10)
                return pd.DataFrame()

            tab_heat1, tab_heat2, tab_heat3 = st.tabs(["1. Peringkat Skor Komposit", "2. Peringkat Performa Absolut", "3. Return Bulanan (MoM %)"])

            with tab_heat1:
                st.markdown(f"**Heatmap Riwayat Peringkat Komposit (Skoring Dinamis)**")
                with st.spinner("Mengkalkulasi komposit skor historis..."):
                    history_score_df = get_detailed_ranking_history(
                        df_perf_full, benchmark_series_full, risk_free_rate,
                        metric_window=cutoff_days, num_columns=num_columns,
                        custom_weights=weights_dict, trading_days_interval=trading_interval,
                        young_funds_list=young_all, bench_ticker=selected_benchmark_ticker
                    )

                    if not history_score_df.empty and len(history_score_df.columns) > 1:
                        rank_score_data = history_score_df.drop(columns=['Streak_Top5'], errors='ignore')
                        rank_score_data = rank_score_data.sort_values(by=rank_score_data.columns[-1], ascending=True)

                        highlight_score = get_custom_highlights(rank_score_data, top_n=selected_top_n)
                        if not highlight_score.empty:
                            st.markdown(f"**Top Highlights (Paling Sering Masuk Peringkat 1-{selected_top_n}):**")
                            st.dataframe(highlight_score, use_container_width=True)

                        st.dataframe(rank_score_data.style.background_gradient(cmap='RdYlGn_r', axis=None).format(precision=0, na_rep="-"), use_container_width=True)
                    else:
                        st.warning("Data tidak mencukupi untuk memvisualisasikan heatmap skor komposit.")

            with tab_heat2:
                st.markdown(f"**Heatmap Peringkat Performa (Kenaikan Harga Absolut)**")
                df_perf_full_clean = df_perf_full.drop(columns=[x for x in young_all if x in df_perf_full.columns], errors='ignore')
                period_ranks_df = get_period_performance_ranking(
                    df_perf_full_clean,
                    trading_days_interval=trading_interval,
                    num_periods=num_columns
                )

                if not period_ranks_df.empty:
                    period_ranks_df = period_ranks_df.sort_values(by=period_ranks_df.columns[-1], ascending=True)
                    highlight_period = get_custom_highlights(period_ranks_df, top_n=selected_top_n)

                    if not highlight_period.empty:
                        st.markdown(f"**Highlights (Paling Konsisten Masuk Top {selected_top_n}):**")
                        st.dataframe(highlight_period, use_container_width=True)

                    st.dataframe(period_ranks_df.style.background_gradient(cmap='RdYlGn_r', axis=None).format(precision=0, na_rep="-"), use_container_width=True)
                else:
                    st.warning("Data historis tidak mencukupi untuk membuat heatmap performa dengan parameter tersebut.")

            with tab_heat3:
                st.markdown(f"**Heatmap Persentase Perubahan Bulanan ({num_columns} Bulan Terakhir)**")
                monthly_pct_df = get_monthly_pct_change(df_perf_full_clean)
                if not monthly_pct_df.empty:
                    # Potong kolom tepat sebanyak jumlah periode yang diminta
                    monthly_pct_df = monthly_pct_df.iloc[:, -num_columns:]

                    monthly_ranks_temp = monthly_pct_df.rank(axis=0, ascending=False, method='min')
                    highlight_monthly = get_custom_highlights(monthly_ranks_temp, top_n=selected_top_n)

                    if not highlight_monthly.empty:
                        st.markdown(f"**Highlights (Paling Sering Masuk Top {selected_top_n} Profit Bulanan Tertinggi):**")
                        st.dataframe(highlight_monthly, use_container_width=True)

                    monthly_pct_df['Rata-rata MoM'] = monthly_pct_df.mean(axis=1)
                    monthly_pct_df = monthly_pct_df.sort_values('Rata-rata MoM', ascending=False)
                    st.dataframe(monthly_pct_df.style.background_gradient(cmap='RdYlGn', axis=None).format("{:.2f}%", na_rep="-"), use_container_width=True)
                else:
                    st.warning("Data tidak mencukupi untuk membentuk interval bulanan.")

        else:
            st.warning(f"Tidak ada data {title} atau data tidak mencukupi untuk perhitungan performa.")

