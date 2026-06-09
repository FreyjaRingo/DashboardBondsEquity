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
    get_nav_performance,
)

from .common import bind_context



def render_overview(tab_overview, ctx):
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
    # --- Tab 1: Ringkasan ---
    with tab_overview:
        st.header("Ringkasan Pasar & Instrumen")
        st.info("Metodologi: Peringkat Top 10 dihitung dari skor komposit dinamis sesuai Fokus Skoring di sidebar. Mode Balanced memakai return 1W/1M/3M, risk-reward, konsistensi ranking, dan momentum climbers.")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Jumlah Equity", len(df_equity.columns))
        with col2:
            st.metric("Jumlah Fixed Income", len(df_bond.columns))
        with col3:
            st.metric("Periode (Hari)", df_all_instruments.shape[0])
        with col4:
            st.metric("Risk-Free Rate (Ask Yield)", f"{risk_free_rate*100:.2f}%")

        st.subheader("Top 10 Produk (Skor Tertinggi & Riwayat Peringkat Skor 7 Hari)")

        # Tambahkan Radio Button untuk memisah kategori
        top10_category = st.radio("Pilih Kategori Produk:", ["Equity", "Fixed Income"], horizontal=True, key="top10_radio")

        # Logika percabangan data sesuai pilihan radio button
        if top10_category == "Equity":
            ranked_to_show = ranked_products_equity
            df_to_show = df_equity
        else:
            ranked_to_show = ranked_products_bond
            df_to_show = df_bond

        if not ranked_to_show.empty:
            with st.spinner(f"Mengkalkulasi jejak peringkat 7 hari terakhir untuk {top10_category}..."):
                history_ranks = get_7d_ranking_history(df_to_show, benchmark_series_sliced, risk_free_rate, eval_window=cutoff_days, custom_weights=weights_dict, young_funds_list=young_all, bench_ticker=selected_benchmark_ticker)

            top_10 = ranked_to_show.head(10).reset_index()
            if 'index' in top_10.columns:
                top_10 = top_10.rename(columns={'index': 'Instrument'})

            top_10['Total_Score'] = top_10['Total_Score'].round(3)

            if not history_ranks.empty:
                top_10 = top_10.merge(history_ranks, left_on='Instrument', right_index=True, how='left')
                cols_to_show = ['Instrument', 'Total_Score'] + list(history_ranks.columns)
            else:
                cols_to_show = ['Instrument', 'Total_Score']

            st.dataframe(top_10[cols_to_show], use_container_width=True, hide_index=True)
        else:
            st.warning(f"Tidak ada data peringkat untuk {top10_category}.")

        # --- TAMBAHAN: TABEL KHUSUS REKSA DANA MUDA ---
        young_list_to_show = young_equities if top10_category == "Equity" else young_bonds
        metrics_to_show = metrics_equity if top10_category == "Equity" else metrics_bond

        if young_list_to_show and metrics_to_show is not None:
            st.divider()
            st.warning(f"Terdapat **{len(young_list_to_show)} {top10_category}** yang umur nya tidak selama interval data.")
            st.caption(f"Instrumen ini dianulir dari evaluasi Skor Komposit Risiko, namun diperingkat secara independen di bawah ini murni berdasarkan kinerja profit absolut pada interval analisis **{date_option}**.")

            # Ekstrak metrik khusus untuk produk muda
            df_young = metrics_to_show.loc[metrics_to_show.index.isin(young_list_to_show)].copy()

            if not df_young.empty:
                # Ranking performa (Interval Return) khusus di dalam kelompok produk muda
                df_young['Peringkat_Performa'] = df_young['Interval_Return'].rank(ascending=False, method='min')
                df_young = df_young.sort_values('Peringkat_Performa').reset_index()
                df_young = df_young.rename(columns={'index': 'Nama Instrumen'})

                # Format teks ke persentase
                df_young[f'Return ({date_option})'] = (df_young['Interval_Return'] * 100).round(2).astype(str) + '%'
                # df_young['Return Sejak Rilis (Inception)'] = (df_young['Inception_Return'] * 100).round(2).astype(str) + '%'

                # Buang desimal pada kolom peringkat
                df_young['Peringkat_Performa'] = df_young['Peringkat_Performa'].fillna(0).astype(int)

                # st.dataframe(
                #     df_young[['Peringkat_Performa', 'Nama Instrumen', f'Return ({date_option})', 'Return Sejak Rilis (Inception)']],
                #     hide_index=True,
                #     use_container_width=True
                # )
                st.dataframe(
                    df_young[['Peringkat_Performa', 'Nama Instrumen', f'Return ({date_option})']],
                    hide_index=True,
                    use_container_width=True
                )
                st.divider()

        # =====================================================================
        # --- TAMBAHAN: TABEL RINGKASAN BoS (NAV & RSI) ---
        # =====================================================================
        st.divider()
        st.subheader("Ringkasan Reversal Struktur (BoS)")

        # Mengambil setting 'bos_len_v5' yang ada di Tab 5 (Perbandingan Historis) via session state.
        # Fallback [5, 7, 10] jika user belum sempat membuka Tab 5.
        current_bos_lens = st.session_state.get("bos_len_v5", [5, 7, 10])

        st.caption(f"Menampilkan produk yang mengalami penembusan struktur (BoS) pada NAV atau RSI menggunakan Pivot: {current_bos_lens}")

        if not df_all_instruments.empty:
            summary_bos_list = []

            for inst in df_all_instruments.columns:
                df_temp = pd.DataFrame({'Close': df_all_instruments[inst].ffill()})

                # 1. Kalkulasi RSI (Standard 14 Day)
                delta = df_temp['Close'].diff()
                gain = delta.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
                loss = (-1 * delta.clip(upper=0)).ewm(alpha=1/14, adjust=False).mean()
                rs = np.where(loss == 0, 100, gain / loss)
                df_temp['RSI'] = np.where(loss == 0, 100, 100 - (100 / (1 + rs)))

                is_bull_nav, is_bear_nav = False, False
                is_bull_rsi, is_bear_rsi = False, False

                # Nilai terakhir untuk dicek terhadap Pivot
                last_nav = df_temp['Close'].iloc[-1]
                last_rsi = df_temp['RSI'].iloc[-1]

                # 2. Deteksi BoS Multi-Length (Sesuai setting di Tab Perbandingan)
                for l in current_bos_lens:
                    # Ambil shift(1) agar nilai hari ini tidak ikut dihitung sebagai max/min masa lalu
                    hi_n = df_temp['Close'].rolling(l).max().shift(1).iloc[-1]
                    lo_n = df_temp['Close'].rolling(l).min().shift(1).iloc[-1]
                    if last_nav > hi_n: is_bull_nav = True
                    if last_nav < lo_n: is_bear_nav = True

                    hi_r = df_temp['RSI'].rolling(l).max().shift(1).iloc[-1]
                    lo_r = df_temp['RSI'].rolling(l).min().shift(1).iloc[-1]
                    if last_rsi > hi_r: is_bull_rsi = True
                    if last_rsi < lo_r: is_bear_rsi = True

                stat_nav = "BoS 🟢" if is_bull_nav else ("BoS 🔴" if is_bear_nav else "-")
                stat_rsi = "BoS 🟢" if is_bull_rsi else ("BoS 🔴" if is_bear_rsi else "-")

                # 3. Filter: Hanya tampilkan produk yang memiliki aktivitas BoS di salah satu indikator
                if stat_nav != "-" or stat_rsi != "-":
                    summary_bos_list.append({
                        "Nama Produk": inst,
                        "BoS NAV": stat_nav,
                        "BoS RSI": stat_rsi,
                        "Kondisi": "Sinkron ✅" if stat_nav == stat_rsi else "Divergensi ⚠️"
                    })

            if summary_bos_list:
                df_bos_summary = pd.DataFrame(summary_bos_list)
                # Tampilkan tabel ringkasan
                st.dataframe(df_bos_summary, use_container_width=True, hide_index=True)
            else:
                st.info("Tidak ada indikasi BoS (Reversal Struktur) pada seluruh instrumen saat ini.")

        st.divider()

        st.subheader(f"Tren Pasar: {selected_bench_label}")

        if not benchmark_series_sliced.empty:
            # Kalkulasi persentase perubahan dari awal periode untuk keterangan tambahan
            bench_start_val = benchmark_series_sliced.iloc[0]
            bench_end_val = benchmark_series_sliced.iloc[-1]
            bench_pct_change = ((bench_end_val / bench_start_val) - 1) * 100

            st.caption(f"Pergerakan nilai **{selected_bench_label}**.")

            fig_bench = px.line(
                x=benchmark_series_sliced.index,
                y=benchmark_series_sliced.values
            )

            # Penyesuaian layout agar grafik terlihat bersih dan area bawahnya terisi warna (area chart)
            fig_bench.update_layout(
                xaxis_title="",
                yaxis_title="Nilai / Harga",
                hovermode="x unified",
                margin=dict(l=0, r=0, t=10, b=0),
                height=300
            )
            fig_bench.update_traces(
                fill='tozeroy',
                line_color='rgba(29, 161, 242, 0.8)',
                fillcolor='rgba(29, 161, 242, 0.1)'
            )

            st.plotly_chart(fig_bench, use_container_width=True)
        else:
            st.warning(f"Data historis untuk benchmark {selected_benchmark_label} tidak tersedia pada rentang waktu ini.")

        # --- GRAFIK LIKUIDITAS (HANYA UNTUK INDEKS) ---
        if selected_benchmark_ticker in df_index_full.columns:
            if selected_benchmark_ticker in df_index_vol.columns:
                liquidity_series = df_index_vol[selected_benchmark_ticker].dropna()
                if not liquidity_series.empty and (liquidity_series != 0).any():
                    st.subheader(f"Volume: {selected_bench_label}")
                    st.caption(f"Volume perdagangan untuk **{selected_bench_label}**.")
                    fig_liq = px.bar(
                        x=liquidity_series.index,
                        y=liquidity_series.values
                    )
                    fig_liq.update_layout(
                        xaxis_title="",
                        yaxis_title="Volume",
                        hovermode="x unified",
                        margin=dict(l=0, r=0, t=10, b=0),
                        height=300
                    )
                    fig_liq.update_traces(
                        marker_color='rgba(255, 165, 0, 0.8)'
                    )
                    st.plotly_chart(fig_liq, use_container_width=True)
        
        st.subheader("NAV Performance Harian (Dari Database)")
        
        # Tambahan Tabel NAV Performance Dinamis (Reksa Dana)
        with st.spinner("Mengkalkulasi NAV Performance..."):
            df_nav_perf = get_nav_performance(df_equity_full)
            if not df_nav_perf.empty:
                st.dataframe(df_nav_perf, use_container_width=True, hide_index=True)
            else:
                st.info("Data NAV Performance tidak tersedia.")

        st.divider()

