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


def render_bonds(tab_gov_bonds, ctx):
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
    #==================== TAB 6: GRAFIK OBLIGASI NEGARA ====================
    with tab_gov_bonds:
        st.header("Grafik Obligasi Negara (SBN/SUN/Sukuk)")

        st.subheader("NAV Performance Harian (Dari Database)")
        with st.spinner("Mengkalkulasi NAV Performance..."):
            df_nav_perf_bonds = get_nav_performance(df_gov_bonds_price_full)
            if not df_nav_perf_bonds.empty:
                st.dataframe(df_nav_perf_bonds, use_container_width=True, hide_index=True)
            else:
                st.info("Data NAV Performance tidak tersedia.")
        
        st.divider()

        # Gunakan data utuh (_full) agar rentang waktu bisa ditarik independen dari sidebar
        if not df_gov_bonds_price_full.empty:
            available_gov_bonds = df_gov_bonds_price_full.columns.tolist()

            selected_gov_bonds = st.multiselect(
                "Pilih Seri Obligasi untuk Ditampilkan:",
                options=available_gov_bonds,
                default=available_gov_bonds[:min(3, len(available_gov_bonds))] if available_gov_bonds else [],
                key="gov_bonds_multiselect"
            )

            if selected_gov_bonds:
                st.divider()
                # --- PANEL KONTROL CUT-OFF TANGGAL OBLIGASI ---
                st.subheader("Cut-off Data Analisis Obligasi")
                col_date1, col_date2 = st.columns(2)

                # Ambil batas data paling awal dan akhir yang tersedia di master data
                min_date_gov = df_gov_bonds_price_full.index.min().date()
                max_date_gov = df_gov_bonds_price_full.index.max().date()

                # Default mundur 3 tahun untuk obligasi (opsional, bisa disesuaikan)
                default_start_gov = max(min_date_gov, max_date_gov - dt.timedelta(days=365*3))

                with col_date1:
                    start_date_gov = st.date_input("Start Date Obligasi", value=default_start_gov, min_value=min_date_gov, max_value=max_date_gov, key="gov_start_date")
                with col_date2:
                    end_date_gov = st.date_input("End Date Obligasi", value=max_date_gov, min_value=min_date_gov, max_value=max_date_gov, key="gov_end_date")

                # Konversi ke datetime pandas untuk slicing
                start_gov_dt = pd.to_datetime(start_date_gov)
                end_gov_dt = pd.to_datetime(end_date_gov)

                st.divider()
                # --- PANEL KONTROL WAKTU ---
                st.subheader("Kontrol Rentang Waktu")
                col_t1, col_t2 = st.columns(2)
                with col_t1:
                    time_options = {"YTD": "YTD", "1 Bulan": "1M", "3 Bulan": "3M", "6 Bulan": "6M", "1 Tahun": "1Y", "2 Tahun": "2Y", "3 Tahun": "3Y", "5 Tahun": "5Y", "Semua (Sesuai Cut-off)": "ALL"}
                    selected_label_raw = st.selectbox("Rentang Waktu Grafik Mentah & Yield:", list(time_options.keys()), index=8, key="bond_raw_time")
                    raw_time_code = time_options[selected_label_raw]

                with col_t2:
                    yield_options = {"YTD": "YTD", "1 Bulan": "1M", "3 Bulan": "3M", "6 Bulan": "6M", "1 Tahun": "1Y", "2 Tahun": "2Y", "3 Tahun": "3Y", "5 Tahun": "5Y", "Semua (Sesuai Cut-off)": "ALL"}
                    selected_label_rebase = st.selectbox("Rentang Waktu Grafik Persentase (Rebasing):", list(yield_options.keys()), index=4, key="bond_rebase")
                    rebase_code = yield_options[selected_label_rebase]

                # --- FUNGSI PEMBANTU (HELPER FUNCTIONS) ---
                def slice_by_time_range(df, time_code):
                    if df.empty or time_code == "ALL": return df
                    latest_date = df.index.max()
                    if time_code == "YTD": start_date = pd.Timestamp(latest_date.year, 1, 1)
                    elif "M" in time_code: start_date = latest_date - pd.DateOffset(months=int(time_code.replace("M", "")))
                    elif "Y" in time_code: start_date = latest_date - pd.DateOffset(years=int(time_code.replace("Y", "")))
                    else: start_date = df.index.min()
                    return df.loc[start_date:latest_date].copy()

                def apply_rebasing(df, y_code):
                    df_sliced = slice_by_time_range(df, y_code)
                    if df_sliced.empty: return pd.DataFrame()
                    df_rebased = pd.DataFrame(index=df_sliced.index)

                    if len(df_sliced) > 1:
                        for col in df_sliced.columns:
                            # Cari tanggal pertama obligasi ini memiliki data (bukan NaN)
                            first_valid_idx = df_sliced[col].first_valid_index()
                            if first_valid_idx is not None:
                                base_val = df_sliced.loc[first_valid_idx, col]
                                # Hitung persentase dari harga rilis tersebut
                                df_rebased[col] = ((df_sliced[col] / base_val) - 1) * 100
                        return df_rebased
                    return pd.DataFrame()

                def add_end_annotations(fig, df_plot, is_percent=True):
                    if df_plot.empty: return fig
                    line_colors = {trace.name: trace.line.color for trace in fig.data}
                    last_date = df_plot.index[-1]
                    for col in df_plot.columns:
                        last_val = df_plot[col].dropna().iloc[-1] if not df_plot[col].dropna().empty else None
                        if last_val is not None:
                            bg_color = line_colors.get(col, "gray")
                            text_val = f"<b>{last_val:.2f}%</b>" if is_percent else f"<b>{last_val:.2f}</b>"
                            fig.add_annotation(
                                x=last_date, y=last_val, text=text_val,
                                showarrow=False, xanchor="left", xshift=8,
                                font=dict(size=11, color="white"), bgcolor=bg_color, borderpad=3, opacity=0.9
                            )
                    fig.update_layout(margin=dict(r=70))
                    return fig

                legend_layout_gov = dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5, title=None)

                # 1. Tarik data tanpa memaksakan backward fill (bfill)
                df_price_raw = df_gov_bonds_price_full[selected_gov_bonds].ffill()
                df_yield_raw = df_gov_bonds_yield_full[selected_gov_bonds].ffill() if not df_gov_bonds_yield_full.empty else pd.DataFrame()

                # 2. Potong rentang waktu sesuai input Cut-off Obligasi yang baru
                df_price_raw = df_price_raw[(df_price_raw.index >= start_gov_dt) & (df_price_raw.index <= end_gov_dt)]
                if not df_yield_raw.empty:
                    df_yield_raw = df_yield_raw[(df_yield_raw.index >= start_gov_dt) & (df_yield_raw.index <= end_gov_dt)]
                # ==========================================
                # SEGMEN 1: ASK PRICE (HARGA PENAWARAN)
                # ==========================================
                st.divider()
                st.subheader("Analisis Harga Obligasi (Ask Price)")

                tab_p1, tab_p2 = st.tabs(["1. Harga Mentah", "2. Persentase Kenaikan (Rebasing)"])

                with tab_p1:
                    df_p_raw_sliced = slice_by_time_range(df_price_raw, raw_time_code)
                    fig_p1 = px.line(df_p_raw_sliced, x=df_p_raw_sliced.index, y=df_p_raw_sliced.columns, title=f"Harga Aktual - Rentang Waktu: {selected_label_raw}")
                    fig_p1 = add_end_annotations(fig_p1, df_p_raw_sliced, is_percent=False)
                    fig_p1.update_layout(xaxis_title="Tanggal", yaxis_title="Ask Price", yaxis=dict(side='right'), legend=legend_layout_gov, hovermode="x unified", height=900)
                    st.plotly_chart(fig_p1, use_container_width=True)

                with tab_p2:
                    df_p_rebase = apply_rebasing(df_price_raw, rebase_code)
                    if not df_p_rebase.empty:
                        fig_p2 = px.line(df_p_rebase, x=df_p_rebase.index, y=df_p_rebase.columns, title=f"Persentase Kenaikan Harga ({selected_label_rebase})")
                        fig_p2.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
                        fig_p2.update_yaxes(ticksuffix="%")
                        fig_p2 = add_end_annotations(fig_p2, df_p_rebase, is_percent=True)
                        fig_p2.update_layout(xaxis_title="Tanggal", yaxis_title="Perubahan Harga (%)", yaxis=dict(side='right'), legend=legend_layout_gov, hovermode="x unified", height=900)
                        st.plotly_chart(fig_p2, use_container_width=True)
                    else:
                        st.warning("Data tidak mencukupi untuk Rebasing.")

                # ==========================================
                # SEGMEN 2: ASK YIELD (IMBAL HASIL)
                # ==========================================
                st.divider()
                st.subheader("Analisis Imbal Hasil (Ask Yield)")

                if not df_yield_raw.empty:
                    df_y_raw_sliced = slice_by_time_range(df_yield_raw, raw_time_code)
                    fig_y1 = px.line(df_y_raw_sliced, x=df_y_raw_sliced.index, y=df_y_raw_sliced.columns, title=f"Yield Aktual (%) - Rentang Waktu: {selected_label_raw}")
                    fig_y1.update_yaxes(ticksuffix="%")
                    fig_y1 = add_end_annotations(fig_y1, df_y_raw_sliced, is_percent=True)
                    fig_y1.update_layout(xaxis_title="Tanggal", yaxis_title="Ask Yield (%)", yaxis=dict(side='right'), legend=legend_layout_gov, hovermode="x unified", height=900)
                    st.plotly_chart(fig_y1, use_container_width=True)
                else:
                    st.warning("Data Yield tidak tersedia sama sekali di database.")
            else:
                st.info("Pilih minimal 1 seri obligasi.")
        else:
            st.warning("Data Obligasi Negara tidak tersedia di database.")

        st.divider()

