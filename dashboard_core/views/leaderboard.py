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


def render_leaderboard(tab_leaderboard_split, ctx):
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
    # ==================== TAB 2: LEADERBOARD ====================
    with tab_leaderboard_split:
        st.header("Leaderboard & Rekomendasi Produk")
        st.info("**Metodologi:** Berfungsi sebagai indikator *Momentum*. Peringkat diukur murni berdasarkan **Return Absolut 5 Hari Kalender** ((Harga Hari Ini / Harga 5 Hari Lalu) - 1). Mengabaikan risiko dan volatilitas untuk mencari aset dengan tren naik jangka pendek tercepat.")
        lb_type = st.radio("Pilih Tipe Leaderboard", ["Equity", "Fixed Income"], horizontal=True, key="lb_split_type")
        if lb_type == "Equity":
            df_lb_full = df_equity_full.drop(columns=[x for x in young_all if x in df_equity_full.columns], errors='ignore')
            title = "Equity"
        else:
            df_lb_full = df_bond_full.drop(columns=[x for x in young_all if x in df_bond_full.columns], errors='ignore')
            title = "Fixed Income"

        if not df_lb_full.empty:
            # UBAH: Panggil dengan days=5
            leaderboard = calculate_daily_leaderboard(df_lb_full, days=5)
            if not leaderboard.empty:
                leaderboard_calc = leaderboard.copy()
                leaderboard_calc['Change_Color'] = leaderboard_calc['Rank_Change'].apply(
                    lambda x: 'Top Climber' if x > 0 else ('Top Laggard' if x < 0 else 'Stabil')
                )
                leaderboard_calc['Return_5d_pct'] = leaderboard_calc['Return_5d'] * 100

                leaderboard_display = leaderboard_calc[['Instrument', 'Return_5d_pct', 'Rank_Today', 'Rank_Change', 'Change_Color']].copy()
                leaderboard_display = leaderboard_display.rename(columns={'Return_5d_pct': 'Return_5d'})
                leaderboard_display['Return_5d'] = leaderboard_display['Return_5d'].map(lambda x: f"{x:.2f}%")

                col1, col2 = st.columns(2)
                with col1:
                    st.subheader(f"Top Climbers 5 Days Return {title}")
                    climbers = leaderboard_display[leaderboard_display['Rank_Change'] > 0].sort_values('Rank_Change', ascending=False).head(10)
                    if not climbers.empty:
                        st.dataframe(climbers, hide_index=True, use_container_width=True)
                    else:
                        st.info("Tidak ada top climbers dalam periode ini.")
                with col2:
                    st.subheader(f"Top Laggards 5 Days Return {title}")
                    laggards = leaderboard_display[leaderboard_display['Rank_Change'] < 0].sort_values('Rank_Change', ascending=True).head(10)
                    if not laggards.empty:
                        st.dataframe(laggards, hide_index=True, use_container_width=True)
                    else:
                        st.info("Tidak ada top laggards dalam periode ini.")

                st.subheader(f"Leaderboard Lengkap Return 5 Hari {title}")
                st.dataframe(leaderboard_display.sort_values('Rank_Today'), hide_index=True, use_container_width=True)

                st.subheader(f"Produk Performa Terbaik Selama 5 Hari {title}")
                top_3 = leaderboard_calc.nsmallest(3, 'Rank_Today')
                top_climbers = leaderboard_calc[leaderboard_calc['Rank_Change'] > 0].nlargest(3, 'Rank_Change')
                col_rec1, col_rec2 = st.columns(2)
                with col_rec1:
                    st.markdown("**Top 3 Performers Saat Ini**")
                    for idx, row in top_3.iterrows():
                        # UBAH: Tampilkan Return_5d
                        st.markdown(f"- **{row['Instrument']}** - Return 5d: {row['Return_5d_pct']:.2f}%")
                # ... (kode Anda sebelumnya) ...
                with col_rec2:
                    st.markdown("**Produk dengan Momentum Terbaik**")
                    for idx, row in top_climbers.iterrows():
                        st.markdown(f"- **{row['Instrument']}** (Naik {row['Rank_Change']} peringkat)")

                # ================= TAMBAHAN SEGMEN PERINGKAT HARIAN =================
                st.divider()

                df_daily_full = df_lb_full

                if not df_daily_full.empty and len(df_daily_full) >= 2:
                    df_daily_full = df_daily_full.ffill()

                    # 1. PROTEKSI HARI LIBUR & WEEKEND
                    # Cek pergerakan harga. Jika yang bergerak kurang dari 3 produk, anggap pasar libur
                    pct_change_all = df_daily_full.pct_change()
                    valid_trading_days = df_daily_full.loc[(pct_change_all.abs() > 0).sum(axis=1) > 2]

                    if len(valid_trading_days) >= 2:
                        # Ambil hari bursa aktif terakhir dan sebelumnya
                        price_today_lb = valid_trading_days.iloc[-1]
                        price_yesterday_lb = valid_trading_days.iloc[-2].replace(0, np.nan)

                        # Ekstrak Tanggal untuk dimunculkan di layar
                        date_today_str = valid_trading_days.index[-1].strftime('%d %b %Y')
                        date_yesterday_str = valid_trading_days.index[-2].strftime('%d %b %Y')

                        st.subheader(f"Peringkat Harian (Daily % Change) - {title}")
                        st.caption(f"Membandingkan penutupan: **{date_today_str}** vs **{date_yesterday_str}**")

                        # 2. Hitung Persentase Perubahan
                        daily_pct_lb = ((price_today_lb / price_yesterday_lb) - 1) * 100

                        # 3. Format DataFrame Peringkat
                        df_daily_lb = daily_pct_lb.reset_index()
                        df_daily_lb.columns = ['Instrument', 'Daily_%_Change']

                        # ELIMINASI: Buang produk yang persis 0.0 (artinya MI belum rilis NAV terbaru)
                        df_daily_lb = df_daily_lb.dropna()
                        df_daily_lb = df_daily_lb[df_daily_lb['Daily_%_Change'] != 0.0]

                        df_daily_lb = df_daily_lb.sort_values('Daily_%_Change', ascending=False)
                        df_daily_lb['Rank'] = range(1, len(df_daily_lb) + 1)

                        # 4. Format teks angka
                        df_daily_display = df_daily_lb[['Rank', 'Instrument', 'Daily_%_Change']].copy()
                        df_daily_display['Daily_%_Change'] = df_daily_display['Daily_%_Change'].round(2).astype(str) + '%'

                        col_d1, col_d2 = st.columns(2)
                        with col_d1:
                            st.markdown("**Top Performers (Gainers)**")
                            if not df_daily_display.empty:
                                st.dataframe(df_daily_display.head(10), hide_index=True, use_container_width=True)
                            else:
                                st.info("Belum ada produk yang naik.")
                        with col_d2:
                            st.markdown("**Bottom Performers (Losers)**")
                            if not df_daily_display.empty:
                                df_losers = df_daily_lb.tail(10).sort_values('Daily_%_Change', ascending=True).copy()
                                df_losers['Daily_%_Change'] = df_losers['Daily_%_Change'].round(2).astype(str) + '%'
                                st.dataframe(df_losers[['Rank', 'Instrument', 'Daily_%_Change']], hide_index=True, use_container_width=True)
                            else:
                                st.info("Belum ada produk yang turun.")
                    else:
                        st.warning("Data hari bursa aktif tidak mencukupi.")
                else:
                    st.warning("Data historis tidak cukup untuk menghitung perubahan harian.")
        else:
            st.warning(f"Tidak ada data {title}.")

