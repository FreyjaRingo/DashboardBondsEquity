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


def render_correlation(tab_correlation, ctx):
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
    # ==================== TAB 3: KORELASI ====================
    with tab_correlation:
        st.header("Analisis Korelasi")
        st.info("**Metodologi:** Menggunakan **Korelasi Pearson** pada pergerakan *return* harian. Nilai 1 (Hijau) berarti pergerakan searah sempurna, -1 (Merah) berlawanan sempurna, dan 0 (Kuning/Pucat) menunjukkan tidak ada hubungan linier antar aset.")

        # --- 1. Ekstrak Daftar Manajer Investasi (MI) Dinamis ---
        all_fund_names = list(df_equity.columns) + list(df_bond.columns)
        mi_set = set()
        for name in all_fund_names:
            if name.startswith("BNP Paribas"):
                mi_set.add("BNP Paribas")
            elif name.startswith("Eastspring"):
                mi_set.add("Eastspring")
            elif name.startswith("TRIM") or name.startswith("Trimegah"):
                mi_set.add("Trimegah")
            else:
                mi_set.add(name.split()[0]) # Ambil kata pertama (Maybank, Schroder, Batavia, dll)

        mi_list = ["Semua"] + sorted(list(mi_set))

        # --- 2. Konfigurasi Filter Sumbu X dan Y ---
        col_corr1, col_corr2 = st.columns(2)
        with col_corr1:
            grup1 = st.selectbox("Pilih Grup Aset 1 (Sumbu Y)", options=["Equity", "Fixed Income"], key="corr_grup1")
            filter_mi1 = st.selectbox(f"Filter MI {grup1} (Sumbu Y):", options=mi_list, index=0, key="mi_grup1")

        with col_corr2:
            grup2 = st.selectbox("Pilih Grup Aset 2 (Sumbu X)", options=["Equity", "Fixed Income", "Indeks", "Komoditas", "Mata Uang", "Suku Bunga"], key="corr_grup2")
            # Filter MI di Sumbu X hanya relevan jika yang dipilih adalah reksa dana
            if grup2 in ["Equity", "Fixed Income"]:
                filter_mi2 = st.selectbox(f"Filter MI {grup2} (Sumbu X):", options=mi_list, index=0, key="mi_grup2")
            else:
                filter_mi2 = "Semua"
                st.selectbox(f"Filter MI (Tidak berlaku untuk {grup2}):", options=["-"], disabled=True)

        # --- 3. Tarik Data Utama ---
        dict_dfs = {
            "Equity": df_equity,
            "Fixed Income": df_bond,
            "Indeks": df_index,
            "Komoditas": df_komoditas,
            "Mata Uang": df_mata_uang,
            "Suku Bunga": df_suku_bunga
        }

        df_grup1 = dict_dfs[grup1].copy()
        df_grup2 = dict_dfs[grup2].copy()

        # --- 4. Eksekusi Pemotongan Kolom Berdasarkan MI ---
        if filter_mi1 != "Semua":
            if filter_mi1 == "Trimegah":
                cols_to_keep = [c for c in df_grup1.columns if c.startswith("Trimegah") or c.startswith("TRIM")]
            else:
                cols_to_keep = [c for c in df_grup1.columns if c.startswith(filter_mi1)]
            df_grup1 = df_grup1[cols_to_keep]

        if filter_mi2 != "Semua" and grup2 in ["Equity", "Fixed Income"]:
            if filter_mi2 == "Trimegah":
                cols_to_keep = [c for c in df_grup2.columns if c.startswith("Trimegah") or c.startswith("TRIM")]
            else:
                cols_to_keep = [c for c in df_grup2.columns if c.startswith(filter_mi2)]
            df_grup2 = df_grup2[cols_to_keep]

        # --- 5. Kalkulasi Return & Matriks ---
        returns_grup1 = df_grup1.dropna(axis=1, how='all').ffill().bfill().pct_change().dropna(how='all')
        returns_grup2 = df_grup2.dropna(axis=1, how='all').ffill().bfill().pct_change().dropna(how='all')
        returns_grup1, returns_grup2 = returns_grup1.align(returns_grup2, join='inner', axis=0)

        if not returns_grup1.empty and not returns_grup2.empty:
            # Jika membandingkan dua dataset yang persis sama (termasuk filter MI-nya sama)
            if grup1 == grup2 and filter_mi1 == filter_mi2:
                title_suffix = filter_mi1 if filter_mi1 != "Semua" else ""
                title = f"Matriks Korelasi Internal {grup1} {title_suffix}".strip()

                corr_matrix = returns_grup1.corr()
                mask_plot = np.triu(np.ones(corr_matrix.shape), k=1).astype(bool)
                corr_matrix_plot = corr_matrix.mask(mask_plot)
            else:
                title_y = f"{filter_mi1} {grup1}" if filter_mi1 != "Semua" else grup1
                title_x = f"{filter_mi2} {grup2}" if filter_mi2 != "Semua" else grup2
                title = f"Matriks Korelasi: {title_y} vs {title_x}"

                corr_dict = {}
                for col2 in returns_grup2.columns:
                    corr_dict[col2] = returns_grup1.apply(lambda x: x.corr(returns_grup2[col2]))
                corr_matrix = pd.DataFrame(corr_dict)
                corr_matrix_plot = corr_matrix.copy()

            fig_corr = px.imshow(
                corr_matrix_plot, text_auto='.2f', aspect="auto",
                color_continuous_scale='RdYlGn', zmin=-1, zmax=1, title=title,
                labels=dict(y=f"Sumbu Y", x=f"Sumbu X", color="Korelasi")
            )
            fig_corr.update_layout(height=800)
            st.plotly_chart(fig_corr, use_container_width=True)

            corr_matrix.index.name = 'Asset_1'
            corr_matrix.columns.name = 'Asset_2'

            if grup1 == grup2 and filter_mi1 == filter_mi2:
                mask_table = np.triu(np.ones(corr_matrix.shape), k=1).astype(bool)
                corr_long = corr_matrix.where(mask_table).stack().reset_index()
            else:
                corr_long = corr_matrix.stack().reset_index()

            corr_long.columns = ['Asset_1', 'Asset_2', 'Correlation']
            corr_long = corr_long.dropna()

            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Top 5 Korelasi Positif Tertinggi")
                top_corr = corr_long.sort_values('Correlation', ascending=False).head(5)
                st.dataframe(top_corr, hide_index=True, use_container_width=True)
            with col2:
                st.subheader("Top 5 Korelasi Terendah")
                bottom_corr = corr_long.sort_values('Correlation', ascending=True).head(5)
                st.dataframe(bottom_corr, hide_index=True, use_container_width=True)
        else:
            st.warning("Data tidak cukup atau filter MI tidak menemukan instrumen yang relevan pada grup yang dipilih.")

