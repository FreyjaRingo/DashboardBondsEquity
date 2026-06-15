"""
RRG Streamlit Page
Standalone page for Relative Rotation Graph analysis
"""

import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rrg_module import calculate_rrg, classify_quadrant, plot_rrg

# Page configuration
st.set_page_config(
    page_title="RRG Analysis",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Relative Rotation Graph (RRG) Analysis")
st.markdown("---")


def get_available_data():
    """
    Get available data from the dashboard data source.
    This function should be modified based on your actual data source.
    """
    # Placeholder - replace with your actual data loading logic
    # Example: from dashboard_core.data import load_data
    return None


def main():
    # Sidebar controls
    st.sidebar.header("⚙️ RRG Settings")

    # Get data source
    st.sidebar.subheader("📁 Data Source")

    # Option to use sample data or real data
    use_sample = st.sidebar.checkbox("Use Sample Data", value=True)

    if use_sample:
        # Generate sample data
        np.random.seed(42)
        dates = pd.date_range(start='2024-01-01', periods=250, freq='B')
        benchmark = 100 * np.cumprod(1 + np.random.normal(0.0003, 0.01, len(dates)))

        fund_names = ['OSO', 'Sukma Bangsa', 'Dana Maksima', 'Sentra Ekuitas',
                      'Prestasi Optimal', 'Milenia Optimal', 'RDPT Dinamis', 'Syailendra',
                      'Trimegah', 'Batavia Saham']

        funds = {}
        for i, name in enumerate(fund_names):
            trend = np.random.uniform(-0.0001, 0.0005)
            volatility = np.random.uniform(0.008, 0.015)
            returns = np.random.normal(trend, volatility, len(dates))
            funds[name] = 100 * np.cumprod(1 + returns)

        df = pd.DataFrame({'Benchmark': benchmark, **funds}, index=dates)
        df.index.name = 'Date'

        st.sidebar.success(f"Loaded sample data: {len(df)} rows, {len(df.columns)} columns")

        # Get all fund columns
        all_columns = df.columns.tolist()
        benchmark_col = 'Benchmark'
        fund_options = [col for col in all_columns if col != benchmark_col]
        fund_cols = st.sidebar.multiselect(
            "Select Funds",
            options=fund_options,
            default=fund_options[:10],
        )
    else:
        # Try to load from dashboard
        data = get_available_data()
        if data is None:
            st.error("No data available. Please use sample data or configure data source.")
            return

        df = data
        all_columns = df.columns.tolist()

        # Let user select benchmark
        benchmark_col = st.sidebar.selectbox("Select Benchmark", all_columns)

        # Select fund columns (multi-select)
        fund_options = [col for col in all_columns if col != benchmark_col]
        fund_cols = st.sidebar.multiselect(
            "Select Funds",
            options=fund_options,
            default=fund_options[:10] if len(fund_options) > 10 else fund_options
        )

    # RRG Parameters
    st.sidebar.subheader("📈 Parameters")
    ratio_window = st.sidebar.slider("RS Ratio Window", 10, 60, 20, 5)
    momentum_window = st.sidebar.slider("RS Momentum Window", 5, 30, 10, 1)
    trail_length = st.sidebar.slider("Trail Length (periods)", 3, 20, 10, 1)

    # Display options
    st.sidebar.subheader("🎨 Display Options")
    show_labels = st.sidebar.checkbox("Show Labels", value=True)
    show_trails = st.sidebar.checkbox("Show Trails", value=True)
    show_table = st.sidebar.checkbox("Show Data Table", value=True)

    # Calculate RRG
    if fund_cols:
        with st.spinner("Calculating RRG metrics..."):
            try:
                rrg_result = calculate_rrg(
                    df,
                    benchmark_col_or_series=benchmark_col,
                    fund_cols=fund_cols,
                    ratio_window=ratio_window,
                    momentum_window=momentum_window
                )

                if rrg_result['current'].empty:
                    st.error("Not enough data to calculate RRG. Please ensure sufficient historical data.")
                    return

                # Main content
                col1, col2 = st.columns([3, 1])

                with col1:
                    # Plot RRG
                    fig = plot_rrg(
                        rrg_result,
                        trail_length=trail_length,
                        title=f"RRG - Reksadana - {rrg_result.get('last_date', 'N/A').strftime('%d %b %Y') if rrg_result.get('last_date') else 'N/A'}",
                        show_labels=show_labels,
                        show_trails=show_trails,
                        figsize=(12, 10)
                    )
                    st.pyplot(fig, clear_figure=True)

                with col2:
                    # Summary by quadrant
                    st.subheader("📊 Summary by Quadrant")

                    current_df = rrg_result['current']
                    quadrant_counts = current_df['Quadrant'].value_counts()

                    for quadrant in ['Leading', 'Improving', 'Weakening', 'Lagging']:
                        count = quadrant_counts.get(quadrant, 0)
                        if count > 0:
                            st.metric(quadrant, count)

                    # List funds by quadrant
                    st.markdown("---")
                    st.subheader("🎯 Funds by Quadrant")

                    for quadrant in ['Leading', 'Improving', 'Weakening', 'Lagging']:
                        funds_in_quad = current_df[current_df['Quadrant'] == quadrant]['FundName'].tolist()
                        if funds_in_quad:
                            color_map = {
                                'Leading': '🟢',
                                'Improving': '🔵',
                                'Weakening': '🟡',
                                'Lagging': '🔴'
                            }
                            st.markdown(f"**{color_map[quadrant]} {quadrant}**")
                            for fund in funds_in_quad:
                                st.markdown(f"  - {fund}")

                # Data Table
                if show_table:
                    st.markdown("---")
                    st.subheader("📋 RRG Data Table")

                    table_df = current_df.copy()
                    table_df = table_df.sort_values('Quadrant')

                    # Format for display
                    display_df = table_df[['FundName', 'RS_Ratio', 'RS_Momentum', 'Quadrant']].copy()
                    display_df['RS_Ratio'] = display_df['RS_Ratio'].round(2)
                    display_df['RS_Momentum'] = display_df['RS_Momentum'].round(2)

                    # Add change columns if trailing data available
                    if not rrg_result['trailing'].empty:
                        trailing_df = rrg_result['trailing']

                        changes = []
                        for fund in table_df['FundName']:
                            fund_trailing = trailing_df[trailing_df['FundName'] == fund].tail(2)
                            if len(fund_trailing) >= 2:
                                rs_ratio_change = fund_trailing['RS_Ratio'].iloc[-1] - fund_trailing['RS_Ratio'].iloc[-2]
                                rs_mom_change = fund_trailing['RS_Momentum'].iloc[-1] - fund_trailing['RS_Momentum'].iloc[-2]
                            else:
                                rs_ratio_change = np.nan
                                rs_mom_change = np.nan
                            changes.append({
                                'RS_Ratio_Change': rs_ratio_change,
                                'RS_Momentum_Change': rs_mom_change
                            })

                        change_df = pd.DataFrame(changes)
                        display_df['RS_Ratio_Change'] = change_df['RS_Ratio_Change'].round(2)
                        display_df['RS_Momentum_Change'] = change_df['RS_Momentum_Change'].round(2)

                    st.dataframe(
                        display_df,
                        use_container_width=True,
                        hide_index=True
                    )

                # Detailed view per quadrant
                st.markdown("---")
                st.subheader("🔍 Detailed Quadrant View")

                tab1, tab2, tab3, tab4 = st.tabs(['🟢 Leading', '🔵 Improving', '🟡 Weakening', '🔴 Lagging'])

                for tab, quadrant in zip([tab1, tab2, tab3, tab4], ['Leading', 'Improving', 'Weakening', 'Lagging']):
                    with tab:
                        quad_funds = current_df[current_df['Quadrant'] == quadrant]
                        if not quad_funds.empty:
                            for _, row in quad_funds.iterrows():
                                with st.expander(f"**{row['FundName']}** - {row['Quadrant']}"):
                                    col_a, col_b = st.columns(2)
                                    with col_a:
                                        st.metric("RS Ratio", f"{row['RS_Ratio']:.2f}")
                                    with col_b:
                                        st.metric("RS Momentum", f"{row['RS_Momentum']:.2f}")

                                    # Mini chart for this fund
                                    if not rrg_result['trailing'].empty:
                                        fund_trailing = rrg_result['trailing'][
                                            rrg_result['trailing']['FundName'] == row['FundName']
                                        ].tail(trail_length)

                                        if not fund_trailing.empty:
                                            fig_mini, ax = plt.subplots(figsize=(6, 4))
                                            ax.plot(fund_trailing['RS_Ratio'].values,
                                                   fund_trailing['RS_Momentum'].values,
                                                   'o-', color='blue', markersize=6)
                                            ax.axhline(y=100, color='gray', linestyle='--', alpha=0.5)
                                            ax.axvline(x=100, color='gray', linestyle='--', alpha=0.5)
                                            ax.set_xlabel('RS Ratio')
                                            ax.set_ylabel('RS Momentum')
                                            ax.set_title(f'{row["FundName"]} - Trail')
                                            ax.grid(True, alpha=0.3)
                                            st.pyplot(fig_mini)
                                        plt.close('all')
                        else:
                            st.info(f"No funds in {quadrant} quadrant")

            except Exception as e:
                st.error(f"Error calculating RRG: {str(e)}")
                import traceback
                st.code(traceback.format_exc())
    else:
        st.warning("Please select at least one fund column.")


if __name__ == "__main__":
    main()
