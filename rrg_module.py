"""
Relative Rotation Graph (RRG) Module for Mutual Funds

Formula:
1. FundIndex = NAV_t / NAV_awal * 100
2. BenchmarkIndex = Benchmark_t / Benchmark_awal * 100
3. RS = FundIndex / BenchmarkIndex * 100
4. RS_Ratio = RS / SMA(RS, ratio_window) * 100
5. RS_Momentum = RS_Ratio / SMA(RS_Ratio, momentum_window) * 100

Quadrant Classification (based on center line = 100):
- Leading: RS_Ratio >= 100 AND RS_Momentum >= 100
- Weakening: RS_Ratio >= 100 AND RS_Momentum < 100
- Improving: RS_Ratio < 100 AND RS_Momentum >= 100
- Lagging: RS_Ratio < 100 AND RS_Momentum < 100
"""

import warnings

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd

warnings.filterwarnings('ignore')


def calculate_rrg(df, benchmark_col_or_series, fund_cols, ratio_window=20, momentum_window=10):
    """
    Calculate RRG metrics for mutual funds.

    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame with Date index and columns for funds.
        Format: Date as index, columns = [fund1, fund2, ...]
    benchmark_col_or_series : str or pd.Series
        Either:
        - Name of benchmark column in df, OR
        - pd.Series with Date index containing benchmark values
    fund_cols : list
        List of fund column names (must exist in df.columns)
    ratio_window : int
        Window for RS_Ratio calculation (default: 20)
    momentum_window : int
        Window for RS_Momentum calculation (default: 10)

    Returns:
    --------
    dict : Dictionary with keys 'current' (DataFrame) and 'trailing' (DataFrame)
           Each contains: FundName, RS_Ratio, RS_Momentum, Quadrant
    """
    # Validate inputs
    if df.empty:
        return {'current': pd.DataFrame(), 'trailing': pd.DataFrame(), 'last_date': None}

    if not fund_cols:
        raise ValueError("fund_cols cannot be empty")
    if ratio_window < 2 or momentum_window < 2:
        raise ValueError("ratio_window and momentum_window must be at least 2")

    # Handle benchmark - could be column name or Series
    if isinstance(benchmark_col_or_series, str):
        if benchmark_col_or_series not in df.columns:
            raise ValueError(f"Benchmark column '{benchmark_col_or_series}' not found in DataFrame")
        benchmark = df[benchmark_col_or_series]
    else:
        benchmark = benchmark_col_or_series

    # Filter valid fund columns
    valid_fund_cols = [col for col in fund_cols if col in df.columns]
    if not valid_fund_cols:
        raise ValueError("None of the fund columns found in DataFrame")

    # Sort by date ascending and keep the latest value for duplicate dates.
    df = df.sort_index()
    df = df[~df.index.duplicated(keep='last')]
    benchmark = pd.to_numeric(benchmark, errors='coerce')
    benchmark = benchmark.replace([np.inf, -np.inf], np.nan)
    benchmark = benchmark.where(benchmark > 0).sort_index()
    benchmark = benchmark[~benchmark.index.duplicated(keep='last')].dropna()

    # Ensure numeric data
    df = df.apply(pd.to_numeric, errors='coerce').replace([np.inf, -np.inf], np.nan)
    df = df.where(df > 0)

    if benchmark.empty:
        return {'current': pd.DataFrame(), 'trailing': pd.DataFrame(), 'last_date': df.index[-1] if not df.empty else None}

    results = []
    trailing_results = []

    for fund_col in valid_fund_cols:
        # Pertahankan tanggal NAV aktual. Benchmark di-forward-fill ke tanggal NAV
        # agar fund yang belum memperbarui NAV tidak terlihat seolah-olah bergerak.
        fund_data = df[fund_col].dropna()
        combined_index = benchmark.index.union(fund_data.index).sort_values()
        benchmark_on_fund_dates = benchmark.reindex(combined_index).ffill().reindex(fund_data.index)
        aligned = pd.concat(
            [fund_data.rename('Fund'), benchmark_on_fund_dates.rename('Benchmark')],
            axis=1,
        ).dropna()

        minimum_observations = ratio_window + momentum_window - 1
        if len(aligned) < minimum_observations:
            continue

        fund_common = aligned['Fund']
        bench_common = aligned['Benchmark']

        # Step 1: Normalize NAV (FundIndex = NAV_t / NAV_awal * 100)
        fund_index = (fund_common / fund_common.iloc[0]) * 100

        # Step 2: Normalize benchmark (BenchmarkIndex = Benchmark_t / Benchmark_awal * 100)
        bench_index = (bench_common / bench_common.iloc[0]) * 100

        # Step 3: Calculate Relative Strength (RS = FundIndex / BenchmarkIndex * 100)
        rs = (fund_index / bench_index) * 100

        # Step 4: Calculate RS_Ratio (RS_Ratio = RS / SMA(RS, ratio_window) * 100)
        rs_sma = rs.rolling(window=ratio_window, min_periods=ratio_window).mean()
        rs_ratio = (rs / rs_sma) * 100

        # Step 5: Calculate RS_Momentum (RS_Momentum = RS_Ratio / SMA(RS_Ratio, momentum_window) * 100)
        rs_ratio_sma = rs_ratio.rolling(window=momentum_window, min_periods=momentum_window).mean()
        rs_momentum = (rs_ratio / rs_ratio_sma) * 100

        # Drop NaN values
        valid_mask = rs_ratio.notna() & rs_momentum.notna()
        rs_ratio = rs_ratio[valid_mask]
        rs_momentum = rs_momentum[valid_mask]

        if rs_ratio.empty or rs_momentum.empty:
            continue

        # Store trailing history (all periods)
        for i in range(len(rs_ratio)):
            trailing_results.append({
                'FundName': fund_col,
                'Date': rs_ratio.index[i],
                'RS_Ratio': rs_ratio.iloc[i],
                'RS_Momentum': rs_momentum.iloc[i],
                'Quadrant': classify_quadrant(rs_ratio.iloc[i], rs_momentum.iloc[i])
            })

        # Store current (last) values
        results.append({
            'FundName': fund_col,
            'Date': rs_ratio.index[-1],
            'RS_Ratio': rs_ratio.iloc[-1],
            'RS_Momentum': rs_momentum.iloc[-1],
            'Quadrant': classify_quadrant(rs_ratio.iloc[-1], rs_momentum.iloc[-1])
        })

    if not results:
        return {'current': pd.DataFrame(), 'trailing': pd.DataFrame(), 'last_date': df.index[-1] if not df.empty else None}

    current_df = pd.DataFrame(results)
    trailing_df = pd.DataFrame(trailing_results) if trailing_results else pd.DataFrame()

    return {
        'current': current_df,
        'trailing': trailing_df,
        'last_date': current_df['Date'].max()
    }


def classify_quadrant(rs_ratio, rs_momentum, center=100):
    """
    Classify a fund into a quadrant based on RS_Ratio and RS_Momentum.

    Parameters:
    -----------
    rs_ratio : float
        RS_Ratio value
    rs_momentum : float
        RS_Momentum value
    center : float
        Center line value (default: 100)

    Returns:
    --------
    str : Quadrant name ('Leading', 'Weakening', 'Improving', or 'Lagging')
    """
    if rs_ratio >= center and rs_momentum >= center:
        return "Leading"
    elif rs_ratio >= center and rs_momentum < center:
        return "Weakening"
    elif rs_ratio < center and rs_momentum >= center:
        return "Improving"
    else:
        return "Lagging"


def plot_rrg(rrg_data, trail_length=5, title=None, figsize=(12, 10), show_labels=True, show_trails=True):
    """
    Plot RRG chart with 4 colored quadrants, trails, and labels.

    Parameters:
    -----------
    rrg_data : dict
        Output from calculate_rrg() function
    trail_length : int
        Number of trailing periods to show (default: 5)
    title : str
        Chart title (default: auto-generated)
    figsize : tuple
        Figure size (default: (12, 10))
    show_labels : bool
        Whether to show fund labels (default: True)
    show_trails : bool
        Whether to show trails (default: True)

    Returns:
    --------
    matplotlib.figure.Figure
    """
    current_df = rrg_data.get('current', pd.DataFrame())
    trailing_df = rrg_data.get('trailing', pd.DataFrame())
    last_date = rrg_data.get('last_date')

    if current_df.empty:
        fig, ax = plt.subplots(figsize=figsize)
        ax.text(0.5, 0.5, 'No data available for RRG chart',
                ha='center', va='center', transform=ax.transAxes)
        return fig

    # Create figure
    fig, ax = plt.subplots(figsize=figsize)

    # Generate unique colors for each fund using a colormap
    n_funds = len(current_df)
    cmap = plt.colormaps.get_cmap('tab20') if n_funds <= 20 else plt.colormaps.get_cmap('hsv')
    fund_colors = {row['FundName']: cmap(i / max(n_funds - 1, 1)) for i, (_, row) in enumerate(current_df.iterrows())}

    visible_trails = []
    if show_trails and not trailing_df.empty:
        for fund_name in current_df['FundName']:
            fund_trailing = trailing_df[trailing_df['FundName'] == fund_name]
            if 'Date' in fund_trailing.columns:
                fund_trailing = fund_trailing.sort_values('Date')
            visible_trails.append(fund_trailing.tail(trail_length))

    visible_trailing_df = (
        pd.concat(visible_trails, ignore_index=True)
        if visible_trails
        else pd.DataFrame(columns=['RS_Ratio', 'RS_Momentum'])
    )

    # Calculate axis limits with padding, including the visible trails.
    all_rs_ratio = current_df['RS_Ratio'].to_numpy(dtype=float)
    all_rs_momentum = current_df['RS_Momentum'].to_numpy(dtype=float)
    if not visible_trailing_df.empty:
        all_rs_ratio = np.concatenate(
            [all_rs_ratio, visible_trailing_df['RS_Ratio'].to_numpy(dtype=float)]
        )
        all_rs_momentum = np.concatenate(
            [all_rs_momentum, visible_trailing_df['RS_Momentum'].to_numpy(dtype=float)]
        )

    # Include center line in range
    all_values = np.concatenate([all_rs_ratio, all_rs_momentum, [100]])
    data_min = np.nanmin(all_values)
    data_max = np.nanmax(all_values)

    # Ensure center (100) is visible
    range_min = min(data_min, 95)
    range_max = max(data_max, 105)

    # Add padding
    padding = (range_max - range_min) * 0.1
    x_min, x_max = range_min - padding, range_max + padding
    y_min, y_max = range_min - padding, range_max + padding

    # Draw quadrant backgrounds
    ax.fill_betweenx([100, y_max], 100, x_max, alpha=0.3, color='#90EE90', label='_nolegend_')
    ax.fill_betweenx([100, y_max], x_min, 100, alpha=0.3, color='#ADD8E6', label='_nolegend_')
    ax.fill_betweenx([y_min, 100], 100, x_max, alpha=0.3, color='#FFFACD', label='_nolegend_')
    ax.fill_betweenx([y_min, 100], x_min, 100, alpha=0.3, color='#FFB6C1', label='_nolegend_')

    # Draw center lines
    ax.axhline(y=100, color='gray', linestyle='--', linewidth=1.5, zorder=1)
    ax.axvline(x=100, color='gray', linestyle='--', linewidth=1.5, zorder=1)

    # Add quadrant labels
    label_padding = 1.5
    ax.text(x_max - label_padding, y_max - label_padding, 'Leading',
            fontsize=14, fontweight='bold', color='darkgreen',
            ha='right', va='top', alpha=0.7)
    ax.text(x_min + label_padding, y_max - label_padding, 'Improving',
            fontsize=14, fontweight='bold', color='darkblue',
            ha='left', va='top', alpha=0.7)
    ax.text(x_max - label_padding, y_min + label_padding, 'Weakening',
            fontsize=14, fontweight='bold', color='darkorange',
            ha='right', va='bottom', alpha=0.7)
    ax.text(x_min + label_padding, y_min + label_padding, 'Lagging',
            fontsize=14, fontweight='bold', color='darkred',
            ha='left', va='bottom', alpha=0.7)

    # Plot trails if available
    if show_trails and not visible_trailing_df.empty:
        for fund_name in current_df['FundName']:
            fund_trailing = visible_trailing_df[
                visible_trailing_df['FundName'] == fund_name
            ]

            if len(fund_trailing) >= 2:
                rs_ratios = fund_trailing['RS_Ratio'].values
                rs_momentums = fund_trailing['RS_Momentum'].values
                color = fund_colors[fund_name]

                # Draw trail line
                ax.plot(rs_ratios, rs_momentums,
                       color=color, linewidth=1.5, alpha=0.6, zorder=2)
                ax.annotate(
                    '',
                    xy=(rs_ratios[-1], rs_momentums[-1]),
                    xytext=(rs_ratios[-2], rs_momentums[-2]),
                    arrowprops=dict(arrowstyle='->', color=color, alpha=0.8, lw=1.5),
                    zorder=3,
                )

    # Plot current points
    for _, row in current_df.iterrows():
        color = fund_colors[row['FundName']]

        # Draw point
        ax.scatter(row['RS_Ratio'], row['RS_Momentum'],
                  c=[color], s=100, zorder=3, edgecolors='black', linewidths=0.5)

        # Add label if enabled
        if show_labels:
            offset = 0.5
            ax.annotate(row['FundName'],
                       (row['RS_Ratio'], row['RS_Momentum']),
                       xytext=(offset, offset),
                       textcoords='offset points',
                       fontsize=8,
                       alpha=0.9)

    # Set axis limits
    ax.set_xlim(x_min, x_max)
    ax.set_ylim(y_min, y_max)

    # Labels and title
    ax.set_xlabel('RS Ratio', fontsize=12)
    ax.set_ylabel('RS Momentum', fontsize=12)

    if title is None:
        date_str = last_date.strftime('%d %b %Y') if last_date else ''
        title = f'RRG - Reksadana - {date_str}'

    ax.set_title(title, fontsize=14, fontweight='bold', pad=10)

    # Grid
    ax.grid(True, alpha=0.3, linestyle=':')
    ax.set_axisbelow(True)

    # Create legend with each fund's color
    legend_elements = [
        mpatches.Patch(facecolor=fund_colors[fund], edgecolor='gray', label=fund)
        for fund in current_df['FundName']
    ]
    ax.legend(handles=legend_elements, loc='upper left', fontsize=9)

    plt.tight_layout()
    return fig


# ============================================================
# EXAMPLE USAGE WITH DUMMY DATA
# ============================================================

def create_dummy_data():
    """
    Create dummy data for testing RRG functions.
    """
    import pandas as pd
    import numpy as np

    np.random.seed(42)

    # Date range (250 trading days = ~1 year)
    dates = pd.date_range(start='2024-01-01', periods=250, freq='B')

    # Generate benchmark (random walk with trend)
    benchmark = 100 * np.cumprod(1 + np.random.normal(0.0003, 0.01, len(dates)))

    # Generate 10 funds with different characteristics
    funds = {}
    fund_names = ['Fund A', 'Fund B', 'Fund C', 'Fund D', 'Fund E',
                  'Fund F', 'Fund G', 'Fund H', 'Fund I', 'Fund J']

    for i, name in enumerate(fund_names):
        # Different volatility and trend for each fund
        trend = np.random.uniform(-0.0001, 0.0005)
        volatility = np.random.uniform(0.008, 0.015)

        returns = np.random.normal(trend, volatility, len(dates))
        nav = 100 * np.cumprod(1 + returns)
        funds[name] = nav

    # Create DataFrame
    df = pd.DataFrame({'Benchmark': benchmark, **funds}, index=dates)
    df.index.name = 'Date'

    return df


if __name__ == "__main__":
    # Create dummy data
    print("Creating dummy data...")
    df = create_dummy_data()

    print(f"Data shape: {df.shape}")
    print(f"Date range: {df.index[0].date()} to {df.index[-1].date()}")
    print(f"Columns: {df.columns.tolist()}")

    # Define benchmark and fund columns
    benchmark_col = 'Benchmark'
    fund_cols = [col for col in df.columns if col != benchmark_col]

    print(f"\nBenchmark: {benchmark_col}")
    print(f"Number of funds: {len(fund_cols)}")

    # Calculate RRG
    print("\nCalculating RRG metrics...")
    rrg_result = calculate_rrg(
        df,
        benchmark_col=benchmark_col,
        fund_cols=fund_cols,
        ratio_window=20,
        momentum_window=10
    )

    print(f"\nCurrent RRG data:")
    print(rrg_result['current'].to_string(index=False))

    # Plot RRG
    print("\nGenerating RRG chart...")
    fig = plot_rrg(
        rrg_result,
        trail_length=10,
        title="RRG - Reksadana - Contoh",
        show_labels=True,
        show_trails=True
    )

    # Save chart
    fig.savefig('rrg_example.png', dpi=150, bbox_inches='tight')
    print("Chart saved as 'rrg_example.png'")

    plt.show()
