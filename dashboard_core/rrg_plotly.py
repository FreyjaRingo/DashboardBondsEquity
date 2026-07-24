"""Port RRG ke Plotly (interaktif: hover, zoom, tail).

Memakai output calculate_rrg() dari rrg_module.py (tidak menduplikasi kalkulasi).
"""

import numpy as np
import pandas as pd
import plotly.graph_objects as go

QUADRANT_COLORS = {
    "Leading": "#90EE90",
    "Improving": "#ADD8E6",
    "Weakening": "#FFFACD",
    "Lagging": "#FFB6C1",
}

_PALETTE = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b",
    "#e377c2", "#7f7f7f", "#bcbd22", "#17becf", "#aec7e8", "#ffbb78",
    "#98df8a", "#ff9896", "#c5b0d5",
]


def plot_rrg_plotly(rrg_data, trail_length=8, title=None, show_labels=True, height=700):
    """Bangun figure Plotly dari output calculate_rrg()."""
    current = rrg_data.get("current", pd.DataFrame())
    trailing = rrg_data.get("trailing", pd.DataFrame())
    last_date = rrg_data.get("last_date")

    fig = go.Figure()
    if current.empty:
        fig.add_annotation(text="Data tidak cukup untuk RRG", showarrow=False, font=dict(size=16))
        return fig

    # Rentang sumbu (ikutkan tail yang terlihat)
    vals = [current["RS_Ratio"].to_numpy(float), current["RS_Momentum"].to_numpy(float), np.array([100.0])]
    tails = {}
    if not trailing.empty:
        for name in current["FundName"]:
            t = trailing[trailing["FundName"] == name].sort_values("Date").tail(trail_length)
            tails[name] = t
            if not t.empty:
                vals.append(t["RS_Ratio"].to_numpy(float))
                vals.append(t["RS_Momentum"].to_numpy(float))
    allv = np.concatenate(vals)
    lo = min(np.nanmin(allv), 95.0)
    hi = max(np.nanmax(allv), 105.0)
    pad = (hi - lo) * 0.1
    lo, hi = lo - pad, hi + pad

    # Latar 4 kuadran
    for x0, x1, y0, y1, q in [
        (100, hi, 100, hi, "Leading"),
        (lo, 100, 100, hi, "Improving"),
        (100, hi, lo, 100, "Weakening"),
        (lo, 100, lo, 100, "Lagging"),
    ]:
        fig.add_shape(type="rect", x0=x0, x1=x1, y0=y0, y1=y1,
                      fillcolor=QUADRANT_COLORS[q], opacity=0.30, line_width=0, layer="below")
    fig.add_hline(y=100, line_dash="dash", line_color="gray")
    fig.add_vline(x=100, line_dash="dash", line_color="gray")

    off = (hi - lo) * 0.02
    for text, x, y, ax_, ay_, color in [
        ("Leading", hi - off, hi - off, "right", "top", "darkgreen"),
        ("Improving", lo + off, hi - off, "left", "top", "darkblue"),
        ("Weakening", hi - off, lo + off, "right", "bottom", "darkorange"),
        ("Lagging", lo + off, lo + off, "left", "bottom", "darkred"),
    ]:
        fig.add_annotation(x=x, y=y, text=f"<b>{text}</b>", showarrow=False,
                           xanchor=ax_, yanchor=ay_, font=dict(size=14, color=color), opacity=0.7)

    # Tail + titik terkini per instrumen
    for i, (_, row) in enumerate(current.iterrows()):
        name = row["FundName"]
        color = _PALETTE[i % len(_PALETTE)]
        t = tails.get(name, pd.DataFrame())

        if len(t) >= 2:
            fig.add_trace(go.Scatter(
                x=t["RS_Ratio"], y=t["RS_Momentum"],
                mode="lines+markers", line=dict(color=color, width=1.5),
                marker=dict(size=5, color=color), opacity=0.55,
                name=name, legendgroup=name, showlegend=False,
                hovertemplate=(f"<b>{name}</b><br>%{{customdata}}<br>"
                               "RS-Ratio: %{x:.2f}<br>RS-Momentum: %{y:.2f}<extra></extra>"),
                customdata=t["Date"].dt.strftime("%d %b %Y") if "Date" in t.columns else None,
            ))
            # Panah arah rotasi
            fig.add_annotation(
                x=t["RS_Ratio"].iloc[-1], y=t["RS_Momentum"].iloc[-1],
                ax=t["RS_Ratio"].iloc[-2], ay=t["RS_Momentum"].iloc[-2],
                xref="x", yref="y", axref="x", ayref="y",
                showarrow=True, arrowhead=2, arrowwidth=1.5, arrowcolor=color, text="",
            )

        fig.add_trace(go.Scatter(
            x=[row["RS_Ratio"]], y=[row["RS_Momentum"]],
            mode="markers+text" if show_labels else "markers",
            marker=dict(size=13, color=color, line=dict(color="black", width=1)),
            text=[name] if show_labels else None, textposition="top center",
            textfont=dict(size=10),
            name=name, legendgroup=name,
            hovertemplate=(f"<b>{name}</b> ({row['Quadrant']})<br>"
                           "RS-Ratio: %{x:.2f}<br>RS-Momentum: %{y:.2f}<extra></extra>"),
        ))

    if title is None:
        date_str = last_date.strftime("%d %b %Y") if last_date is not None else ""
        title = f"Relative Rotation Graph — {date_str}"

    fig.update_layout(
        title=title, height=height,
        xaxis=dict(title="RS-Ratio", range=[lo, hi], zeroline=False),
        yaxis=dict(title="RS-Momentum", range=[lo, hi], zeroline=False),
        legend=dict(orientation="v", x=1.02, y=1),
        margin=dict(l=40, r=40, t=60, b=40),
        plot_bgcolor="white",
    )
    return fig
