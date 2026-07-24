"""Unit test metrik sektoral (dashboard_core/sector_metrics.py)."""

import unittest

import numpy as np
import pandas as pd

from dashboard_core.sector_metrics import (
    align_cross_market,
    composite_scores,
    multi_timeframe_returns,
    rank_delta,
    rebase,
    relative_returns,
    sma_badges,
)


def make_sector_prices(n_days=800, seed=0):
    np.random.seed(seed)
    dates = pd.date_range("2022-01-03", periods=n_days, freq="B")
    cols = [".SPX"] + [f"SEC{i}" for i in range(11)]
    data = {
        c: 100 * np.cumprod(1 + np.random.normal(0.0004 - 0.0002 * i / 11, 0.011, n_days))
        for i, c in enumerate(cols)
    }
    return pd.DataFrame(data, index=dates)


class TestSectorMetrics(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.px = make_sector_prices()

    def test_multi_tf_returns_manual(self):
        rets = multi_timeframe_returns(self.px)
        self.assertEqual(list(rets.columns), ["1D", "1W", "1M", "3M", "6M", "YTD", "12M"])
        s = self.px[".SPX"]
        self.assertAlmostEqual(rets.loc[".SPX", "1M"], s.iloc[-1] / s.iloc[-22] - 1, places=12)
        self.assertAlmostEqual(rets.loc[".SPX", "1D"], s.iloc[-1] / s.iloc[-2] - 1, places=12)

    def test_ytd_return(self):
        rets = multi_timeframe_returns(self.px)
        s = self.px["SEC0"]
        base = s[s.index < pd.Timestamp(s.index[-1].year, 1, 1)].iloc[-1]
        self.assertAlmostEqual(rets.loc["SEC0", "YTD"], s.iloc[-1] / base - 1, places=12)

    def test_relative_returns(self):
        rets = multi_timeframe_returns(self.px)
        rel = relative_returns(rets, ".SPX")
        self.assertNotIn(".SPX", rel.index)
        self.assertAlmostEqual(
            rel.loc["SEC0", "3M"],
            rets.loc["SEC0", "3M"] - rets.loc[".SPX", "3M"], places=12,
        )

    def test_composite_scores(self):
        sc = composite_scores(self.px, ".SPX")
        self.assertEqual(len(sc), 11)
        self.assertAlmostEqual(sc["Score"].mean(), 0.0, places=9)  # z-score
        self.assertEqual(int(sc["Rank"].min()), 1)
        self.assertTrue(sc["Rank"].is_monotonic_increasing)  # sudah tersortir

    def test_rank_delta(self):
        d = rank_delta(self.px, ".SPX", lookback_days=5)
        self.assertEqual(d.notna().sum(), 11)

    def test_rebase_starts_at_100(self):
        rb = rebase(self.px[["SEC0", "SEC1"]].tail(100))
        self.assertAlmostEqual(rb.iloc[0]["SEC0"], 100.0, places=9)
        self.assertAlmostEqual(rb.iloc[0]["SEC1"], 100.0, places=9)

    def test_align_cross_market_limited_ffill(self):
        a = self.px[["SEC0"]]
        b = self.px[["SEC1"]].iloc[:-20]  # market B berhenti 20 hari lebih awal
        comb = align_cross_market([a, b], max_ffill=5)
        tail = comb["SEC1"].tail(20)
        self.assertEqual(tail.notna().sum(), 5)  # ffill maks 5 hari, sisanya NaN

    def test_sma_badges_format(self):
        badges = sma_badges(self.px[["SEC0"]])
        self.assertRegex(badges["SEC0"], r"^[▲▼·]/[▲▼·]$")


if __name__ == "__main__":
    unittest.main()
