"""Unit test kalkulasi metrik reksa dana (dashboard_core/metrics.py).

Jalankan: python -m unittest discover tests   (atau: python -m pytest tests)
"""

import unittest

import numpy as np
import pandas as pd

from dashboard_core.metrics import calculate_metrics, calculate_ranking_scores


def make_prices(n_days=400, n_funds=6, seed=42):
    np.random.seed(seed)
    dates = pd.date_range("2023-01-02", periods=n_days, freq="B")
    data = {
        f"Fund {chr(65 + i)}": 1000 * np.cumprod(1 + np.random.normal(0.0004, 0.009, n_days))
        for i in range(n_funds)
    }
    bench = pd.Series(
        7000 * np.cumprod(1 + np.random.normal(0.0003, 0.008, n_days)),
        index=dates, name=".JKSE",
    )
    return pd.DataFrame(data, index=dates), bench


class TestCalculateMetrics(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.prices, cls.bench = make_prices()
        cls.metrics = calculate_metrics(
            cls.prices, cls.bench, risk_free_rate=0.065,
            eval_window=252, young_funds_list=tuple(), bench_ticker=".JKSE",
        )

    def test_output_shape(self):
        self.assertIsNotNone(self.metrics)
        self.assertEqual(len(self.metrics), self.prices.shape[1])
        for col in ["Return_1W", "Return_1M", "Return_3M", "Volatility",
                    "Sharpe_Ratio", "Beta", "Alpha", "Z_Score"]:
            self.assertIn(col, self.metrics.columns, f"kolom {col} hilang")

    def test_return_1m_matches_manual(self):
        s = self.prices["Fund A"]
        manual = s.iloc[-1] / s.iloc[-23] - 1  # get_period_return(df, 22)
        self.assertAlmostEqual(self.metrics.loc["Fund A", "Return_1M"], manual, places=10)

    def test_return_1w_matches_manual(self):
        s = self.prices["Fund B"]
        manual = s.iloc[-1] / s.iloc[-6] - 1  # get_period_return(df, 5)
        self.assertAlmostEqual(self.metrics.loc["Fund B", "Return_1W"], manual, places=10)

    def test_volatility_annualized(self):
        rets = self.prices.tail(252).pct_change()
        manual = rets["Fund A"].std() * np.sqrt(252)
        self.assertAlmostEqual(self.metrics.loc["Fund A", "Volatility"], manual, places=6)

    def test_beta_reasonable(self):
        # Fund acak vs benchmark acak independen -> beta dekat 0, dan bukan NaN
        betas = self.metrics["Beta"].dropna()
        self.assertEqual(len(betas), self.prices.shape[1])
        self.assertTrue((betas.abs() < 1.5).all())


class TestRankingScores(unittest.TestCase):
    def test_ranking_produces_rank(self):
        prices, bench = make_prices(seed=7)
        metrics = calculate_metrics(prices, bench, 0.065, eval_window=252,
                                    young_funds_list=tuple(), bench_ticker=".JKSE")
        ranked = calculate_ranking_scores(metrics, weights=None, young_funds_list=tuple())
        self.assertFalse(ranked.empty)
        self.assertEqual(len(ranked), len(metrics))

    def test_custom_weights_return_focus(self):
        prices, bench = make_prices(seed=9)
        metrics = calculate_metrics(prices, bench, 0.065, eval_window=252,
                                    young_funds_list=tuple(), bench_ticker=".JKSE")
        w = 1.0 / 3.0
        ranked = calculate_ranking_scores(
            metrics, weights={"Return_1W": w, "Return_1M": w, "Return_3M": w},
            young_funds_list=tuple(),
        )
        self.assertFalse(ranked.empty)


if __name__ == "__main__":
    unittest.main()
