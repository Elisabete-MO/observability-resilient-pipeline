import unittest
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "benchmarks")))
from load_test import percentile, positive_int, positive_float


class TestBenchmarkMetrics(unittest.TestCase):
    def test_percentile_empty(self):
        self.assertIsNone(percentile([], 0.95))

    def test_percentile_calculations(self):
        # 100 valores de 0.001s a 0.100s
        latencies = [i * 0.001 for i in range(1, 101)]
        p50 = percentile(latencies, 0.50)
        p95 = percentile(latencies, 0.95)
        p99 = percentile(latencies, 0.99)

        # Esperado: p50 ~ 50ms, p95 ~ 95ms, p99 ~ 99ms
        self.assertAlmostEqual(p50, 50.0, places=1)
        self.assertAlmostEqual(p95, 95.0, places=1)
        self.assertAlmostEqual(p99, 99.0, places=1)

    def test_positive_converters(self):
        self.assertEqual(positive_int("10"), 10)
        with self.assertRaises(Exception):
            positive_int("0")
        with self.assertRaises(Exception):
            positive_int("-5")

        self.assertEqual(positive_float("1.5"), 1.5)
        with self.assertRaises(Exception):
            positive_float("-0.1")


if __name__ == "__main__":
    unittest.main()
