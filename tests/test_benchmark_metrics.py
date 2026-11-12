import contextlib
import io
import json
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "benchmarks")))
import load_test
from load_test import DEFAULT_URL, percentile, positive_int, positive_float


class _FakeResponse:
    status = 202

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self):
        return b'{"status": "published"}'


def run_benchmark(argv):
    with (
        mock.patch.object(load_test, "urlopen", return_value=_FakeResponse()),
        mock.patch.object(sys, "argv", ["load_test.py", *argv]),
        contextlib.redirect_stdout(io.StringIO()) as stdout,
    ):
        exit_code = load_test.main()
    return exit_code, json.loads(stdout.getvalue())


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


class TestBenchmarkTargetUrl(unittest.TestCase):
    def test_default_url_uses_ipv4_loopback(self):
        self.assertEqual(DEFAULT_URL, "http://127.0.0.1:8000/api/v1/events")

    def test_default_url_is_used_when_url_is_omitted(self):
        exit_code, report = run_benchmark(["--rate", "1", "--duration", "1"])
        self.assertEqual(exit_code, 0)
        self.assertEqual(report["url"], DEFAULT_URL)

    def test_url_argument_overrides_default(self):
        custom_url = "http://example.test:9000/api/v1/events"
        exit_code, report = run_benchmark(
            ["--url", custom_url, "--rate", "1", "--duration", "1"]
        )
        self.assertEqual(exit_code, 0)
        self.assertEqual(report["url"], custom_url)


if __name__ == "__main__":
    unittest.main()
