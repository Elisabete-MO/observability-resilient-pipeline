import os
import re
import unittest

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def read_project_file(*parts):
    with open(os.path.join(ROOT_DIR, *parts), encoding="utf-8") as handle:
        return handle.read()


class TestComposeConfig(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.compose = read_project_file("compose.yaml")
        cls.errors_scenario = read_project_file(
            "benchmarks", "scenarios", "simulate_errors.sh"
        )

    def test_processing_error_rate_is_interpolated_from_host(self):
        self.assertIn(
            'PROCESSING_ERROR_RATE: "${PROCESSING_ERROR_RATE:-0}"',
            self.compose,
        )
        self.assertNotIn('PROCESSING_ERROR_RATE: "0"', self.compose)

    def test_processing_delay_is_interpolated_from_host(self):
        self.assertIn(
            'PROCESSING_MIN_MS: "${PROCESSING_MIN_MS:-50}"',
            self.compose,
        )
        self.assertIn(
            'PROCESSING_MAX_MS: "${PROCESSING_MAX_MS:-200}"',
            self.compose,
        )

    def test_error_scenario_recreates_and_verifies_applied_rate(self):
        self.assertIn(
            'PROCESSING_ERROR_RATE="0.20" docker compose up -d '
            "--no-deps --scale consumer=1 --force-recreate consumer",
            self.errors_scenario,
        )
        self.assertIn(
            'PROCESSING_ERROR_RATE="0" docker compose up -d '
            "--no-deps --scale consumer=1 --force-recreate consumer",
            self.errors_scenario,
        )
        self.assertIn("printenv PROCESSING_ERROR_RATE", self.errors_scenario)
        self.assertIn('"state":"firing"', self.errors_scenario)

    def test_error_scenario_waits_full_two_minute_window_before_recovery(self):
        durations = [
            int(value)
            for value in re.findall(r"--duration (\d+)", self.errors_scenario)
        ]
        self.assertTrue(durations)
        self.assertGreaterEqual(max(durations), 120)
        self.assertGreaterEqual(max(durations[:-1] or [0]), 60)

    def test_kafka_exporter_filters_match_required_topic_and_group(self):
        self.assertIn("--topic.filter=^raw-events$$", self.compose)
        self.assertIn("--group.filter=^events-processor$$", self.compose)


if __name__ == "__main__":
    unittest.main()
