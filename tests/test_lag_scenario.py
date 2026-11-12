import os
import re
import unittest

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
THRESHOLD = 500
ALERT_FOR_SECONDS = 30


def read_project_file(*parts):
    with open(os.path.join(ROOT_DIR, *parts), encoding="utf-8") as handle:
        return handle.read()


class TestLagScenario(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.scenario = read_project_file(
            "benchmarks", "scenarios", "simulate_lag.sh"
        )
        cls.compose = read_project_file("compose.yaml")

        rate_match = re.search(r"--rate (\d+)", cls.scenario)
        duration_match = re.search(r"--duration (\d+)", cls.scenario)
        min_delay_match = re.search(
            r'PROCESSING_MIN_MS: "\$\{PROCESSING_MIN_MS:-(\d+)\}"',
            cls.compose,
        )

        if not (rate_match and duration_match and min_delay_match):
            raise AssertionError("Parametros do cenario de lag nao encontrados.")

        cls.rate = int(rate_match.group(1))
        cls.duration = int(duration_match.group(1))
        # Limitite otimista: menor atraso possivel do consumer.
        cls.max_consumer_rps = 1000 / int(min_delay_match.group(1))

    def test_load_exceeds_consumer_capacity(self):
        self.assertGreater(self.rate, self.max_consumer_rps)

    def test_lag_scenario_starts_without_error_injection(self):
        self.assertIn(
            'PROCESSING_ERROR_RATE="0" docker compose up -d --no-deps '
            "--scale consumer=1 --force-recreate consumer",
            self.scenario,
        )

    def test_lag_scenario_waits_for_firing_and_resolution(self):
        self.assertIn('"state":"firing"', self.scenario)
        self.assertIn(
            'grep -q \'"alertname":"HighConsumerLag"\'',
            self.scenario,
        )

    def test_lag_stays_above_threshold_longer_than_alert_window(self):
        lag_at_end = (
            self.rate - self.max_consumer_rps
        ) * self.duration

        self.assertGreater(lag_at_end, THRESHOLD)

        time_to_cross = THRESHOLD / (self.rate - self.max_consumer_rps)
        remaining_load = max(0, self.duration - time_to_cross)
        drain_below_threshold = (
            lag_at_end - THRESHOLD
        ) / self.max_consumer_rps
        seconds_above_threshold = remaining_load + drain_below_threshold

        self.assertGreater(
            seconds_above_threshold,
            ALERT_FOR_SECONDS,
            "O cenario precisa manter lag > 500 por mais de 30s "
            "mesmo no limite otimista do consumer.",
        )

    def test_lag_is_still_above_threshold_when_alert_is_checked(self):
        # O script verifica o alerta 40s apos o fim da carga.
        check_delay_seconds = 40
        lag_at_end = (
            self.rate - self.max_consumer_rps
        ) * self.duration
        lag_at_check = lag_at_end - self.max_consumer_rps * check_delay_seconds

        self.assertGreater(lag_at_check, THRESHOLD)


if __name__ == "__main__":
    unittest.main()
