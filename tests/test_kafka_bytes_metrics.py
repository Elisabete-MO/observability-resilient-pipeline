import json
import os
import unittest

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def read_project_file(*parts):
    with open(os.path.join(ROOT_DIR, *parts), encoding="utf-8") as handle:
        return handle.read()


class TestKafkaBytesMetrics(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.compose = read_project_file("compose.yaml")
        cls.prometheus = read_project_file(
            "infra", "prometheus", "prometheus.yml"
        )
        cls.jmx_config = read_project_file(
            "infra", "jmx-exporter", "kafka-bytes.yml"
        )
        cls.dashboard = json.loads(
            read_project_file(
                "infra",
                "grafana",
                "provisioning",
                "dashboards",
                "json",
                "pipeline.json",
            )
        )

    def test_kafka_broker_enables_remote_jmx_for_exporter(self):
        self.assertIn('KAFKA_JMX_PORT: "9999"', self.compose)
        self.assertIn('KAFKA_JMX_HOSTNAME: "kafka"', self.compose)

    def test_jmx_exporter_is_a_pinned_sidecar(self):
        self.assertIn("jmx-exporter:", self.compose)
        self.assertIn(
            "bitnami/jmx-exporter@sha256:"
            "bbfd383a68c5b94db03d3c07c5d74fea3c88b8776aac1533a23b2ed8da6aad8a",
            self.compose,
        )
        self.assertIn(
            "hostPort: kafka:9999",
            self.jmx_config,
        )
        self.assertIn(
            "./infra/jmx-exporter/kafka-bytes.yml:"
            "/config/kafka-bytes.yml:ro",
            self.compose,
        )

    def test_jmx_config_maps_real_broker_metrics(self):
        self.assertIn("BytesInPerSec", self.jmx_config)
        self.assertIn("BytesOutPerSec", self.jmx_config)
        self.assertIn(
            "kafka_server_brokertopicmetrics_bytesin_total",
            self.jmx_config,
        )
        self.assertIn(
            "kafka_server_brokertopicmetrics_bytesout_total",
            self.jmx_config,
        )

    def test_prometheus_scrapes_jmx_exporter_every_five_seconds(self):
        self.assertIn("scrape_interval: 5s", self.prometheus)
        self.assertIn("job_name: kafka-jmx", self.prometheus)
        self.assertIn('targets: ["jmx-exporter:5556"]', self.prometheus)

    def test_kafka_exporter_is_still_configured(self):
        self.assertIn("danielqsj/kafka-exporter:v1.8.0", self.compose)
        self.assertIn("--topic.filter=^raw-events$$", self.compose)
        self.assertIn("--group.filter=^events-processor$$", self.compose)

    def test_dashboard_keeps_ten_panels_and_adds_traffic_panel(self):
        panels = self.dashboard["panels"]
        panel_ids = {panel["id"] for panel in panels}

        self.assertTrue(set(range(1, 11)).issubset(panel_ids))
        self.assertIn(11, panel_ids)

        traffic_panels = [panel for panel in panels if panel["id"] == 11]
        self.assertEqual(len(traffic_panels), 1)

        panel = traffic_panels[0]
        self.assertEqual(
            panel["fieldConfig"]["defaults"]["unit"],
            "Bps",
        )

        expressions = [
            target.get("expr", "") for target in panel.get("targets", [])
        ]
        self.assertTrue(
            any(
                "kafka_server_brokertopicmetrics_bytesin_total" in expr
                for expr in expressions
            )
        )
        self.assertTrue(
            any(
                "kafka_server_brokertopicmetrics_bytesout_total" in expr
                for expr in expressions
            )
        )
        self.assertTrue(
            any("rate(" in expr for expr in expressions),
            "O painel deve converter contadores em bytes/s com rate().",
        )
        self.assertTrue(
            any('topic=""' in expr for expr in expressions),
            "O painel deve consultar o agregado do broker.",
        )
        self.assertTrue(
            any('topic="raw-events"' in expr for expr in expressions),
            "O painel deve consultar o tráfego do tópico raw-events.",
        )

    def test_observability_docs_document_real_metric_names(self):
        document = read_project_file("docs", "05-observability.md")
        self.assertIn(
            "kafka_server_brokertopicmetrics_bytesin_total",
            document,
        )
        self.assertIn(
            "kafka_server_brokertopicmetrics_bytesout_total",
            document,
        )


if __name__ == "__main__":
    unittest.main()
