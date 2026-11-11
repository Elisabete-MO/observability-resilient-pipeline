import importlib.util
import os
import unittest

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PROVISIONER_PATH = os.path.join(
    ROOT_DIR, "infra", "grafana", "provisioning", "viewer_provision.py"
)


def read_project_file(*parts):
    with open(os.path.join(ROOT_DIR, *parts), encoding="utf-8") as handle:
        return handle.read()


def load_provisioner():
    spec = importlib.util.spec_from_file_location("viewer_provision", PROVISIONER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestGrafanaViewerProvisioning(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.compose = read_project_file("compose.yaml")
        cls.env_example = read_project_file(".env.example")
        cls.script = read_project_file(
            "infra", "grafana", "provisioning", "viewer_provision.py"
        )
        cls.readme = read_project_file("README.md")
        cls.guide = read_project_file(
            "docs", "guia-execucao-observability.md"
        )
        cls.provisioner = load_provisioner()

    def test_known_admin_credentials_are_not_published(self):
        legacy_password = "admin_" + "resilient_" + "2026"
        for content in (self.compose, self.env_example):
            self.assertNotIn(legacy_password, content)
            self.assertNotIn("GF_ADMIN_PASSWORD:-", content)
            self.assertNotIn("GF_ADMIN_USER:-", content)

    def test_required_secrets_fail_closed(self):
        self.assertIn("${GF_ADMIN_USER:?", self.compose)
        self.assertIn("${GF_ADMIN_PASSWORD:?", self.compose)
        self.assertIn("${GF_VIEWER_PASSWORD:?", self.compose)
        self.assertNotIn("${GF_ADMIN_USER:-", self.compose)
        self.assertNotIn("${GF_ADMIN_PASSWORD:-", self.compose)
        self.assertNotIn("${GF_VIEWER_PASSWORD:-", self.compose)

    def test_env_example_contains_empty_required_placeholders(self):
        self.assertIn("GF_ADMIN_USER=\n", self.env_example)
        self.assertIn("GF_ADMIN_PASSWORD=\n", self.env_example)
        self.assertIn("GF_VIEWER_PASSWORD=\n", self.env_example)

    def test_grafana_disables_anonymous_access_and_public_signup(self):
        self.assertIn('GF_AUTH_ANONYMOUS_ENABLED: "false"', self.compose)
        self.assertIn('GF_USERS_ALLOW_SIGN_UP: "false"', self.compose)

    def test_grafana_has_healthcheck_and_provisioner_waits_for_it(self):
        self.assertIn("/api/health", self.compose)
        self.assertIn("grafana-viewer-provisioner:", self.compose)
        self.assertIn("condition: service_healthy", self.compose)
        self.assertIn('restart: "no"', self.compose)

    def test_provisioner_is_one_shot_and_hardened(self):
        self.assertIn('command: ["python", "/provision/viewer_provision.py"]', self.compose)
        self.assertIn("read_only: true", self.compose)
        self.assertIn("cap_drop:", self.compose)
        self.assertIn("mem_limit: 64m", self.compose)

    def test_provisioner_uses_grafana_admin_api(self):
        self.assertIn("/api/health", self.script)
        self.assertIn("/api/users/lookup", self.script)
        self.assertIn("/api/admin/users", self.script)
        self.assertIn("/api/orgs/", self.script)
        self.assertIn("VIEWER_ROLE = \"Viewer\"", self.script)
        self.assertIn('"PUT"', self.script)

    def test_provisioner_preserves_existing_password_and_checks_server_admin(self):
        self.assertIn("a senha existente sera preservada", self.script)
        self.assertIn("isGrafanaAdmin", self.script)
        self.assertIn("Revise-o manualmente", self.script)

    def test_provisioner_never_logs_secrets(self):
        self.assertIn("def redact", self.script)
        self.assertIn("redact(", self.script)
        self.assertNotIn("print(f\"[viewer-provision] {config", self.script)
        self.assertNotIn("log(config", self.script)

    def test_load_config_requires_all_secret_variables(self):
        module = self.provisioner
        base = {
            "GF_ADMIN_USER": "admin",
            "GF_ADMIN_PASSWORD": "admin-secret",
            "GF_VIEWER_PASSWORD": "viewer-secret",
        }
        config = module.load_config(base)
        self.assertEqual(config["viewer_login"], "viewer")
        self.assertEqual(config["viewer_email"], "viewer@localhost.local")
        self.assertEqual(config["org_id"], 1)

        for missing in base:
            with self.subTest(missing=missing):
                incomplete = dict(base)
                incomplete.pop(missing)
                with self.assertRaises(module.ProvisionError) as context:
                    module.load_config(incomplete)
                self.assertIn(missing, str(context.exception))

    def test_redact_removes_known_secrets(self):
        module = self.provisioner
        message = module.redact(
            "falha admin-secret e viewer-secret", ["admin-secret", "viewer-secret"]
        )
        self.assertEqual(message, "falha *** e ***")

    def test_docs_do_not_publish_default_admin_password(self):
        legacy_password = "admin_" + "resilient_" + "2026"
        for content in (self.readme, self.guide):
            self.assertNotIn(legacy_password, content)

    def test_readme_documents_local_secrets_and_viewer_profile(self):
        self.assertIn("GF_ADMIN_PASSWORD", self.readme)
        self.assertIn("GF_VIEWER_PASSWORD", self.readme)
        self.assertIn("Viewer", self.readme)


if __name__ == "__main__":
    unittest.main()
