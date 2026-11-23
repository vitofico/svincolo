import base64
import importlib.util
from pathlib import Path
import re
import tomllib
import unittest
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError


spec = importlib.util.spec_from_file_location(
    "alias_editor", Path(__file__).resolve().parents[1] / "deploy/alias-ui/editor.py"
)
editor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(editor)

CONFIG = '''
# svincolo-latest sol sol
# svincolo-latest haiku haiku
[[caller]]
name = "spunto"
routes = ["high", "sol", "haiku"]
[[caller]]
name = "quire"
routes = ["high"]
[[route]]
name = "sol"
upstream = "openai"
locality = "cloud"
surfaces = { chat = "managed", responses = "raw" }
[[route]]
name = "haiku"
upstream = "anthropic"
locality = "cloud"
surfaces = { chat = "managed", messages = "raw" }
[[route]]
name = "high"
upstream_model = "sol"
upstream = "openai"
locality = "cloud"
default_max_tokens = 8192
validation_retries = 1
surfaces = { chat = "managed", responses = "raw" }
'''


class AliasEditorTest(unittest.TestCase):
    def test_key_forms_and_inactive_destinations(self):
        config = CONFIG + """
[[upstream]]
name = "openai"
base_url = "https://api.openai.com/v1"
key_file = "openai.key"
[[upstream]]
name = "ollama"
base_url = "http://localhost:11434"
"""
        html = editor.page(config, "123", states={"openai": "Key rejected"})
        self.assertIn('type="password"', html)
        self.assertIn('OpenAI API key · Key rejected', html)
        self.assertNotIn("Ollama API key", html)
        self.assertNotIn('value="latest/sol"', html)
        with self.assertRaises(editor.Invalid):
            editor.edit(config, "update", "high", "latest/sol", {"openai"})
        self.assertIn('value="latest/sol"', editor.page(config, "123", states={"openai": "Active"}))

    def test_verify_before_updating_only_selected_secret_field(self):
        cfg = {"upstream": [{"name": "openai", "base_url": editor.PROVIDERS["openai"][1],
                             "key_file": "openai.key"}]}
        value = "fictional-test-token"
        with patch.object(editor, "kubernetes") as write, patch.object(editor, "provider_status") as status:
            status.return_value = "Key rejected"
            with self.assertRaises(editor.Invalid):
                editor.save_provider_key(cfg, "v1", "openai", value)
            write.assert_not_called()
            status.return_value = "Verification unavailable"
            with self.assertRaises(OSError):
                editor.save_provider_key(cfg, "v1", "openai", value)
            write.assert_not_called()
            status.return_value = "Active"
            editor.save_provider_key(cfg, "v1", "openai", value)
            write.assert_called_once_with([
                {"op": "test", "path": "/metadata/resourceVersion", "value": "v1"},
                {"op": "add", "path": "/data/openai.key", "value": base64.b64encode(value.encode()).decode()},
            ])
        cfg["caller"] = [{"key_file": "openai.key"}]
        self.assertEqual(editor.provider_key_files(cfg), {})

    def test_provider_validation_uses_official_models_endpoint_and_classifies_errors(self):
        for provider in editor.PROVIDERS:
            with self.subTest(provider=provider), patch.object(editor, "build_opener") as factory:
                opener = factory.return_value
                response = MagicMock()
                response.read.return_value = b'{"data": []}'
                opener.open.return_value.__enter__.return_value = response
                self.assertEqual(editor.provider_status(provider, "synthetic"), "Active")
                request = opener.open.call_args.args[0]
                self.assertEqual(request.full_url, editor.PROVIDERS[provider][1] + "/models")
                header = "Authorization" if provider == "openai" else "X-api-key"
                self.assertIn("synthetic", request.headers[header])
                opener.open.side_effect = HTTPError(request.full_url, 401, "denied", {}, None)
                self.assertEqual(editor.provider_status(provider, "synthetic"), "Key rejected")
                opener.open.side_effect = TimeoutError()
                self.assertEqual(editor.provider_status(provider, "synthetic"), "Verification unavailable")

    def test_create_retarget_and_delete(self):
        added = editor.edit(CONFIG, "create", "notes", "sol")
        config = tomllib.loads(added)
        self.assertEqual(config["caller"][0]["routes"][-1], "notes")
        self.assertEqual(config["caller"][1]["routes"], ["high"])
        changed = editor.edit(added, "update", "notes", "haiku")
        self.assertEqual(tomllib.loads(changed)["route"][-1]["surfaces"],
                         {"chat": "managed", "messages": "raw"})
        removed = editor.edit(changed, "delete", "notes", "")
        self.assertEqual(tomllib.loads(removed), tomllib.loads(CONFIG))

    def test_shared_tiers_only_accept_and_display_families(self):
        for tier in ("high", "medium", "low"):
            with self.subTest(tier=tier):
                config = CONFIG.replace('"high"', f'"{tier}"')
                with self.assertRaisesRegex(editor.Invalid, "family"):
                    editor.edit(config, "update", tier, "sol")
                updated = editor.edit(config, "update", tier, "latest/haiku")
                html = editor.page(updated, "1")
                card = html.split(f"<h2>{tier}</h2>")[1].split("</section>")[0]
                self.assertEqual(re.findall(r'<option value="([^"]*)"', card), ["latest/haiku", "latest/sol"])
                self.assertIn('value="latest/haiku" selected>Haiku</option>', card)
                self.assertIn("# svincolo-follow haiku", updated)

    def test_protects_route_and_preserves_tier_settings(self):
        for action, name, model in [("create", "high", "sol"), ("create", "claude-new", "sol"),
                                    ("update", "sol", "haiku"), ("delete", "high", ""),
                                    ("create", "notes", "high")]:
            with self.subTest(action=action, name=name), self.assertRaises(editor.Invalid):
                editor.edit(CONFIG, action, name, model)
        config = tomllib.loads(editor.edit(CONFIG, "update", "high", "latest/haiku"))
        self.assertEqual(config["route"][-1]["validation_retries"], 1)
        self.assertEqual(config["route"][-1]["default_max_tokens"], 8192)
        self.assertEqual(config["caller"], tomllib.loads(CONFIG)["caller"])


if __name__ == "__main__":
    unittest.main()
