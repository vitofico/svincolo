import importlib.util
from pathlib import Path
import re
import tomllib
import unittest


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
