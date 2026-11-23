"""Svincolo operator UI for managing aliases that are visible to Spunto."""

import base64
import copy
import hashlib
import hmac
import json
import os
import re
import ssl
import tomllib
from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit
from urllib.request import HTTPSHandler, HTTPRedirectHandler, Request, build_opener

PATH = "/svincolo/aliases/"
ORIGINS = set(os.environ.get("ALIAS_ORIGIN", "http://localhost:8082").split(","))
ACCOUNT = Path("/var/run/secrets/kubernetes.io/serviceaccount")
SECRET = "https://kubernetes.default.svc/api/v1/namespaces/svincolo/secrets/svincolo-config"
DIGEST = Path("/run/operator/digest")
NAME = re.compile(r"[a-z][a-z0-9_-]{0,62}")
TIERS = {"low", "medium", "high"}
MANAGED = {"structured_modes", "validation_retries", "default_max_tokens"}


class Invalid(ValueError):
    pass


def route_index(config):
    spunto = next(c for c in config["caller"] if c["name"] == "spunto")
    routes = {r["name"]: r for r in config["route"] if r["name"] in spunto["routes"]}
    targets = {k: r for k, r in routes.items() if "upstream_model" not in r
               and r["surfaces"].get("chat") == "managed"}
    aliases = {k: r for k, r in routes.items() if "upstream_model" in r
               and r["surfaces"].get("chat") == "managed"}
    return spunto, targets, aliases


def replace_section(text, table, name, transform):
    blocks = re.split(r"(?=^\[\[)", text, flags=re.M)
    matches = [i for i, block in enumerate(blocks) if block.startswith(f"[[{table}]]")
               and tomllib.loads(block)[table][0]["name"] == name]
    if len(matches) != 1:
        raise Invalid("The configuration changed. Reload the page.")
    i = matches[0]
    blocks[i] = transform(blocks[i])
    return "".join(blocks)


def toml_value(value):
    if isinstance(value, dict):
        return "{ " + ", ".join(k + " = " + toml_value(v) for k, v in value.items()) + " }"
    return json.dumps(value, ensure_ascii=False)


def route_text(route):
    return "[[route]]\n" + "".join(f"{k} = {toml_value(v)}\n" for k, v in route.items()) + "\n"


def callers_for(config, name):
    return [c["name"] for c in config["caller"] if name in c["routes"] or "*" in c["routes"]]


def edit(text, action, name, model):
    config = tomllib.loads(text)
    spunto, targets, aliases = route_index(config)
    existing = aliases.get(name)
    family = None
    if name in TIERS and action in ("create", "update") and not model.startswith("latest/"):
        raise Invalid("Choose a model family for this alias.")
    if model.startswith("latest/"):
        family = model.removeprefix("latest/")
        latest = dict(re.findall(r"^# svincolo-latest (sol|terra|haiku) ([a-z0-9.-]+)$", text, re.M))
        if family not in latest:
            raise Invalid("No current release available for this family.")
        model = latest[family]
    if action == "create":
        if not NAME.fullmatch(name) or name.startswith(("gpt-", "claude-")):
            raise Invalid("Use a short alias starting with a lowercase letter. Provider model names are reserved.")
        if any(r["name"] == name for r in config["route"]):
            raise Invalid("This route name is already in use.")
    elif action not in ("update", "delete") or not existing:
        raise Invalid("Select an existing alias.")
    if action == "delete":
        if name in TIERS or callers_for(config, name) != ["spunto"]:
            raise Invalid("Only custom aliases used exclusively by Spunto can be removed here.")
        config["route"].remove(existing)
        spunto["routes"].remove(name)
        text = replace_section(text, "route", name, lambda _: "")
    else:
        if model not in targets:
            raise Invalid("Choose a catalog model available to Spunto.")
        route = copy.deepcopy(targets[model])
        route.update(name=name, upstream_model=model)
        marker = f"# svincolo-follow {family}\n" if family else ""
        def serialize():
            return route_text(route).replace("[[route]]\n", "[[route]]\n" + marker, 1)
        if existing:
            # Preserve the tier's token/repair settings while changing its destination.
            for field in MANAGED | {"discover"}:
                if field in existing:
                    route[field] = existing[field]
            if "structured_modes" in existing and existing["upstream"] != route["upstream"]:
                raise Invalid("This alias has provider-specific modes. Select a model from the same provider.")
            config["route"][config["route"].index(existing)] = route
            text = replace_section(text, "route", name, lambda _: serialize())
        else:
            config["route"].append(route)
            spunto["routes"].append(name)
            text = text.rstrip() + "\n\n" + serialize()
    if action in ("create", "delete"):
        def update_caller(block):
            return re.sub(r'^routes\s*=\s*\[[^\]]*\]', "routes = " + toml_value(spunto["routes"]),
                          block, count=1, flags=re.M)
        text = replace_section(text, "caller", "spunto", update_caller)
    if tomllib.loads(text) != config:
        raise Invalid("Unable to safely update this configuration format.")
    return text


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args):
        return None


def kubernetes(patch=None):
    tls = ssl.create_default_context(cafile=str(ACCOUNT / "ca.crt"))
    headers = {"Authorization": "Bearer " + (ACCOUNT / "token").read_text().strip()}
    if patch is not None:
        headers["Content-Type"] = "application/json-patch+json"
    req = Request(SECRET, data=None if patch is None else json.dumps(patch).encode(),
                  headers=headers, method="GET" if patch is None else "PATCH")
    with build_opener(NoRedirect(), HTTPSHandler(context=tls)).open(req, timeout=8) as res:
        if patch is None:
            secret = json.load(res)
            return (secret["metadata"]["resourceVersion"],
                    base64.b64decode(secret["data"]["svincolo.toml"]).decode())


def scopes(config):
    return {"app": sorted(c["name"] for c in config["caller"]),
            "provider": sorted({u.get("provider", u["name"]) for u in config["upstream"]})}


def budget_forms(config):
    if not os.environ.get("SVINCOLO_SPEND_DIR"):
        return ""
    import spend
    policies = spend.read("limits.json")
    state = spend.read("usage.json")
    version = spend.revision(policies)
    cards = []
    for scope, names in scopes(config).items():
        for name in names:
            rule = policies[scope].get(name, {})
            period = rule.get("period", "month")
            options = "".join(f'<option value="{p}" {"selected" if p == period else ""}>{p.title()}</option>'
                              for p in spend.PERIODS)
            usage = state["periods"][period][scope].get(name, 0)
            fields = "".join(f'<label>{kind.title()} cap (USD)<input aria-label="{kind} cap for {escape(name)}" '
                             f'inputmode="decimal" name="{kind}" placeholder="Unlimited" value="{spend.label(rule.get(kind))}"></label>'
                             for kind in ("soft", "hard"))
            cards.append(f'<section><h3>{scope.title()} · {escape(name)}</h3>'
                         f'<p>Recorded this {period}: ${spend.label(usage)}</p>'
                         f'<form method="post"><input type="hidden" name="action" value="budget">'
                         f'<input type="hidden" name="scope" value="{scope}"><input type="hidden" name="name" value="{escape(name,quote=True)}">'
                         f'<input type="hidden" name="revision" value="{version}"><label>Resets each<select name="period">{options}</select></label>'
                         f'{fields}<button>Save cap</button></form></section>')
    return ('<h2 id="budgets">Spending caps</h2><p>USD estimates reset on UTC calendar boundaries. Weeks start Monday. '
            'Soft caps warn on the dashboard. Hard caps block new calls once recorded spend reaches the limit. '
            'Usage refreshes every 30 seconds. Unpriced usage is excluded. Leave a limit blank for unlimited.</p>'
            + "".join(cards))


def page(text, revision, notice=""):
    config = tomllib.loads(text)
    _, targets, aliases = route_index(config)
    def fields(action, name=""):
        return (f'<input type="hidden" name="action" value="{action}">'
                f'<input type="hidden" name="revision" value="{escape(revision, quote=True)}">'
                + (f'<input type="hidden" name="name" value="{escape(name, quote=True)}">' if name else ""))
    followers = {}
    for block in re.split(r"(?=^\[\[)", text, flags=re.M):
        following = re.search(r"^# svincolo-follow (sol|terra|haiku)$", block, re.M)
        if following and block.startswith("[[route]]"):
            followers[tomllib.loads(block)["route"][0]["name"]] = following[1]
    latest = dict(re.findall(r"^# svincolo-latest (sol|terra|haiku) ([a-z0-9.-]+)$", text, re.M))
    def select(current=None, family_only=False):
        label = "Model family" if family_only else "Destination model"
        choices = {}
        for family, dest in sorted(latest.items()):
            if dest in targets:
                choices["latest/" + family] = family.title() if family_only else f"Latest {family.title()} · {dest}"
        if not family_only:
            for model, route in sorted(targets.items()):
                choices[model] = f"{model} · {', '.join(route['surfaces'])}"
        options = []
        if current not in choices:
            prompt = "Choose a family" if family_only else "Choose a model"
            options.append(f'<option value="" selected disabled>{prompt}</option>')
        for value, caption in choices.items():
            selected = " selected" if value == current else ""
            options.append(f'<option value="{escape(value, quote=True)}"{selected}>{escape(caption)}</option>')
        return f'<select aria-label="{label}" name="model" required>' + "".join(options) + "</select>"
    cards = []
    for name, r in aliases.items():
        callers = callers_for(config, name)
        remove = ""
        if name not in TIERS and callers == ["spunto"]:
            remove = (f'<details><summary>Remove alias</summary><form method="post">{fields("delete", name)}'
                      f'<button>Confirm removal of {escape(name)}</button></form></details>')
        current = "latest/" + followers[name] if name in followers else r["upstream_model"]
        cards.append(f'<section><h2>{escape(name)}</h2><p>Used by {escape(", ".join(callers))}</p>'
                     f'<form method="post">{fields("update", name)}{select(current, family_only=name in TIERS)}'
                     f'<button>Save destination</button></form>{remove}</section>')
    return rf"""<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<link rel="icon" href="favicon.ico?v=2"><title>Aliases and spending caps · Svincolo</title><style>
*{{box-sizing:border-box}}body{{background:#f5f3ed;color:#202720;font:16px/1.5 Inter,system-ui,sans-serif;margin:0}}main{{max-width:54rem;margin:auto;padding:2rem 1rem 4rem}}h1{{font-size:2.6rem;letter-spacing:-.05em;margin:.5rem 0}}h2{{font-size:1.15rem;margin:0}}section{{padding:1.25rem;background:#fff;border:1px solid #cdcfc4;border-radius:.6rem;margin-top:1rem}}p{{margin:.35rem 0 .8rem}}form{{display:flex;gap:.5rem;flex-wrap:wrap}}select,input,button{{font:inherit;padding:.55rem;border-radius:.35rem;border:1px solid #9ca79b;max-width:100%}}button{{background:#214b37;color:white;cursor:pointer}}label{{display:grid;gap:.2rem}}form{{align-items:end}}h3{{margin:.2rem 0}}#budgets{{margin-top:2rem}}summary{{cursor:pointer}}details{{margin-top:.65rem}}a{{color:#214b37}}[role=status]{{background:#e4ecdf;padding:.7rem}}
</style><main><a href="{escape(os.environ.get('DASHBOARD_URL', '/'), quote=True)}">Svincolo dashboard</a><h1>Model aliases</h1><a href="#budgets">Spending caps</a>
<p>New aliases appear in Spunto. Saving a shared alias changes its destination for every app listed.</p>
{('<p role="status">'+escape(notice)+'</p>') if notice else ''}
<section><h2>Create an alias</h2><form method="post">{fields("create")}<input aria-label="Alias name" name="name" placeholder="Alias name" pattern="[a-z][a-z0-9_\-]{{0,62}}" required maxlength="63">{select()}<button>Create alias</button></form></section>
{''.join(cards)}{budget_forms(config)}</main></html>"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def send(self, status, body="", **headers):
        data = body.encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Security-Policy", "default-src 'none'; img-src 'self'; style-src 'unsafe-inline'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'")
        for k, v in headers.items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def authorized(self):
        if urlsplit(self.path).path != PATH:
            self.send(404)
            return False
        scheme, _, encoded = self.headers.get("Authorization", "").partition(" ")
        try:
            raw = base64.b64decode(encoded, validate=True)
        except ValueError:
            raw = b""
        if scheme.lower() != "basic" or not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), DIGEST.read_text().strip()):
            self.send(401, "Operator login required.", **{"WWW-Authenticate": 'Basic realm="Svincolo operator"'})
            return False
        return True

    def do_GET(self):
        if urlsplit(self.path).path == PATH + "favicon.ico":
            icon = Path(__file__).with_name("favicon.ico").read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "image/x-icon")
            self.send_header("Content-Length", str(len(icon)))
            self.end_headers()
            self.wfile.write(icon)
            return
        if not self.authorized():
            return
        try:
            rev, text = kubernetes()
            self.send(200, page(text, rev, "Changes saved."
                                if self.path == PATH + "?saved" else ""))
        except (OSError, ValueError, KeyError):
            self.send(503, "Router configuration unavailable.")

    def do_POST(self):
        if not self.authorized():
            return
        if self.headers.get("Origin") not in ORIGINS or self.headers.get_content_type() != "application/x-www-form-urlencoded":
            self.send(403, "Open the editor to submit this change.")
            return
        try:
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 < size <= 4096:
                raise Invalid("Invalid form size.")
            form = parse_qs(self.rfile.read(size).decode(), strict_parsing=True, keep_blank_values=True, max_num_fields=7)
            if any(len(v) != 1 for v in form.values()):
                raise Invalid("Duplicate form field.")
            rev, text = kubernetes()
            if form.get("action") == ["budget"]:
                import spend
                scope, name = form["scope"][0], form["name"][0]
                if name not in scopes(tomllib.loads(text)).get(scope, ()):
                    raise Invalid("Unknown app or provider.")
                try:
                    spend.save(form["revision"][0], scope, name, form["period"][0], form["soft"][0], form["hard"][0])
                except ValueError as exc:
                    raise Invalid(str(exc)) from exc
                self.send(303, Location=PATH + "?saved#budgets")
                return
            if form.get("revision") != [rev]:
                self.send(409, page(text, rev, "The catalog changed. Review the current values and submit again."))
                return
            updated = edit(text, form["action"][0], form["name"][0], form.get("model", [""])[0])
            if updated != text:
                kubernetes([{"op":"test", "path":"/metadata/resourceVersion", "value":rev},
                            {"op":"replace", "path":"/data/svincolo.toml",
                             "value":base64.b64encode(updated.encode()).decode()}])
            self.send(303, Location=PATH + "?saved")
        except Invalid as exc:
            self.send(400, escape(str(exc)))
        except (ValueError, KeyError):
            self.send(400, "Invalid form.")
        except HTTPError as exc:
            self.send(409 if exc.code in (409, 422) else 503, "Save failed. Reload the editor and try again.")
        except OSError:
            self.send(503, "Router configuration unavailable.")


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8082), Handler).serve_forever()
