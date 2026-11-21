#!/usr/bin/env python3
"""Estimate monthly API costs from svincolo's JSONL logs."""

import argparse
import base64
import io
from pathlib import Path
from bisect import bisect_right
from collections import defaultdict
from contextlib import nullcontext
from dataclasses import dataclass, fields
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
from fractions import Fraction
from html import escape
from typing import Optional
import csv
import json
import re
import sys


TOKEN_FIELDS = (
    "input_tokens",
    "cache_read_input_tokens",
    "cache_creation_input_tokens",
    "output_tokens",
)


SURFACE_LABELS = {
    "managed_chat": "Managed Chat Completions",
    "messages": "Anthropic Messages",
    "responses": "OpenAI Responses",
    "embeddings": "Embeddings",
    "transcriptions": "Audio transcriptions",
}


def read_prices(path):
    with open(path, encoding="utf-8") as stream:
        table = json.load(stream, parse_float=Decimal)
    prices = {}
    for provider, models in object_items(table):
        for model, history in object_items(models):
            entries = []
            for effective, rates in object_items(history):
                try:
                    day = date.fromisoformat(effective)
                    if day.isoformat() != effective:
                        raise ValueError
                    price = parse_price(rates)
                except (InvalidOperation, TypeError, ValueError) as error:
                    raise ValueError(f"invalid rates for {provider}/{model} on {effective}") from error
                entries.append((day, price))
            entries.sort()
            prices[provider, model] = (
                [day for day, _ in entries],
                [rates for _, rates in entries],
            )
    return prices


def object_items(value):
    if not isinstance(value, dict):
        raise ValueError("expected an object in the price table")
    return value.items()


def parse_rate(value, units=1_000_000):
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise ValueError("expected a decimal rate")
    amount = Decimal(value)
    if not amount.is_finite() or amount < 0:
        raise ValueError("expected a finite nonnegative rate")
    return Fraction(amount) / units


@dataclass
class Price:
    rates: tuple = ()
    input_tokens_over: int = 0
    long_rates: tuple = ()
    audio_input_rate: Optional[Fraction] = None
    audio_duration_rate: Optional[Fraction] = None

    def cost(self, known, tokens, audio_input, duration, uploaded):
        if not known:
            return Fraction(0)
        measured = duration if duration is not None else uploaded
        if self.audio_duration_rate is not None:
            return None if measured is None else measured * self.audio_duration_rate
        if duration is not None or (uploaded is not None and audio_input is None):
            return None
        if audio_input is not None and self.audio_input_rate is None:
            return None
        rates = self.rates
        if self.long_rates and sum(tokens[:3]) > self.input_tokens_over:
            if known != 1:
                return None
            rates = self.long_rates
        cost = sum(token * rate for token, rate in zip(tokens, rates))
        if audio_input is not None:
            cost += audio_input * self.audio_input_rate
        return cost


def parse_price(value):
    rates = dict(object_items(value))
    if set(rates) == {"audio_duration_usd_per_minute"}:
        return Price(audio_duration_rate=parse_rate(rates["audio_duration_usd_per_minute"], 60_000))
    audio_input_rate = None
    if "audio_input_tokens" in rates:
        audio_input_rate = parse_rate(rates.pop("audio_input_tokens"))
    if "long_context" not in rates:
        return Price(parse_token_rates(rates), audio_input_rate=audio_input_rate)
    if audio_input_rate is not None:
        raise ValueError("long-context pricing requires text token rates")
    long_context = dict(object_items(rates.pop("long_context")))
    threshold = count(long_context, "input_tokens_over")
    if threshold == 0:
        raise ValueError("input_tokens_over must be positive")
    del long_context["input_tokens_over"]
    return Price(parse_token_rates(rates), threshold, parse_token_rates(long_context))


def parse_token_rates(rates):
    if set(rates) != set(TOKEN_FIELDS):
        raise ValueError("expected all four token rates")
    return tuple(parse_rate(rates[key]) for key in TOKEN_FIELDS)


def count(event, name):
    value = event.get(name)
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer")
    return value


def optional_count(event, name):
    return count(event, name) if name in event else None


def identifier(event, name):
    value = event.get(name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a nonempty string")
    return value


@dataclass
class Totals:
    requests: int = 0
    attempts: int = 0
    known_attempts: int = 0
    unknown_usage_attempts: int = 0
    unpriced_attempts: int = 0
    input_tokens: int = 0
    cache_read_input_tokens: int = 0
    cache_creation_input_tokens: int = 0
    output_tokens: int = 0
    known_cost_usd: Fraction = Fraction(0)
    audio_input_tokens: int = 0
    audio_duration_ms: int = 0
    audio_token_attempts: int = 0
    audio_duration_attempts: int = 0
    uploaded_audio_duration_ms: int = 0
    uploaded_duration_attempts: int = 0
    upload_duration_priced_attempts: int = 0

    def add(self, row):
        for field in fields(self):
            name = field.name
            setattr(self, name, getattr(self, name) + getattr(row, name))

    def columns(self):
        return [
            decimal_text(value) if isinstance(value, Fraction) else value
            for value in (getattr(self, field.name) for field in fields(self))
        ]


def decimal_text(value):
    # Render terminating token prices exactly and retain at least 28 significant
    # digits if conversion from a per-minute rate produces a recurring decimal.
    with localcontext() as context:
        context.prec = max(28, len(str(abs(value.numerator))) + value.denominator.bit_length())
        return format(Decimal(value.numerator) / Decimal(value.denominator), "f")


def summarize(paths, month, prices, date_range=None):
    groups = defaultdict(Totals)
    seen = {}
    malformed_lines = duplicates = 0
    for path in paths:
        source = (nullcontext(sys.stdin) if path == "-" else open(path, encoding="utf-8")
                  if isinstance(path, (str, Path)) else nullcontext(path))
        with source as stream:
            for line_number, line in enumerate(stream, 1):
                if not line.strip():
                    continue
                try:
                    event = json.loads(line)
                except ValueError:
                    malformed_lines += 1
                    continue
                if not isinstance(event, dict) or event.get("event") not in ("managed_chat_usage", "native_usage"):
                    continue
                try:
                    timestamp = count(event, "timestamp_s")
                    day = datetime.fromtimestamp(timestamp, timezone.utc).date()
                    if month is not None and (day.year, day.month) != month:
                        continue
                    if date_range and not date_range[0] <= day <= date_range[1]:
                        continue
                    request_id = identifier(event, "request_id")
                    group = tuple(identifier(event, name) for name in
                                  ("caller", "provider", "upstream_model"))
                    surface = "managed_chat"
                    if event["event"] == "native_usage":
                        surface = identifier(event, "surface")
                        if surface not in SURFACE_LABELS or surface == "managed_chat":
                            raise ValueError("unsupported native usage surface")
                    attempts = count(event, "attempts")
                    known = count(event, "known_attempts")
                    tokens = tuple(count(event, name) for name in TOKEN_FIELDS)
                    if known > attempts or (known == 0 and any(tokens)):
                        raise ValueError("attempt counts are inconsistent with known usage")
                    audio_input = optional_count(event, "audio_input_tokens")
                    duration = optional_count(event, "audio_duration_ms")
                    uploaded = optional_count(event, "uploaded_audio_duration_ms")
                    if surface == "transcriptions" and known:
                        if (all(value is None for value in (audio_input, duration, uploaded))
                                or (duration is not None and (audio_input is not None or uploaded is not None))
                                or (audio_input is None and any(tokens))):
                            raise ValueError("inconsistent transcription usage measurements")
                    elif any(value is not None for value in (audio_input, duration, uploaded)):
                        raise ValueError("audio measurements require known transcription usage")
                    signature = (timestamp, group, surface, attempts, known, tokens, audio_input, duration, uploaded)
                    if request_id in seen:
                        if seen[request_id] != signature:
                            raise ValueError("conflicting events for the same request_id")
                        duplicates += 1
                        continue
                    seen[request_id] = signature
                except (OverflowError, OSError, ValueError) as error:
                    raise ValueError(f"{path}:{line_number}: {error}") from error

                dates, history = prices.get(group[1:], ((), ()))
                index = bisect_right(dates, day) - 1
                unpriced = attempts
                cost = Fraction(0)
                upload_priced = 0
                if index >= 0:
                    price = history[index]
                    estimate = price.cost(known, tokens, audio_input, duration, uploaded)
                    unpriced = known if estimate is None else 0
                    if estimate is not None:
                        cost = estimate
                        if uploaded is not None and price.audio_duration_rate is not None:
                            upload_priced = known
                groups[(*group, surface)].add(Totals(
                    requests=1,
                    attempts=attempts,
                    known_attempts=known,
                    unknown_usage_attempts=attempts - known,
                    unpriced_attempts=unpriced,
                    **dict(zip(TOKEN_FIELDS, tokens)),
                    known_cost_usd=cost,
                    audio_input_tokens=audio_input if audio_input is not None else 0,
                    audio_duration_ms=duration if duration is not None else 0,
                    audio_token_attempts=known if audio_input is not None else 0,
                    audio_duration_attempts=known if duration is not None else 0,
                    uploaded_audio_duration_ms=uploaded if uploaded is not None else 0,
                    uploaded_duration_attempts=known if uploaded is not None else 0,
                    upload_duration_priced_attempts=upload_priced,
                ))
    return groups, malformed_lines, duplicates


def write_tsv(groups, output=None):
    writer = csv.writer(output if output is not None else sys.stdout, delimiter="\t", lineterminator="\n")
    writer.writerow(["scope", "caller", "provider", "upstream_model",
                     *(field.name for field in fields(Totals))])
    total = Totals()
    combined = defaultdict(Totals)
    for key, row in groups.items():
        combined[key[:3]].add(row)
    for group, row in sorted(combined.items()):
        writer.writerow(["group", *group, *row.columns()])
        total.add(row)
    writer.writerow(["total", "", "", "", *total.columns()])


def usd(row):
    whole, _, fractional = decimal_text(row.known_cost_usd).partition(".")
    return f"${int(whole):,}.{fractional.ljust(2, '0')}"


def coverage(row):
    if not row.attempts:
        return '<span class="status">No attempts recorded</span>'
    if row.unknown_usage_attempts or row.unpriced_attempts:
        return '<span class="status partial">Partial estimate</span>'
    return '<span class="status">Usage and prices recorded</span>'


def gaps(row):
    return (f"{row.unknown_usage_attempts:,} missing usage · "
            f"{row.unpriced_attempts:,} unpriced")


def counted(number, noun):
    return f"{number:,} {noun}{'' if number == 1 else 's'}"


def audio_seconds(milliseconds):
    seconds, remainder = divmod(milliseconds, 1000)
    return f"{seconds:,}.{remainder:03d} s audio"


def recorded_usage(surface, row):
    if not row.known_attempts:
        return "Unknown"
    if surface == "embeddings":
        return f"{row.input_tokens:,} input tokens"
    if surface == "transcriptions":
        values = []
        if row.audio_token_attempts:
            values.extend((f"{row.input_tokens:,} text input tokens",
                           f"{row.audio_input_tokens:,} audio input tokens",
                           f"{row.output_tokens:,} text output tokens"))
        if row.audio_duration_attempts:
            values.append(f"{audio_seconds(row.audio_duration_ms)} reported by provider")
        if row.uploaded_duration_attempts:
            values.append(f"{audio_seconds(row.uploaded_audio_duration_ms)} uploaded")
        if row.upload_duration_priced_attempts:
            attempts = counted(row.upload_duration_priced_attempts, "attempt")
            values.append(f"{attempts} priced by upload duration")
        return "<br>".join(values)
    values = [f"{row.input_tokens:,} uncached input", f"{row.output_tokens:,} output"]
    if row.cache_read_input_tokens:
        values.append(f"{row.cache_read_input_tokens:,} cache read")
    if row.cache_creation_input_tokens:
        values.append(f"{row.cache_creation_input_tokens:,} cache write")
    return " · ".join(values)


def model_row(provider, model, surface, row):
    status = ""
    if row.unknown_usage_attempts or row.unpriced_attempts:
        status = f'<small class="partial">Partial estimate · {gaps(row)}</small>'
    return f"""<tr>
        <th scope="row">{escape(model)}<small>{escape(provider)}</small></th>
        <td class="api-detail">{SURFACE_LABELS[surface]}<small>{counted(row.requests, "request")} · {counted(row.attempts, "attempt")}</small></td>
        <td class="tokens">{recorded_usage(surface, row)}</td>
        <td class="amount">{usd(row)}{status}</td>
    </tr>"""


# Colors belong to API surfaces, so a legend keeps its meaning across months.
API_COLORS = {
    "managed_chat": ("Managed chat", "#bdaaff"),
    "messages": ("Messages", "#78c9b1"),
    "responses": ("Responses", "#83b9ff"),
    "embeddings": ("Embeddings", "#f3cc75"),
    "transcriptions": ("Transcriptions", "#ff957b"),
}
APP_COLORS = [
    ("#e9e3ff", "#614aa1"), ("#ffe7d7", "#9b522e"),
    ("#def2eb", "#2a6d59"), ("#dfebfd", "#40658a"),
]


def write_html(month, groups, malformed, *, period_label=None):
    total = Totals()
    apps = defaultdict(list)
    api_totals = defaultdict(Totals)
    caller_totals = defaultdict(Totals)
    for (caller, provider, model, surface), row in groups.items():
        apps[caller].append((provider, model, surface, row))
        caller_totals[caller].add(row)
        api_totals[surface].add(row)
        total.add(row)

    period = period_label or date.fromisoformat(month + "-01").strftime("%B %Y")
    css = Path(__file__).with_name("cost-dashboard.css").read_text(encoding="utf-8")
    export = io.StringIO()
    write_tsv(groups, export)
    encoded = base64.b64encode(export.getvalue().encode()).decode()
    ranked_surfaces = sorted(api_totals, key=lambda name: (-api_totals[name].known_cost_usd, name))
    stops, legend = [], []
    end = 0.0
    for name in ranked_surfaces:
        short, color = API_COLORS[name]
        share = (float(100 * api_totals[name].known_cost_usd / total.known_cost_usd)
                 if total.known_cost_usd else 0.0)
        if share:
            stops.append(f"{color} {end:.5f}% {end + share:.5f}%")
            end += share
        legend.append(f'<a href="#api-{name}" style="--tone:{color}"><i class="dot"></i>'
                      f'<span>{short}</span><b>{share:.1f}%</b></a>')
    gradient = f"conic-gradient({', '.join(stops)})" if stops else "#494355"
    text_input = total.input_tokens + total.cache_read_input_tokens + total.cache_creation_input_tokens
    duration = total.audio_duration_ms + total.uploaded_audio_duration_ms
    has_duration = total.audio_duration_attempts or total.uploaded_duration_attempts
    duration_text = audio_seconds(duration).removesuffix(" audio") if has_duration else "—"
    audio_note = (f"{total.audio_input_tokens:,} audio input tokens" if total.audio_token_attempts
                  else counted(total.audio_duration_attempts + total.uploaded_duration_attempts, "measured clip"))
    completeness = not (total.unknown_usage_attempts or total.unpriced_attempts)
    coverage_text = ("Complete" if completeness else "Partial") if total.attempts else "No activity"
    print(f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="icon" href="favicon.ico?v=2"><title>Svincolo · Usage &amp; spend · {month}</title><style>{css}</style></head>
<body><div class="top"><div class="nav-inner">
<a class="brand" href="#overview"><i class="mark" aria-hidden="true"></i>Svincolo</a>
<nav aria-label="Report sections"><a href="#overview">Overview</a><a href="#apps">Apps</a><a href="#apis">APIs</a></nav>
<a class="export" download="svincolo-{month}.tsv" href="data:text/tab-separated-values;base64,{encoded}"><span aria-hidden="true">↓</span> Export TSV</a>
</div></div>
<main><header id="overview" class="title-row"><h1>Usage &amp; spend</h1>
<div class="period-stamp"><time datetime="{month}">{period}</time><small>UTC</small></div></header>
<section class="hero" aria-label="Spend and share by API">
<div class="hero-metric"><div class="label">Estimated spend · USD</div><div class="amount">{usd(total)}</div>
<p class="activity">{counted(total.requests, "request")} · {counted(total.attempts, "attempt")} · {counted(len(apps), "app")}</p></div>
<div class="ring" style="background:{gradient}" aria-hidden="true"><div class="ring-core"><strong>{len(api_totals)}</strong>active APIs</div></div>
<div class="legend" aria-label="Share of known spend by API"><span class="legend-title">Spend by API</span>{''.join(legend)}</div>
</section>
<dl class="readings">
<div><dt>Text tokens</dt><dd>{text_input + total.output_tokens:,}</dd><small>{text_input:,} input · {total.output_tokens:,} output</small></div>
<div><dt>Cached input</dt><dd>{total.cache_read_input_tokens + total.cache_creation_input_tokens:,}</dd><small>{total.cache_read_input_tokens:,} read · {total.cache_creation_input_tokens:,} written</small></div>
<div><dt>Measured audio</dt><dd>{duration_text}</dd><small>{audio_note}</small></div>
<div><dt>Cost coverage</dt><dd>{coverage_text}</dd><small>{gaps(total)}</small></div></dl>
<div class="panels"><section id="apps" class="panel" aria-labelledby="apps-title"><div class="panel-head"><h2 id="apps-title">Applications</h2>
<span class="secondary">{counted(len(apps), "app")}</span></div>""")
    if not apps:
        unit = "period" if period_label else "month"
        print(f'<p class="empty">No API usage records for this {unit}.</p>')
    else:
        print('<div class="headings app-grid" aria-hidden="true"><span>Application</span><span>Calls</span>'
              '<span>Share</span><span class="money">Cost · USD</span><span></span></div>')
    ranked = sorted(apps, key=lambda name: (-caller_totals[name].known_cost_usd, name))
    for index, caller in enumerate(ranked):
        subtotal = caller_totals[caller]
        name = escape(caller)
        share = float(100 * subtotal.known_cost_usd / total.known_cost_usd) if total.known_cost_usd else 0
        tint, letter = APP_COLORS[index % len(APP_COLORS)]
        partial = ' <span class="partial">(partial)</span>' if subtotal.unknown_usage_attempts or subtotal.unpriced_attempts else ''
        print(f"""<details><summary class="app-grid" style="--tint:{tint};--letter:{letter};--share:{share:.4f}%">
<span class="app-name"><i class="monogram" aria-hidden="true">{escape(caller[:1].upper())}</i><span class="caller">{name}</span></span>
<span>{subtotal.requests:,}<span class="sr"> {"request" if subtotal.requests == 1 else "requests"}</span></span>
<span class="share"><span class="track"><i></i></span>{share:.1f}%<span class="sr"> of known spend</span></span>
<span class="money"><strong>{usd(subtotal)}</strong>{partial}</span><i class="arrow" aria-hidden="true"></i>
</summary><div class="models" role="region" aria-label="Models for {name}" tabindex="0">
<table><caption class="sr">Models for {name}</caption><thead><tr><th scope="col">Model / provider</th><th scope="col">API</th>
<th scope="col">Recorded usage</th><th scope="col">USD</th></tr></thead><tbody>""")
        for provider, model, surface, row in sorted(apps[caller], key=lambda item: (-item[3].known_cost_usd, *item[:3])):
            print(model_row(provider, model, surface, row))
        print("</tbody></table></div></details>")
    print("""</section><section id="apis" class="panel" aria-labelledby="apis-title">
<div class="panel-head"><h2 id="apis-title">API activity</h2><span class="secondary">Cost · USD</span></div><div class="apis">""")
    for surface in ranked_surfaces:
        row = api_totals[surface]
        color = API_COLORS[surface][1]
        partial = f' · {gaps(row)}' if row.unknown_usage_attempts or row.unpriced_attempts else ''
        print(f"""<div id="api-{surface}" class="api-row" style="--tone:{color}"><i class="dot"></i>
<div><h3>{SURFACE_LABELS[surface]}</h3><span class="secondary">{counted(row.requests, "request")} · {counted(row.attempts, "attempt")}{partial}</span></div>
<strong class="money">{usd(row)}</strong></div>""")
    if not ranked_surfaces:
        print('<p class="empty">No recorded calls.</p>')
    print("""</div></section></div><footer><span>Cost basis</span> · Configured provider rates and recorded usage.
Local provider fees exclude hosting. Tool and storage fees are separate.""")
    if malformed:
        print(f" Skipped {malformed:,} non-JSON log lines.")
    print("</footer></main></body></html>")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", help="UTC month in YYYY-MM format")
    parser.add_argument("--dates", nargs=2, metavar=("START", "END"), help="inclusive UTC date range")
    parser.add_argument("--prices", required=True, help="dated JSON price table")
    parser.add_argument("--html", action="store_true", help="render a standalone HTML report")
    parser.add_argument("logs", metavar="LOG", nargs="*", help="JSONL files (default: stdin)")
    args = parser.parse_args()

    try:
        date_range = None
        month_key = None
        label = None
        if args.dates and not args.month:
            first, last = map(date.fromisoformat, args.dates)
            if first > last:
                raise ValueError("End date precedes start date")
            date_range = (first, last)
            period_id = f"{first}_{last}"
            label = f"{first} to {last}"
        elif args.month and not args.dates and re.fullmatch(r"[0-9]{4}-[0-9]{2}", args.month):
            month = date.fromisoformat(f"{args.month}-01")
            month_key = (month.year, month.month)
            period_id = args.month
        else:
            raise ValueError("Select --month YYYY-MM or --dates START END")
        prices = read_prices(args.prices)
        groups, malformed, duplicates = summarize(args.logs or ["-"], month_key, prices, date_range)
    except (OSError, ValueError) as error:
        parser.exit(2, f"cost-report: {error}\n")

    if args.html:
        write_html(period_id, groups, malformed, period_label=label)
    else:
        write_tsv(groups)
    if not groups:
        print(f"cost-report: no API usage events for {period_id} in input", file=sys.stderr)
    if malformed:
        print(f"cost-report: skipped {malformed} non-JSON log lines", file=sys.stderr)
    if duplicates:
        print(f"cost-report: skipped {duplicates} duplicate usage events", file=sys.stderr)


if __name__ == "__main__":
    main()
