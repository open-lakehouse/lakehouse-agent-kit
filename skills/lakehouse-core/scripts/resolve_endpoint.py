#!/usr/bin/env python3
"""Resolve one lakehouse service endpoint: config -> discover -> preferred default -> fail.

Stdlib-only; run as `python3 -I resolve_endpoint.py <section>.<key> ...`. This is the
single implementation of the resolution order (IMPLEMENTATION_PLAN.md §3.4, CONTRIBUTING.md
portability rule 2) that every product skill calls instead of hardcoding a host:port.

  1. CONFIG     LAKEHOUSE_<SECTION>_<KEY>, or the key in the profile the USER selected
                (--profile/--file/LAKEHOUSE_PROFILE*). Authoritative: returned even if a
                reachability probe fails (a warning is printed).
  2. DISCOVER   TCP-probe specific candidates only: each --candidate HOST[:PORT] the
                caller passes (in order, first reachable wins; reported by position, see
                below), then — when the effective invocation is `docker` — the host-side
                port `docker ps` reports as published for the conventional container port.
                More than one matching published endpoint = ambiguous = fail, judged
                BEFORE probing (never pick the one that happens to answer).
  3. PREFERRED  the conventional port on localhost (registry below), returned ONLY if
                reachable, and labeled a preference, not a certainty.
  4. FAIL LOUD  exit 1 naming the env var / profile key to set. Never silently assume.

Safety: no port ranges or arbitrary hosts are scanned; every probe is one bounded TCP
connect to a named candidate.

TOPOLOGY ONLY. The resolver answers "where does this service listen?". It never selects a
profile (no selection => only LAKEHOUSE_* env vars are consulted, profile files are not
read) and refuses identity/targeting keys (profile name, catalog, schema, bucket,
warehouse, credentials) — those are confirmed with the user (load_profile.py).

No-secret-echo: configured values are validated with load_profile.check_value (userinfo
and credential parameters are rejected with value-free messages); only validated config
values, docker-derived hosts, and integer ports ever reach stdout/stderr. Candidate text
is never echoed anywhere; candidates are referred to by position (`#N`).

Candidates: `--candidate-env VAR` (preferred) names an exported env var holding host[:port];
`--candidate HOST[:PORT]` passes it by value. Both are probed in command-line order.

Output (`--format env` is the consumable contract — eval it to set LAKEHOUSE_<SECTION>_<KEY>):
  config / docker discovery / preferred default
          export LAKEHOUSE_<SECTION>_<KEY>='<validated value>'
  --candidate-env VAR wins
          export LAKEHOUSE_<SECTION>_<KEY>="sc://${VAR}[:port][/path]"   (indirect: the value
          is expanded by the caller's shell at eval time, never printed by the resolver)
  --candidate (by value) wins
          export LAKEHOUSE_RESOLVED_CANDIDATE=N   — index only; the endpoint variable is NOT
          set, because a by-value candidate cannot be safely echoed. Use --candidate-env.
--format text prints the value (or `candidate #N` for a candidate win); --format json prints
the result object ("value": null plus "candidate", and "value_ref" for --candidate-env).
Provenance goes to stderr.
Exit status: 0 resolved, 1 unresolved or invalid configured value, 2 usage error
(unknown / identity key, bad profile selection).
"""

from __future__ import annotations

import argparse
import importlib.util
import io
import ipaddress
import json
import os
import re
import shlex
import socket
import subprocess
import sys
import tempfile
import threading
from contextlib import redirect_stderr, redirect_stdout
from urllib.parse import urlparse


def _load_sibling(name: str):
    """Import a sibling script by path (`python3 -I` keeps the script dir off sys.path)."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), name + ".py")
    spec = importlib.util.spec_from_file_location(f"_lakehouse_core_{name}", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


lp = _load_sibling("load_profile")

PREFERRED_HOST = "localhost"
DEFAULT_TIMEOUT = 0.5   # seconds per TCP probe
MAX_TIMEOUT = 5.0

# Endpoint registry: section.key -> (kind, scheme, preferred port or None, path suffix,
# docker name/image tokens). Preferred ports are the services' generic UPSTREAM conventional
# ports — a PREFERENCE tried after config and discovery, never an assumption. They must
# agree with templates/lakehouse.profile.example.* (checked by --self-test). A deployment
# that remaps a port (e.g. UC moved off 8080 to avoid Airflow) says so in its own profile,
# never here. Unity Catalog OSS: UnityCatalogServer.DEFAULT_PORT = 8080 upstream.
ENDPOINTS = {
    "spark.connect_uri":          ("sc_uri", "sc", 15002, "", ("spark",)),
    "kafka.bootstrap_servers":    ("hostports", "", 9092, "", ("kafka", "redpanda")),
    "unity_catalog.rest_url":     ("http_url", "http", 8080, "/api/2.1/unity-catalog",
                                   ("unitycatalog", "unity", "uc")),
    "unity_catalog.iceberg_rest": ("http_url", "http", 8080, "/api/2.1/unity-catalog/iceberg/v1",
                                   ("unitycatalog", "unity", "uc")),
    "mlflow.tracking_uri":        ("http_url", "http", 5000, "", ("mlflow",)),
    "mlflow.gateway_uri":         ("http_url", "http", None, "", ("mlflow",)),
    "airflow.base_url":           ("http_url", "http", 8080, "", ("airflow",)),
    "object_store.s3_endpoint":   ("http_url", "http", 9000, "", ("minio", "seaweedfs", "seaweed", "s3")),
    "delta_sharing.endpoint":     ("http_url", "https", 8443, "", ("sharing",)),
}
# Identity / targeting: which profile, catalog, schema, bucket, or credentials to act on.
# The resolver refuses these outright — guessing them has side effects.
IDENTITY_KEYS = {"profile.name", "naming.default_catalog", "naming.schemas",
                 "object_store.bucket", "object_store.warehouse", "object_store.region",
                 "k8s.namespace", "k8s.context"}
NAME_TOKEN_RE = re.compile(r"[a-z0-9]+")
DOCKER_PORT_RE = re.compile(r"^(?P<ip>[0-9.]+|\[?[0-9a-fA-F:]*\]?):(?P<host>\d{1,5})->(?P<ctr>\d{1,5})/tcp$")


class ResolveError(Exception):
    def __init__(self, msg: str, code: int):
        super().__init__(msg)
        self.code = code


def env_hint(endpoint: str) -> str:
    section, key = endpoint.split(".", 1)
    kind, scheme, _, suffix, _ = ENDPOINTS[endpoint]
    example = "<host>:<port>" if kind == "hostports" else f"{scheme or 'http'}://<host>:<port>{suffix}"
    return (f"export {lp.env_var(section, key)}={example}  "
            f"(or set [{section}] {key} in the lakehouse profile the user selected)")


def fmt_hp(host: str, port: int) -> str:
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


def build_value(endpoint: str, host: str, port: int) -> str:
    kind, scheme, _, suffix, _ = ENDPOINTS[endpoint]
    return fmt_hp(host, port) if kind == "hostports" else f"{scheme}://{fmt_hp(host, port)}{suffix}"


def tcp_probe(host: str, port: int, timeout: float) -> bool:
    """One TCP connect, bounded in wall time (DNS lookups included, via a daemon thread)."""
    result = [False]

    def attempt():
        try:
            with socket.create_connection((host, port), timeout=timeout):
                result[0] = True
        except (OSError, ValueError):
            pass

    t = threading.Thread(target=attempt, daemon=True)
    t.start()
    t.join(timeout * 2 + 0.5)
    return result[0]


def docker_ps(timeout: float):
    """Rows of (name, image, ports) from `docker ps`, or None if docker is unavailable."""
    try:
        out = subprocess.run(["docker", "ps", "--format", "{{.Names}}\t{{.Image}}\t{{.Ports}}"],
                             capture_output=True, text=True, timeout=max(timeout * 4, 2.0))
    except (OSError, subprocess.SubprocessError):
        return None
    if out.returncode != 0:
        return None
    return [tuple((line.split("\t") + ["", ""])[:3]) for line in out.stdout.splitlines() if line.strip()]


def parse_candidate(n: int, source, port_default, environ: dict) -> dict:
    """("value", "host[:port]") or ("env", "VAR") -> {n, host, port, env, had_port}.
    Candidates are host[:port] ONLY, screened with the loader's host:port checks (no
    userinfo, credential params, `? ; & # /`, control chars). Candidate text is
    caller-supplied and may be anything a user pasted, so it is NEVER echoed: messages
    and results refer to it by position. An env var NAME may be shown; its value never."""
    how, text = source
    where = f"--candidate #{n}"
    env_name = None
    if how == "env":
        if not isinstance(text, str) or not lp.ENV_NAME_RE.fullmatch(text):
            raise ResolveError(f"--candidate-env #{n}: must name an env var "
                               "([A-Z_][A-Z0-9_]*); value not shown", 2)
        env_name, where = text, f"--candidate-env #{n} ({text})"
        text = environ.get(env_name)
        if text is None or not text.strip():
            raise ResolveError(f"{where}: variable is unset or empty (export it first)", 2)
        if text != text.strip():
            raise ResolveError(f"{where}: value has surrounding whitespace; value not shown", 2)
    if not isinstance(text, str) or lp.has_control_chars(text):
        raise ResolveError(f"{where}: " + lp.MSG_CONTROL, 2)
    text = text.strip()
    had_port = not re.fullmatch(r"\[[^\]]*\]|[^:\[\]]+", text)  # host without a port?
    if not had_port:
        if port_default is None:
            raise ResolveError(f"{where}: needs an explicit port (this endpoint has no "
                               "preferred port); value not shown", 2)
        text = f"{text}:{port_default}"
    errors: list[str] = []
    value = lp.check_value(where, "hostports", text, errors)
    if errors or not value or "," in value:
        raise ResolveError(errors[0] if errors else
                           f"{where}: expected exactly one host[:port]; value not shown", 2)
    host, _, port = value.rpartition(":")
    return {"n": n, "host": host.strip("[]"), "port": int(port), "env": env_name, "had_port": had_port}


def value_ref(endpoint: str, cand: dict) -> str:
    """Shell-expandable reference to a --candidate-env win, e.g. `sc://${VAR}:15002`. Safe
    inside double quotes: only ${VAR} expands; the scheme/port/path are fixed literals."""
    kind, scheme, _, suffix, _ = ENDPOINTS[endpoint]
    hp = "${" + cand["env"] + "}" + ("" if cand["had_port"] else f":{cand['port']}")
    return hp if kind == "hostports" else f"{scheme}://{hp}{suffix}"


def docker_candidates(rows, port, hints, container):
    """Host-side (host, port, container) published for container port `port`. Restricted to
    the profile's container when one is configured, else to name/image hint matches."""
    found = []
    for name, image, ports in rows or []:
        if container:
            if name != container:
                continue
        elif not set(hints) & set(NAME_TOKEN_RE.findall((name + " " + image).lower())):
            continue  # token match: "uc" matches "lh-uc" but not "duckdb"
        for spec in ports.split(","):
            m = DOCKER_PORT_RE.match(spec.strip())
            if not m or int(m.group("ctr")) != port:
                continue
            ip = m.group("ip").strip("[]")
            try:
                addr = ipaddress.ip_address(ip) if ip else None
            except ValueError:
                continue
            host = PREFERRED_HOST if addr is None or addr.is_unspecified else str(addr)
            hp = (host, int(m.group("host")), lp.shown(name))
            if hp not in found:
                found.append(hp)
    return found


def resolve(endpoint: str, config_value, config_source: str, invocation, container,
            candidates, timeout: float, probe=tcp_probe, ps=docker_ps) -> dict:
    """Apply the four tiers. Returns a result dict; raises ResolveError (exit 1) when
    nothing resolves or the configured value is invalid."""
    section, key = endpoint.split(".", 1)
    kind, _, pref_port, _, hints = ENDPOINTS[endpoint]
    var = lp.env_var(section, key)
    notes: list[str] = []

    # 1. CONFIG — validated like the loader does; errors never contain the value.
    if config_value is not None and not lp.is_blank(config_value):
        errors: list[str] = []
        value = lp.check_value(endpoint, kind, config_value, errors)
        if not errors and isinstance(value, str):
            for n, host in enumerate(lp.hosts_of(kind, value), start=1):
                if lp.PLACEHOLDER_HOST_RE.fullmatch(host):
                    errors.append(f"{endpoint}: " + (lp.MSG_PLACEHOLDER_HP.format(n=n)
                                  if kind == "hostports" else lp.MSG_PLACEHOLDER_URI))
        if errors:
            raise ResolveError(f"invalid configured value from {config_source}:\n"
                               + "\n".join(f"  - {e}" for e in errors), 1)
        if kind == "hostports":
            targets = [(h.rpartition(":")[0].strip("[]"), int(h.rpartition(":")[2]))
                       for h in value.split(",")]
        else:
            u = urlparse(value)
            # No explicit port: the scheme's own default (sc:// is Spark Connect's 15002).
            default = {"https": 443, "http": 80, "sc": 15002}[u.scheme]
            targets = [(lp.netloc_host(u.netloc).strip("[]"), u.port or default)]
        reachable = any(probe(h, p, timeout) for h, p in targets)
        if not reachable:
            notes.append(f"configured endpoint from {config_source} did not answer a TCP probe "
                         f"within {timeout}s; it is still authoritative — check the service or the "
                         "profile, do not substitute another endpoint")
        return {"endpoint": endpoint, "tier": "config", "value": value, "source": config_source,
                "reachable": reachable, "env_var": var, "notes": notes}

    # 2. DISCOVER — caller-named candidates first (ordered), then docker metadata.
    probed: list[str] = []
    for cand in candidates:
        n = cand["n"]
        label = f"--candidate-env #{n} ({cand['env']})" if cand["env"] else f"--candidate #{n}"
        ok = probe(cand["host"], cand["port"], timeout)
        probed.append(f"{label} ({'reachable' if ok else 'unreachable'})")
        if ok:
            # The caller already holds the candidate; report WHICH one, never its text.
            result = {"endpoint": endpoint, "tier": "discover", "value": None, "candidate": n,
                      "source": label, "reachable": True, "env_var": var, "notes": notes}
            if cand["env"]:
                result["candidate_env"] = cand["env"]
                result["value_ref"] = value_ref(endpoint, cand)
                notes.append(f"{label} is reachable; --format env assigns {var} from "
                             f"${cand['env']} (its value is never printed); pin it with {var}")
            else:
                notes.append(f"{label} is reachable; its value is not repeated and {var} is NOT "
                             "set by --format env — use the candidate you passed, or pass it via "
                             f"--candidate-env; pin it with {var}")
            return result
    docker_note = "skipped (invocation is not docker)"
    if invocation == "docker" and pref_port is not None:
        rows = ps(timeout)
        if rows is None:
            docker_note = "skipped (docker CLI unavailable or `docker ps` failed)"
        else:
            # Ambiguity is judged on EVERY matching published endpoint, before probing: two
            # matching services is ambiguous even if only one happens to answer right now.
            matches = docker_candidates(rows, pref_port, hints, container)
            # docker_candidates already merges duplicate mappings of ONE container (e.g. its
            # 0.0.0.0 and :: bindings); any remaining entries are distinct publications.
            if len(matches) > 1:
                raise ResolveError(
                    f"could not resolve {endpoint}: docker discovery is ambiguous — "
                    f"{len(matches)} matching published endpoints "
                    f"({', '.join(f'{fmt_hp(h, p)} from {name}' for h, p, name in matches)}); "
                    f"refusing to pick one.\nAsk the user which one, then: {env_hint(endpoint)}", 1)
            if matches:
                host, port, name = matches[0]
                ok = probe(host, port, timeout)
                probed.append(f"{fmt_hp(host, port)} from container {name} "
                              f"({'reachable' if ok else 'unreachable'})")
                if ok:
                    return {"endpoint": endpoint, "tier": "discover",
                            "value": build_value(endpoint, host, port),
                            "source": f"docker ps (container {name} publishes {pref_port})",
                            "reachable": True, "env_var": var, "notes": notes}
                docker_note = "the one matching container did not answer"
            else:
                docker_note = "no running container publishes the conventional port"

    # 3. PREFERRED DEFAULT — only if reachable; labeled a preference.
    if pref_port is not None:
        ok = probe(PREFERRED_HOST, pref_port, timeout)
        probed.append(f"{fmt_hp(PREFERRED_HOST, pref_port)} preferred default "
                      f"({'reachable' if ok else 'unreachable'})")
        if ok:
            notes.append(f"preferred default (conventional port {pref_port}), verified reachable — "
                         f"a preference, not a certainty; pin it with {var}")
            return {"endpoint": endpoint, "tier": "preferred_default",
                    "value": build_value(endpoint, PREFERRED_HOST, pref_port),
                    "source": f"preferred default (conventional port {pref_port})",
                    "reachable": True, "env_var": var, "notes": notes}

    # 4. FAIL LOUD.
    raise ResolveError(
        f"could not resolve {endpoint} — refusing to assume an endpoint.\n"
        f"  config:    {config_source}\n"
        f"  discover:  docker {docker_note}\n"
        f"  probed:    {', '.join(probed) or 'nothing (no candidates, no preferred default)'}\n"
        f"Set it: {env_hint(endpoint)}", 1)


def config_lookup(args, endpoint: str, environ: dict, cwd: str, home: str):
    """(value or None, source description, effective invocation, container). Reads a
    profile file ONLY if the user explicitly selected one — never auto-selects."""
    section, key = endpoint.split(".", 1)
    var = lp.env_var(section, key)
    explicit = args.file or args.profile or environ.get("LAKEHOUSE_PROFILE_FILE") \
        or environ.get("LAKEHOUSE_PROFILE")
    raw: dict = {}
    where = "no profile selected (the resolver never auto-selects one; only env vars consulted)"
    if explicit:
        try:
            path, by = lp.select(args, environ, cwd, home)
            if path is not None and not os.path.isabs(path):
                path = os.path.join(cwd, path)
            raw = lp.read_profile_file(path, []) if path else {}
        except lp.ProfileError as exc:
            raise ResolveError(str(exc), 2) from None
        where = f"profile selected by {by}"
    for sec, body in lp.env_to_sections(environ, []).items():
        for k, v in body.items():
            if v != "":
                raw.setdefault(sec, {})[k] = v
            else:  # empty override unsets the key, as in load_profile
                raw.setdefault(sec, {}).pop(k, None)
    body = raw.get(section) if isinstance(raw.get(section), dict) else {}
    value = body.get(key)
    if value is not None and not lp.is_blank(value):
        source = var if environ.get(var, "").strip() else f"[{section}] {key} ({where})"
    else:
        value, source = None, f"{var} unset and no [{section}] {key} — {where}"

    def pick(sec_body, k, kind):
        v = sec_body.get(k) if isinstance(sec_body, dict) else None
        if v is None or lp.is_blank(v):
            return None
        errs: list[str] = []
        out = lp.check_value(k, kind, v, errs)
        return None if errs else out

    invocation = args.invocation or pick(body, "invocation", "invocation") \
        or pick(raw.get("profile"), "invocation", "invocation")
    container = pick(body, "container", "str")
    if container is not None and not lp.SAFE_NAME_RE.fullmatch(container):
        container = None
    return value, source, invocation, container


def run(argv: list[str], environ: dict, cwd: str, home: str, probe=tcp_probe, ps=docker_ps) -> int:
    ap = argparse.ArgumentParser(prog="resolve_endpoint.py", description=__doc__.split("\n\n", 1)[0])
    ap.add_argument("endpoint", nargs="?", help=f"one of: {', '.join(ENDPOINTS)}")
    ap.add_argument("--file", help="profile file the user chose (*.toml or *.env)")
    ap.add_argument("--profile", help="name of the profile the user chose")
    ap.add_argument("--candidate", action="append", dest="cands", metavar="HOST[:PORT]",
                    type=lambda v: ("value", v),
                    help="endpoint to probe in the discover tier, by value (repeatable, in "
                         "order); a win is reported by index only")
    ap.add_argument("--candidate-env", action="append", dest="cands", metavar="VAR",
                    type=lambda v: ("env", v),
                    help="NAME of an exported env var holding host[:port] to probe (repeatable, "
                         "in order); a win yields an indirect `export ...=\"...${VAR}...\"`")
    ap.add_argument("--invocation", choices=lp.INVOCATIONS,
                    help="override the effective invocation (docker enables `docker ps` discovery)")
    ap.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT,
                    help=f"seconds per TCP probe (default {DEFAULT_TIMEOUT}, max {MAX_TIMEOUT})")
    ap.add_argument("--format", choices=("text", "json", "env"), default="text")
    ap.add_argument("--self-test", action="store_true", help="run built-in test cases and exit")
    args = ap.parse_args(argv)
    if args.self_test:
        return self_test()

    def fail(msg, code):
        print(f"resolve_endpoint: {msg}", file=sys.stderr)
        return code

    endpoint = args.endpoint
    if not endpoint:
        return fail(f"missing endpoint; one of: {', '.join(ENDPOINTS)}", 2)
    if endpoint not in ENDPOINTS:
        section, _, key = endpoint.partition(".")
        # Echo the argument only when it is a known schema key (never arbitrary input).
        known = key in lp.SCHEMA.get(section, {})
        label = endpoint if known else "<endpoint not shown>"
        if known and (endpoint in IDENTITY_KEYS or key.endswith("_env") or section == "profile"):
            return fail(f"{label} is identity/targeting, not topology — the resolver never "
                        "auto-selects it. Confirm it with the user and read it from their "
                        "profile via load_profile.py.", 2)
        return fail(f"{label} is not a resolvable endpoint; one of: {', '.join(ENDPOINTS)}", 2)
    if not 0 < args.timeout <= MAX_TIMEOUT:
        return fail(f"--timeout must be in (0, {MAX_TIMEOUT}]", 2)
    pref_port = ENDPOINTS[endpoint][2]
    try:
        candidates = [parse_candidate(n, c, pref_port, environ)
                      for n, c in enumerate(args.cands or [], start=1)]
        value, source, invocation, container = config_lookup(args, endpoint, environ, cwd, home)
        result = resolve(endpoint, value, source, invocation, container, candidates,
                         args.timeout, probe=probe, ps=ps)
    except ResolveError as exc:
        return fail(str(exc), exc.code)

    for note in result["notes"]:
        print(f"resolve_endpoint: note: {note}", file=sys.stderr)
    print(f"resolve_endpoint: {endpoint} <- {result['tier']} ({result['source']})", file=sys.stderr)
    if args.format == "json":
        print(json.dumps(result, indent=2, sort_keys=True))
    elif result["value"] is None:  # a candidate won: never print its text
        if args.format == "text":
            print(f"candidate #{result['candidate']}")
        elif "value_ref" in result:  # indirect: the caller's shell expands ${VAR} at eval
            print(f'export {result["env_var"]}="{result["value_ref"]}"')
        else:
            print(f"export LAKEHOUSE_RESOLVED_CANDIDATE={result['candidate']}")
    elif args.format == "env":
        print(f"export {result['env_var']}={shlex.quote(result['value'])}")
    else:
        print(result["value"])
    return 0


# -- self-test -------------------------------------------------------------------------
def _call(argv, environ, cwd, probe=None, ps=None):
    out, err = io.StringIO(), io.StringIO()
    kwargs = {}
    if probe is not None:
        kwargs["probe"] = probe
    if ps is not None:
        kwargs["ps"] = ps
    with redirect_stdout(out), redirect_stderr(err):
        code = run(argv, environ, cwd, cwd, **kwargs)
    return code, out.getvalue(), err.getvalue()


def _probe_set(*reachable):
    """A fake probe that answers only for the given (host, port) pairs, and records calls."""
    calls: list[tuple[str, int]] = []

    def probe(host, port, timeout):
        calls.append((host, port))
        return (host, port) in reachable
    probe.calls = calls
    return probe


def self_test() -> int:
    failures: list[str] = []

    def expect(label, cond, detail=""):
        print(f"  {'ok  ' if cond else 'FAIL'} {label}")
        if not cond:
            failures.append(f"{label}: {detail}")

    nothing = _probe_set()
    no_docker = lambda timeout: None  # noqa: E731
    with tempfile.TemporaryDirectory() as tmp:
        env = {"LAKEHOUSE_PROFILE_DIR": tmp}

        # config-wins: env value beats a reachable preferred default and candidates.
        probe = _probe_set(("localhost", 15002), ("cand.internal", 15002))
        code, out, err = _call(["spark.connect_uri", "--candidate", "cand.internal"],
                               {**env, "LAKEHOUSE_SPARK_CONNECT_URI": "sc://spark.internal:15010"}, tmp, probe)
        expect("config wins over candidates and a reachable preferred default",
               code == 0 and out.strip() == "sc://spark.internal:15010" and "<- config" in err,
               f"{code} {out!r} {err!r}")
        code, out, err = _call(["spark.connect_uri"],
                               {**env, "LAKEHOUSE_SPARK_CONNECT_URI": "sc://spark.internal:15010"}, tmp, nothing)
        expect("config stays authoritative when unreachable (warns, exit 0)",
               code == 0 and out.strip() == "sc://spark.internal:15010" and "still authoritative" in err,
               f"{code} {out!r} {err!r}")
        sel = os.path.join(tmp, "chosen.profile.toml")
        with open(sel, "w", encoding="utf-8") as fh:
            fh.write('[profile]\nname = "chosen"\ninvocation = "local"\n'
                     '[kafka]\nbootstrap_servers = "k1.internal:9093,k2.internal:9093"\n')
        code, out, err = _call(["kafka.bootstrap_servers", "--profile", "chosen"], env, tmp,
                               _probe_set(("k2.internal", 9093)))
        expect("config from the user-selected profile (--profile) wins",
               code == 0 and out.strip() == "k1.internal:9093,k2.internal:9093"
               and "profile selected by --profile" in err, f"{code} {out!r} {err!r}")
        code, out, err = _call(["kafka.bootstrap_servers", "--profile", "chosen"],
                               {**env, "LAKEHOUSE_KAFKA_BOOTSTRAP_SERVERS": ""}, tmp,
                               _probe_set(("localhost", 9092)))
        expect("empty env override unsets the profile value -> falls through to preferred default",
               code == 0 and out.strip() == "localhost:9092", f"{code} {out!r} {err!r}")

        # discovery-picks-reachable: first reachable --candidate, in order.
        probe = _probe_set(("b.internal", 15002), ("c.internal", 15003), ("localhost", 15002))
        code, out, err = _call(["spark.connect_uri", "--candidate", "a.internal", "--candidate",
                                "b.internal", "--candidate", "c.internal:15003"], env, tmp, probe)
        expect("discovery picks the first reachable candidate (before the preferred default), by position",
               code == 0 and out.strip() == "candidate #2" and "<- discover" in err
               and probe.calls == [("a.internal", 15002), ("b.internal", 15002)],
               f"{code} {out!r} {err!r} {probe.calls}")
        rows = [("lh-spark-1", "apache/spark:4.0.1", "0.0.0.0:15012->15002/tcp, :::15012->15002/tcp"),
                ("lh-airflow", "apache/airflow:3", "0.0.0.0:8080->8080/tcp")]
        code, out, err = _call(["spark.connect_uri", "--invocation", "docker"], env, tmp,
                               _probe_set(("localhost", 15012), ("localhost", 15002)), lambda t: rows)
        expect("docker discovery finds a non-standard published port (beats preferred default)",
               code == 0 and out.strip() == "sc://localhost:15012" and "docker ps" in err,
               f"{code} {out!r} {err!r}")
        rows2 = rows + [("spark-b", "apache/spark:4.0.1", "127.0.0.1:15013->15002/tcp")]
        code, out, err = _call(["spark.connect_uri", "--invocation", "docker"], env, tmp,
                               _probe_set(("localhost", 15012), ("127.0.0.1", 15013)), lambda t: rows2)
        expect("ambiguous docker discovery (both reachable) fails loud instead of picking one",
               code == 1 and out == "" and "ambiguous" in err and "LAKEHOUSE_SPARK_CONNECT_URI" in err,
               f"{code} {out!r} {err!r}")
        probe = _probe_set(("127.0.0.1", 15013), ("localhost", 15002))
        code, out, err = _call(["spark.connect_uri", "--invocation", "docker"], env, tmp, probe,
                               lambda t: rows2)
        expect("two matching containers, only one reachable -> still ambiguous, fails before probing",
               code == 1 and out == "" and "ambiguous" in err and probe.calls == [],
               f"{code} {out!r} {err!r} {probe.calls}")
        same = [("spark-a", "apache/spark:4.0.1", "0.0.0.0:15012->15002/tcp"),
                ("spark-b", "apache/spark:4.0.1", "0.0.0.0:15012->15002/tcp, :::15012->15002/tcp")]
        probe = _probe_set(("localhost", 15012))
        code, out, err = _call(["spark.connect_uri", "--invocation", "docker"], env, tmp, probe,
                               lambda t: same)
        expect("two distinct matching containers reporting the SAME host:port -> ambiguous, no probe",
               code == 1 and "ambiguous" in err and probe.calls == [], f"{code} {err!r} {probe.calls}")
        probe = _probe_set()
        code, out, err = _call(["spark.connect_uri", "--invocation", "docker"], env, tmp, probe,
                               lambda t: rows)
        expect("single unreachable docker match: exactly one docker probe, then the preferred default",
               code == 1 and probe.calls == [("localhost", 15012), ("localhost", 15002)]
               and "did not answer" in err, f"{code} {err!r} {probe.calls}")
        duck = [("duckdb", "duckdb/duckdb:1.1", "0.0.0.0:18080->8080/tcp"),
                ("lh-uc", "unitycatalog/unitycatalog:0.3", "0.0.0.0:18081->8080/tcp")]
        code, out, err = _call(["unity_catalog.rest_url", "--invocation", "docker"], env, tmp,
                               _probe_set(("localhost", 18081)), lambda t: duck)
        expect("docker hints match whole name/image tokens ('uc' does not match 'duckdb')",
               code == 0 and out.strip() == "http://localhost:18081/api/2.1/unity-catalog",
               f"{code} {out!r} {err!r}")
        code, out, err = _call(["spark.connect_uri"], env, tmp,
                               _probe_set(("localhost", 15012)), lambda t: rows)
        expect("docker is not consulted unless the invocation is docker",
               code == 1 and "invocation is not docker" in err, f"{code} {out!r} {err!r}")

        # Real socket: discovery and preferred default use a bounded TCP connect.
        srv = socket.socket()
        srv.bind(("127.0.0.1", 0))
        srv.listen(4)
        live = srv.getsockname()[1]
        dead_sock = socket.socket()
        dead_sock.bind(("127.0.0.1", 0))
        dead = dead_sock.getsockname()[1]
        dead_sock.close()
        try:
            code, out, err = _call(["mlflow.tracking_uri", "--candidate", f"127.0.0.1:{dead}",
                                    "--candidate", f"127.0.0.1:{live}", "--timeout", "0.3",
                                    "--format", "json"], env, tmp, ps=no_docker)
            res = json.loads(out) if code == 0 else {}
            expect("real TCP probe: unreachable candidate skipped, live one chosen (json output)",
                   res.get("candidate") == 2 and res.get("value") is None
                   and res.get("tier") == "discover" and str(live) not in out + err,
                   f"{code} {out!r} {err!r}")
        finally:
            srv.close()

        # preferred-default-when-unset-but-reachable.
        code, out, err = _call(["unity_catalog.rest_url", "--format", "env"], env, tmp,
                               _probe_set(("localhost", 8080)))
        expect("preferred default used when unset but reachable, labeled a preference",
               code == 0 and out.strip() == "export LAKEHOUSE_UNITY_CATALOG_REST_URL="
               "http://localhost:8080/api/2.1/unity-catalog"
               and "preferred_default" in err and "a preference, not a certainty" in err,
               f"{code} {out!r} {err!r}")

        probe = _probe_set(("mlflow.internal", 443))
        code, out, err = _call(["mlflow.tracking_uri"],
                               {**env, "LAKEHOUSE_MLFLOW_TRACKING_URI": "https://mlflow.internal"}, tmp, probe)
        expect("configured URL without a port probes its scheme default (https -> 443), no warning",
               code == 0 and probe.calls == [("mlflow.internal", 443)] and "authoritative" not in err,
               f"{code} {err!r} {probe.calls}")

        # Exactly one generic default per endpoint: the shipped template must agree.
        tmpl = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..",
                            "templates", "lakehouse.profile.example.toml")
        if os.path.isfile(tmpl) and lp.tomllib is not None:
            data, mismatch = lp.read_toml(tmpl), []
            for ep, (kind, _, port, _, _) in ENDPOINTS.items():
                sec, key = ep.split(".", 1)
                val = (data.get(sec) or {}).get(key)
                if not val or port is None:  # no generic default to disagree with
                    continue
                got = int(val.rsplit(":", 1)[1]) if kind == "hostports" else urlparse(val).port
                if got != port:
                    mismatch.append((ep, got, port))
            expect("preferred ports agree with templates/lakehouse.profile.example.toml "
                   "(one authoritative generic default)", not mismatch, str(mismatch))
        else:
            print("  skip template port parity (templates/ not found next to this skill)")

        # fail-loud-when-unreachable.
        for ep in ENDPOINTS:
            code, out, err = _call([ep], env, tmp, nothing)
            var = lp.env_var(*ep.split(".", 1))
            if not (code == 1 and out == "" and var in err and "refusing to assume" in err):
                expect(f"fail loud for {ep}", False, f"{code} {out!r} {err!r}")
                break
        else:
            expect(f"fail loud (exit 1, names the env var + profile key) for all {len(ENDPOINTS)} "
                   "endpoints when nothing is reachable", True)
        code, out, err = _call(["mlflow.gateway_uri"], env, tmp, _probe_set(("localhost", 5000)))
        expect("endpoint without a conventional port never borrows one (fails loud)",
               code == 1 and "no preferred default" in err, f"{code} {out!r} {err!r}")

        # never-guesses-identity: a single discoverable profile is NOT read without selection.
        with open(os.path.join(tmp, "only.profile.toml"), "w", encoding="utf-8") as fh:
            fh.write('[profile]\nname = "only"\ninvocation = "docker"\n'
                     '[spark]\nconnect_uri = "sc://only-profile.internal:15002"\ncontainer = "x"\n')
        os.remove(sel)
        probe = _probe_set(("only-profile.internal", 15002))
        code, out, err = _call(["spark.connect_uri"], env, tmp, probe, lambda t: rows)
        expect("never auto-selects a profile: lone discoverable profile ignored without selection",
               code == 1 and "only-profile" not in out + err and "never auto-selects" in err
               and probe.calls == [("localhost", 15002)], f"{code} {out!r} {err!r} {probe.calls}")
        ident = ["profile.name", "naming.default_catalog", "naming.schemas", "object_store.bucket",
                 "object_store.warehouse", "unity_catalog.token_env", "object_store.secret_key_env"]
        bad = []
        for key in ident:
            code, out, err = _call([key], {**env, "LAKEHOUSE_NAMING_DEFAULT_CATALOG": "cat-SENTINEL"},
                                   tmp, _probe_set(("localhost", 15002)))
            if not (code == 2 and out == "" and "identity/targeting" in err and "SENTINEL" not in err):
                bad.append((key, code, err))
        expect(f"refuses {len(ident)} identity/targeting keys (exit 2, value never shown)", not bad, str(bad))
        code, out, err = _call(["spark.connect_uri", "--profile", "nope"], env, tmp, nothing)
        expect("unknown --profile -> exit 2 (selection errors relayed, nothing guessed)",
               code == 2 and "no profile named" in err, f"{code} {err!r}")

        # env-format contract: eval'ing the output sets the endpoint variable in every tier.
        def eval_env(out_text, shell_env, var):
            script = out_text + f'\nprintf "%s|%s" "${{{var}-UNSET}}" "${{LAKEHOUSE_RESOLVED_CANDIDATE-UNSET}}"'
            r = subprocess.run(["/bin/sh", "-c", script], capture_output=True, text=True,
                               env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"), **shell_env})
            return r.stdout
        V = "LAKEHOUSE_SPARK_CONNECT_URI"
        contract = [
            ("config", ["spark.connect_uri"], {V: "sc://spark.internal:15010"}, _probe_set(), None,
             V, "sc://spark.internal:15010|UNSET"),
            ("docker discovery", ["spark.connect_uri", "--invocation", "docker"], {},
             _probe_set(("localhost", 15012)), lambda t: rows, V, "sc://localhost:15012|UNSET"),
            ("preferred default", ["spark.connect_uri"], {}, _probe_set(("localhost", 15002)), None,
             V, "sc://localhost:15002|UNSET"),
            ("--candidate-env host only", ["spark.connect_uri", "--candidate-env", "MY_SPARK"],
             {"MY_SPARK": "spark.box"}, _probe_set(("spark.box", 15002)), None,
             V, "sc://spark.box:15002|UNSET"),
            ("--candidate-env host:port, after a dead by-value candidate",
             ["spark.connect_uri", "--candidate", "dead.box", "--candidate-env", "MY_SPARK"],
             {"MY_SPARK": "spark.box:15099"}, _probe_set(("spark.box", 15099)), None,
             V, "sc://spark.box:15099|UNSET"),
            ("--candidate-env http path suffix", ["unity_catalog.rest_url", "--candidate-env", "UC"],
             {"UC": "[::1]"}, _probe_set(("::1", 8080)), None, "LAKEHOUSE_UNITY_CATALOG_REST_URL",
             "http://[::1]:8080/api/2.1/unity-catalog|UNSET"),
            ("--candidate-env host:port list kind", ["kafka.bootstrap_servers", "--candidate-env", "KB"],
             {"KB": "broker.box:9093"}, _probe_set(("broker.box", 9093)), None,
             "LAKEHOUSE_KAFKA_BOOTSTRAP_SERVERS", "broker.box:9093|UNSET"),
            ("--candidate by value (index only, endpoint var NOT set)",
             ["spark.connect_uri", "--candidate", "spark.box"], {}, _probe_set(("spark.box", 15002)),
             None, V, "UNSET|1"),
        ]
        bad = []
        for label, argv, extra, probe, ps_fn, var, want in contract:
            code, out, err = _call(argv + ["--format", "env"], {**env, **extra}, tmp, probe, ps_fn)
            shell_env = {k: v for k, v in extra.items() if not k.startswith("LAKEHOUSE_")}
            got = eval_env(out, shell_env, var) if code == 0 else f"exit {code}: {err}"
            if got != want:
                bad.append((label, want, got))
        expect(f"--format env contract: eval sets the endpoint variable for {len(contract) - 1} tier/"
               "candidate shapes; a by-value candidate win is index-only", not bad, str(bad))
        code, out, err = _call(["spark.connect_uri", "--candidate-env", "NOPE_UNSET"], env, tmp, nothing)
        code2, _, err2 = _call(["spark.connect_uri", "--candidate-env", "lower;rm"], env, tmp, nothing)
        expect("--candidate-env: unset var / invalid var name -> exit 2",
               code == 2 and "unset" in err and code2 == 2 and "lower" not in err2, f"{err!r} {err2!r}")

        # no-secret-echo: every error / output path stays free of credential material.
        secrets = ["hunter2pw", "tok3nvalue", "kafkapw", "k3yvalue", "alicepw", "SUPERSECRET"]
        leaky = [
            ("unity_catalog.rest_url", {"LAKEHOUSE_UNITY_CATALOG_REST_URL":
                                        "https://alice:hunter2pw@uc.internal/api/2.1/unity-catalog"}, []),
            ("spark.connect_uri", {"LAKEHOUSE_SPARK_CONNECT_URI":
                                   "sc://spark.internal:15002/;user_id=me;token=tok3nvalue"}, []),
            ("kafka.bootstrap_servers", {"LAKEHOUSE_KAFKA_BOOTSTRAP_SERVERS": "carol:kafkapw@k1:9092"}, []),
            ("delta_sharing.endpoint", {"LAKEHOUSE_DELTA_SHARING_ENDPOINT":
                                        "https://share.internal/ds?api_key=k3yvalue"}, []),
            ("mlflow.tracking_uri", {"LAKEHOUSE_MLFLOW_TRACKING_URI": "http://bob:hunter2pw@[::1"}, []),
            ("airflow.base_url", {"LAKEHOUSE_AIRFLOW_BASE_URL": "http://AIRFLOW_HOST:8080?x=hunter2pw"}, []),
            ("spark.connect_uri", {}, ["alice:alicepw@h:15002"]),
            ("spark.connect_uri", {}, ["h:15002/?token=tok3nvalue"]),
            ("spark.connect_uri", {}, ["hunter2pw\n:1"]),
        ]
        bad = []
        for ep, extra, cands in leaky:
            argv = [ep] + [a for c in cands for a in ("--candidate", c)]
            for fmt in ("text", "json", "env"):
                code, out, err = _call(argv + ["--format", fmt],
                                       {**env, **extra, "LAKEHOUSE_UC_TOKEN": "SUPERSECRET"}, tmp,
                                       _probe_set(("localhost", 15002)))
                if code == 0 or "Traceback" in err or any(s in out + err for s in secrets):
                    bad.append((ep, fmt, code, out + err))
        leaky_cands = [["h:15002;token=tok3nvalue"], ["h:15002&k3yvalue"], ["h#hunter2pw:15002"],
                       ["k1:9092,kafkapw:9092"], ["[kafkapw]:15002"]]
        for cands in leaky_cands:
            argv = ["spark.connect_uri"] + [a for c in cands for a in ("--candidate", c)]
            for fmt in ("text", "json", "env"):
                code, out, err = _call(argv + ["--format", fmt], env, tmp, _probe_set(("localhost", 15002)))
                if code != 2 or "Traceback" in err or any(s in out + err for s in secrets):
                    bad.append((cands, fmt, code, out + err))
        expect(f"{len(leaky) + len(leaky_cands)} credential-bearing / malformed config & candidate "
               "values x 3 formats rejected, no traceback, no secret in stdout/stderr",
               not bad, str(bad)[:400])
        # Canaries in SYNTACTICALLY VALID candidates: never echoed, reachable or not.
        canaries = ["canaryhunter2pw.internal:15002", "canary-tok3nvalue"]
        canary_env = {"CAND_A": "canaryenvsecret.internal", "CAND_B": "canary-envtok3n:15002"}
        bad = []
        for reach in ((), (("canaryhunter2pw.internal", 15002),), (("canary-tok3nvalue", 15002),),
                      (("canaryenvsecret.internal", 15002),), (("canary-envtok3n", 15002),)):
            for extra_env in ({}, {"LAKEHOUSE_SPARK_CONNECT_URI": "sc://spark.internal:15002"}):
                for inv in ([], ["--invocation", "docker"]):
                    for fmt in ("text", "json", "env"):
                        argv = ["spark.connect_uri", "--format", fmt, *inv]
                        for c in canaries:
                            argv += ["--candidate", c]
                        argv += ["--candidate-env", "CAND_A", "--candidate-env", "CAND_B"]
                        code, out, err = _call(argv, {**env, **canary_env, **extra_env}, tmp,
                                               _probe_set(*reach), lambda t: rows2)
                        expected = 0 if (reach or extra_env) else 1
                        if code != expected or "canary" in (out + err).lower():
                            bad.append((reach, extra_env, inv, fmt, code, out + err))
        expect("canary secrets in valid --candidate / --candidate-env values (reachable / "
               "unreachable, success / failure, 3 formats) never reach stdout or stderr",
               not bad, str(bad)[:400])
        bad = []
        for val in ("alice:alicepw@h:15002", "h:15002;token=tok3nvalue", " hunter2pw.x ",
                    "h\nhunter2pw", "a:1,kafkapw:2"):
            for fmt in ("text", "json", "env"):
                code, out, err = _call(["spark.connect_uri", "--candidate-env", "CAND", "--format", fmt],
                                       {**env, "CAND": val}, tmp, _probe_set(("localhost", 15002)))
                if code != 2 or "Traceback" in err or any(x in out + err for x in secrets):
                    bad.append((val, fmt, code, out + err))
        expect("5 credential-bearing / malformed --candidate-env values x 3 formats -> exit 2, "
               "value never shown", not bad, str(bad)[:400])
        code, out, err = _call(["unity_catalog.rest_url"],
                               {**env, "LAKEHOUSE_UC_TOKEN": "SUPERSECRET",
                                "LAKEHOUSE_UNITY_CATALOG_TOKEN_ENV": "LAKEHOUSE_UC_TOKEN"},
                               tmp, _probe_set(("localhost", 8080)))
        expect("successful resolution never prints referenced credential values",
               code == 0 and "SUPERSECRET" not in out + err, f"{code} {out!r} {err!r}")
        with open(os.path.join(tmp, "leak.profile.toml"), "w", encoding="utf-8") as fh:
            fh.write('[profile]\nname = "leak"\ninvocation = "local"\n'
                     '[mlflow]\ntracking_uri = "http://bob:hunter2pw@mlflow.internal:5000"\n')
        code, out, err = _call(["mlflow.tracking_uri", "--profile", "leak"], env, tmp, nothing)
        expect("credential in the selected profile file -> exit 1, value not echoed",
               code == 1 and "hunter2pw" not in out + err and "userinfo" in err, f"{code} {err!r}")
        code, out, err = _call(["hunter2pw.connect_uri"], env, tmp, nothing)
        expect("unknown endpoint argument -> exit 2 without echoing it",
               code == 2 and "hunter2pw" not in err, f"{code} {err!r}")

    for f in failures:
        print(f"self-test FAIL: {f}", file=sys.stderr)
    if failures:
        return 1
    print("self-test OK")
    return 0


def main() -> int:
    return run(sys.argv[1:], dict(os.environ), os.getcwd(), os.path.expanduser("~"))


if __name__ == "__main__":
    sys.exit(main())
