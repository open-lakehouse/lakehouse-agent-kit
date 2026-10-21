#!/usr/bin/env python3
"""Load, validate, and normalize a lakehouse connection profile.

Stdlib-only; run as `python3 -I load_profile.py ...`. Never makes network calls and
never prints secret values. The contract is documented in docs/connection-profile.md.

Selection (first match wins; the loader NEVER auto-selects a discovered profile):
  1. --file PATH                 a *.toml or *.env profile file
  2. --profile NAME              a discovered profile whose [profile].name == NAME
  3. LAKEHOUSE_PROFILE_FILE      env equivalent of --file
  4. LAKEHOUSE_PROFILE           env equivalent of --profile
  5. LAKEHOUSE_PROFILE_NAME set  env-only profile built from LAKEHOUSE_<SECTION>_<KEY>
With no selection the loader lists the candidates it found and exits 2.

Values: the selected file, then LAKEHOUSE_<SECTION>_<KEY> process env vars override
individual keys (disable with --no-env-overrides).

Output: normalized JSON (default) or `export` lines (--format env) on stdout.
Exit status: 0 valid, 1 invalid profile, 2 no/ambiguous selection or usage error.
"""

from __future__ import annotations

import argparse
import glob
import io
import json
import os
import re
import shlex
import sys
import tempfile
from contextlib import redirect_stderr, redirect_stdout
from urllib.parse import urlparse

try:
    import tomllib  # Python >= 3.11
except ImportError:  # pragma: no cover - exercised only on old interpreters
    tomllib = None

INVOCATIONS = ("local", "docker", "k8s")
CLI_SECTIONS = ("spark", "kafka", "mlflow", "airflow")
CONTROL_VARS = ("LAKEHOUSE_PROFILE", "LAKEHOUSE_PROFILE_FILE", "LAKEHOUSE_PROFILE_DIR")

# section -> key -> (type, required). Types are validated in check_value().
SCHEMA: dict[str, dict[str, tuple[str, bool]]] = {
    "profile": {"name": ("name", True), "invocation": ("invocation", True)},
    "k8s": {"namespace": ("str", False), "context": ("str", False)},
    "spark": {"connect_uri": ("sc_uri", True), "invocation": ("invocation", False),
              "container": ("str", False), "pod": ("str", False), "image_notes": ("str", False)},
    "unity_catalog": {"rest_url": ("http_url", True), "iceberg_rest": ("http_url", False),
                      "token_env": ("env_name", False)},
    "kafka": {"bootstrap_servers": ("hostports", True), "invocation": ("invocation", False),
              "container": ("str", False), "pod": ("str", False)},
    "object_store": {"kind": ("store_kind", True), "s3_endpoint": ("http_url", True),
                     "bucket": ("str", True), "warehouse": ("s3_uri", True),
                     "region": ("str", False), "path_style": ("bool", False),
                     "access_key_env": ("env_name", False), "secret_key_env": ("env_name", False)},
    "mlflow": {"tracking_uri": ("http_url", True), "gateway_uri": ("http_url", False),
               "invocation": ("invocation", False), "container": ("str", False), "pod": ("str", False)},
    "airflow": {"base_url": ("http_url", True), "invocation": ("invocation", False),
                "container": ("str", False), "pod": ("str", False),
                "username_env": ("env_name", False), "password_env": ("env_name", False)},
    "delta_sharing": {"endpoint": ("http_url", True), "bearer_token_env": ("env_name", False)},
    "naming": {"default_catalog": ("str", False), "schemas": ("str_list", False)},
}
# Longest first so LAKEHOUSE_UNITY_CATALOG_* is not read as section "unity".
SECTIONS_BY_PREFIX = sorted(SCHEMA, key=len, reverse=True)
STORE_KINDS = ("s3",)
INLINE_SECRET_KEYS = {"token", "password", "secret", "secret_key", "access_key", "bearer_token", "api_key"}
ENV_NAME_RE = re.compile(r"^[A-Z_][A-Z0-9_]*$")
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
PLACEHOLDER_HOST_RE = re.compile(r"^[A-Z][A-Z0-9_]*_HOST$")


class ProfileError(Exception):
    """Selection/usage problem (exit 2)."""


def env_var(section: str, key: str) -> str:
    return f"LAKEHOUSE_{section}_{key}".upper()


# -- reading ---------------------------------------------------------------------------
def read_toml(path: str) -> dict:
    if tomllib is None:
        raise ProfileError(f"{path}: reading TOML needs Python >= 3.11 (tomllib); "
                           "use the .env profile variant on older interpreters")
    try:
        with open(path, "rb") as fh:
            return tomllib.load(fh)
    except tomllib.TOMLDecodeError as exc:
        raise ProfileError(f"{path}: invalid TOML: {exc}") from None


def split_env_name(name: str):
    """LAKEHOUSE_<SECTION>_<KEY> -> (section, key), or None if not a profile key."""
    if not name.startswith("LAKEHOUSE_") or name in CONTROL_VARS:
        return None
    rest = name[len("LAKEHOUSE_"):].lower()
    for section in SECTIONS_BY_PREFIX:
        if rest.startswith(section + "_") and rest[len(section) + 1:]:
            return section, rest[len(section) + 1:]
    return None


def parse_env_lines(text: str, path: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for n, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        if "=" not in line:
            raise ProfileError(f"{path}:{n}: expected KEY=VALUE, got {raw.strip()!r}")
        key, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        out[key.strip()] = value
    return out


def env_to_sections(env: dict[str, str], warnings: list[str], strict_origin: str = "") -> dict:
    """Turn LAKEHOUSE_<SECTION>_<KEY> vars into {section: {key: str}}. Unknown LAKEHOUSE_*
    names are ignored in the process env (they may be secrets) but warned about in a file."""
    data: dict = {}
    for name, value in env.items():
        parsed = split_env_name(name)
        if parsed is None:
            if strict_origin:
                warnings.append(f"{strict_origin}: ignoring unrecognized variable {name}")
            continue
        section, key = parsed
        # Inline-secret keys are kept from files (so validate() rejects them) but not from
        # the process env, where holding a secret is legitimate.
        if key not in SCHEMA[section] and not (strict_origin and key in INLINE_SECRET_KEYS):
            if strict_origin:
                warnings.append(f"{strict_origin}: ignoring unknown key {name}")
            continue
        data.setdefault(section, {})[key] = value
    return data


def read_profile_file(path: str, warnings: list[str]) -> dict:
    if not os.path.isfile(path):
        raise ProfileError(f"profile file not found: {path}")
    if path.endswith(".toml"):
        return read_toml(path)
    if path.endswith(".env"):
        with open(path, encoding="utf-8") as fh:
            return env_to_sections(parse_env_lines(fh.read(), path), warnings, strict_origin=path)
    raise ProfileError(f"{path}: unsupported profile format (expected .toml or .env)")


# -- discovery / selection -------------------------------------------------------------
def search_dirs(environ: dict, cwd: str, home: str) -> list[str]:
    if environ.get("LAKEHOUSE_PROFILE_DIR"):
        return [d for d in environ["LAKEHOUSE_PROFILE_DIR"].split(os.pathsep) if d]
    xdg = environ.get("XDG_CONFIG_HOME") or os.path.join(home, ".config")
    return [cwd, os.path.join(xdg, "lakehouse")]


def discover(environ: dict, cwd: str, home: str) -> list[tuple[str, str]]:
    """Return [(path, profile name or '<unreadable: …>')] for every candidate profile file."""
    found: list[tuple[str, str]] = []
    seen = set()
    for d in search_dirs(environ, cwd, home):
        for pattern in ("*.profile.toml", "*.profile.env"):
            for path in sorted(glob.glob(os.path.join(d, pattern))):
                real = os.path.realpath(path)
                if ".example." in os.path.basename(path) or real in seen:
                    continue
                seen.add(real)
                try:
                    name = read_profile_file(path, []).get("profile", {}).get("name", "")
                    found.append((path, str(name) or "<no [profile].name>"))
                except ProfileError as exc:
                    found.append((path, f"<unreadable: {exc}>"))
    return found


def candidates_text(cands: list[tuple[str, str]], dirs: list[str]) -> str:
    if not cands:
        return f"  (no *.profile.toml / *.profile.env files in: {', '.join(dirs)})"
    return "\n".join(f"  - {name}  ({path})" for path, name in cands)


def select(args, environ: dict, cwd: str, home: str):
    """Return (path or None, selected_by). Raises ProfileError when nothing is chosen."""
    if args.file and args.profile:
        raise ProfileError("pass only one of --file and --profile")
    if environ.get("LAKEHOUSE_PROFILE_FILE") and environ.get("LAKEHOUSE_PROFILE") \
            and not (args.file or args.profile):
        raise ProfileError("both LAKEHOUSE_PROFILE_FILE and LAKEHOUSE_PROFILE are set; unset one")
    if args.file:
        return args.file, "--file"
    name = args.profile
    by = "--profile"
    if not name and environ.get("LAKEHOUSE_PROFILE_FILE"):
        return environ["LAKEHOUSE_PROFILE_FILE"], "LAKEHOUSE_PROFILE_FILE"
    if not name and environ.get("LAKEHOUSE_PROFILE"):
        name, by = environ["LAKEHOUSE_PROFILE"], "LAKEHOUSE_PROFILE"
    if name:
        cands = discover(environ, cwd, home)
        matches = [p for p, n in cands if n == name]
        if len(matches) == 1:
            return matches[0], by
        dirs = search_dirs(environ, cwd, home)
        if not matches:
            raise ProfileError(f"no profile named '{name}' ({by}). Candidates:\n"
                               + candidates_text(cands, dirs))
        raise ProfileError(f"profile name '{name}' is ambiguous; choose one with --file:\n"
                           + "\n".join(f"  - {p}" for p in matches))
    if environ.get("LAKEHOUSE_PROFILE_NAME"):
        return None, "env (LAKEHOUSE_PROFILE_NAME)"
    cands = discover(environ, cwd, home)
    raise ProfileError(
        "no lakehouse profile selected — refusing to guess. Ask the user which profile to use, "
        "then pass --profile <name> or --file <path> (or set LAKEHOUSE_PROFILE / "
        "LAKEHOUSE_PROFILE_FILE). Candidates:\n" + candidates_text(cands, search_dirs(environ, cwd, home)))


# -- validation ------------------------------------------------------------------------
def hosts_of(kind: str, value: str) -> list[str]:
    if kind == "hostports":
        return [hp.rsplit(":", 1)[0].strip("[]") for hp in value.split(",")]
    # urlparse().hostname lower-cases; keep case so *_HOST placeholders are detectable.
    netloc = urlparse(value).netloc.rsplit("@", 1)[-1]
    if netloc.startswith("[") and "]" in netloc:  # IPv6 literal
        return [netloc[1:netloc.index("]")]]
    return [netloc.split(":", 1)[0]]


def check_value(where: str, kind: str, value, errors: list[str]):
    """Validate and normalize one value. Returns the normalized value (or None)."""
    if kind == "bool":
        if isinstance(value, bool):
            return value
        low = str(value).strip().lower()
        if low in ("true", "1", "yes"):
            return True
        if low in ("false", "0", "no"):
            return False
        errors.append(f"{where}: expected a boolean, got {value!r}")
        return None
    if kind == "str_list":
        items = value if isinstance(value, list) else str(value).split(",")
        if not all(isinstance(i, str) for i in items):
            errors.append(f"{where}: expected a list of strings")
            return None
        return [i.strip() for i in items if i.strip()]
    if not isinstance(value, str):
        errors.append(f"{where}: expected a string, got {type(value).__name__}")
        return None
    value = value.strip()
    if kind == "invocation":
        value = value.lower()
        if value not in INVOCATIONS:
            errors.append(f"{where}: must be one of {'|'.join(INVOCATIONS)}, got {value!r}")
    elif kind == "store_kind":
        value = value.lower()
        if value not in STORE_KINDS:
            errors.append(f"{where}: must be one of {'|'.join(STORE_KINDS)}, got {value!r}")
    elif kind == "name":
        if not NAME_RE.match(value):
            errors.append(f"{where}: {value!r} must match {NAME_RE.pattern}")
    elif kind == "env_name":
        if not ENV_NAME_RE.match(value):
            errors.append(f"{where}: must be an env var NAME (e.g. LAKEHOUSE_UC_TOKEN), "
                          "never the secret itself")
    elif kind in ("http_url", "sc_uri", "s3_uri"):
        schemes = {"http_url": ("http", "https"), "sc_uri": ("sc",), "s3_uri": ("s3", "s3a")}[kind]
        u = urlparse(value)
        try:
            u.port  # raises ValueError on a non-numeric/out-of-range port
            ok = u.scheme in schemes and bool(u.netloc)
        except ValueError:
            ok = False
        if not ok:
            errors.append(f"{where}: {value!r} must be a {'/'.join(s + '://' for s in schemes)} URI")
    elif kind == "hostports":
        parts = [p.strip() for p in value.split(",") if p.strip()]
        for p in parts:
            host, _, port = p.rpartition(":")
            if not host or not port.isdigit() or not 0 < int(port) <= 65535:
                errors.append(f"{where}: {p!r} must be host:port")
        value = ",".join(parts)
    return value


def validate(raw: dict, require: list[str], allow_placeholders: bool,
             errors: list[str], warnings: list[str]) -> dict:
    out: dict = {}
    for section, body in raw.items():
        if section not in SCHEMA:
            warnings.append(f"ignoring unknown section [{section}]")
            continue
        if not isinstance(body, dict):
            errors.append(f"[{section}] must be a table")
            continue
        for key in body:
            if key in INLINE_SECRET_KEYS:
                errors.append(f"{section}.{key}: inline secrets are not allowed; set "
                              f"{section}.{key}_env to the NAME of an env var instead")
            elif key not in SCHEMA[section]:
                warnings.append(f"ignoring unknown key {section}.{key}")
        # Empty strings mean unset; a section with nothing set is treated as absent.
        present = {k: v for k, v in body.items() if k in SCHEMA[section] and v != ""}
        if not present and section != "profile":
            continue
        norm: dict = {}
        for key, (kind, required) in SCHEMA[section].items():
            if key not in present:
                if required:
                    errors.append(f"{section}.{key}: required key is missing "
                                  f"(TOML [{section}] {key} or env {env_var(section, key)})")
                norm[key] = None
                continue
            norm[key] = check_value(f"{section}.{key}", kind, present[key], errors)
            if kind in ("http_url", "sc_uri", "hostports") and isinstance(norm[key], str):
                for host in hosts_of(kind, norm[key]):
                    if PLACEHOLDER_HOST_RE.match(host):
                        msg = f"{section}.{key}: host {host!r} is an unfilled template placeholder"
                        (warnings if allow_placeholders else errors).append(msg)
        out[section] = norm

    if "profile" not in out:
        errors.append("[profile] section is required (name, invocation)")
        return out
    default_inv = out["profile"].get("invocation")
    needs_ns = False
    for section in CLI_SECTIONS:
        if section not in out:
            continue
        inv = out[section].get("invocation") or default_inv
        out[section]["invocation"] = inv
        if inv == "docker" and not out[section].get("container"):
            errors.append(f"{section}.container: required when invocation is 'docker' "
                          f"(or set {section}.invocation = \"local\")")
        if inv == "k8s":
            needs_ns = True
            if not out[section].get("pod"):
                errors.append(f"{section}.pod: required when invocation is 'k8s'")
    if needs_ns and not (out.get("k8s") or {}).get("namespace"):
        errors.append("k8s.namespace: required when any invocation is 'k8s'")
    for section in require:
        if section not in SCHEMA:
            errors.append(f"--require: unknown section '{section}' (known: {', '.join(SCHEMA)})")
        elif section not in out:
            errors.append(f"[{section}] is required by the calling skill but is not configured")
    return out


def secret_status(profile: dict, environ: dict) -> dict:
    status = {}
    for section, body in profile.items():
        for key, value in body.items():
            if key.endswith("_env") and value:
                status[value] = "set" if environ.get(value) else "unset"
    return status


# -- output ----------------------------------------------------------------------------
def to_env_lines(profile: dict) -> str:
    lines = []
    for section, body in profile.items():
        for key, value in body.items():
            if value is None:
                continue
            if isinstance(value, bool):
                value = "true" if value else "false"
            elif isinstance(value, list):
                value = ",".join(value)
            lines.append(f"export {env_var(section, key)}={shlex.quote(value)}")
    return "\n".join(lines)


def run(argv: list[str], environ: dict, cwd: str, home: str) -> int:
    ap = argparse.ArgumentParser(prog="load_profile.py", description=__doc__.split("\n\n", 1)[0])
    ap.add_argument("--file", help="profile file (*.toml or *.env)")
    ap.add_argument("--profile", help="name of a discovered profile ([profile].name)")
    ap.add_argument("--require", default="", help="comma-separated sections the caller needs, e.g. spark,kafka")
    ap.add_argument("--format", choices=("json", "env"), default="json")
    ap.add_argument("--list", action="store_true", help="list discovered candidate profiles and exit")
    ap.add_argument("--no-env-overrides", action="store_true",
                    help="ignore LAKEHOUSE_<SECTION>_<KEY> process env vars")
    ap.add_argument("--allow-placeholders", action="store_true",
                    help="downgrade unfilled *_HOST template placeholders to warnings")
    ap.add_argument("--self-test", action="store_true", help="run built-in test cases and exit")
    args = ap.parse_args(argv)

    if args.self_test:
        return self_test()
    if args.list:
        print(candidates_text(discover(environ, cwd, home), search_dirs(environ, cwd, home)))
        return 0

    warnings: list[str] = []
    try:
        path, selected_by = select(args, environ, cwd, home)
        if path is not None and not os.path.isabs(path):
            path = os.path.join(cwd, path)
        raw = read_profile_file(path, warnings) if path else {}
    except ProfileError as exc:
        print(f"load_profile: {exc}", file=sys.stderr)
        return 2

    overrides = []
    if not args.no_env_overrides:
        for section, body in env_to_sections(environ, warnings).items():
            for key, value in body.items():
                raw.setdefault(section, {})[key] = value
                overrides.append(env_var(section, key))

    errors: list[str] = []
    require = [s.strip() for s in args.require.split(",") if s.strip()]
    profile = validate(raw, require, args.allow_placeholders, errors, warnings)
    for w in warnings:
        print(f"load_profile: warning: {w}", file=sys.stderr)
    if errors:
        where = path or "environment"
        print(f"load_profile: invalid profile ({where}):\n" + "\n".join(f"  - {e}" for e in errors),
              file=sys.stderr)
        return 1

    if args.format == "env":
        print(to_env_lines(profile))
        return 0
    profile["_meta"] = {
        "source": path or "environment",
        "selected_by": selected_by,
        "env_overrides": sorted(overrides),
        "secrets": secret_status(profile, environ),
        "warnings": warnings,
    }
    print(json.dumps(profile, indent=2, sort_keys=True))
    return 0


# -- self-test -------------------------------------------------------------------------
GOOD_TOML = """
[profile]
name = "dev"
invocation = "docker"
[spark]
connect_uri = "sc://spark.internal:15002"
container = "spark-1"
[kafka]
bootstrap_servers = "k1.internal:9092, k2.internal:9092"
invocation = "local"
[unity_catalog]
rest_url = "https://uc.internal/api/2.1/unity-catalog"
token_env = "LAKEHOUSE_UC_TOKEN"
[naming]
schemas = ["bronze", "gold"]
"""
BAD_TOML = """
[profile]
name = "broken"
invocation = "docker"
[spark]
connect_uri = "http://spark.internal:15002"
[kafka]
container = "kafka"
[unity_catalog]
rest_url = "http://UC_HOST:8080/api/2.1/unity-catalog"
token = "s3cr3t"
"""


def _call(argv, environ, cwd):
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        code = run(argv, environ, cwd, cwd)
    return code, out.getvalue(), err.getvalue()


def self_test() -> int:
    failures: list[str] = []

    def expect(label, cond, detail=""):
        print(f"  {'ok  ' if cond else 'FAIL'} {label}")
        if not cond:
            failures.append(f"{label}: {detail}")

    with tempfile.TemporaryDirectory() as tmp:
        for name, text in (("dev.profile.toml", GOOD_TOML), ("broken.profile.toml", BAD_TOML)):
            with open(os.path.join(tmp, name), "w", encoding="utf-8") as fh:
                fh.write(text)
        base = {"LAKEHOUSE_PROFILE_DIR": tmp, "LAKEHOUSE_UC_TOKEN": "x"}

        code, out, err = _call([], dict(base), tmp)
        expect("no selection -> exit 2, lists candidates, refuses to guess",
               code == 2 and "refusing to guess" in err and "dev" in err and "broken" in err, err)

        env = dict(base, LAKEHOUSE_KAFKA_BOOTSTRAP_SERVERS="k9.internal:9093")
        code, out, err = _call(["--profile", "dev", "--require", "spark,kafka"], env, tmp)
        data = json.loads(out) if code == 0 else {}
        expect("--profile dev -> valid, env override applied, invocations resolved",
               code == 0 and data["kafka"]["bootstrap_servers"] == "k9.internal:9093"
               and data["spark"]["invocation"] == "docker" and data["kafka"]["invocation"] == "local"
               and data["_meta"]["secrets"] == {"LAKEHOUSE_UC_TOKEN": "set"}
               and data["_meta"]["env_overrides"] == ["LAKEHOUSE_KAFKA_BOOTSTRAP_SERVERS"],
               err or out)

        code, out, err = _call(["--file", "broken.profile.toml"], dict(base), tmp)
        wanted = ("spark.connect_uri", "spark.container", "kafka.bootstrap_servers",
                  "unfilled template placeholder", "inline secrets")
        expect("invalid profile -> exit 1 naming every problem",
               code == 1 and all(w in err for w in wanted), err)

        code, out, err = _call(["--require", "mlflow"], dict(
            base, LAKEHOUSE_PROFILE_NAME="ci", LAKEHOUSE_PROFILE_INVOCATION="local",
            LAKEHOUSE_MLFLOW_TRACKING_URI="http://mlflow.internal:5000"), tmp)
        expect("env-only profile -> valid", code == 0 and json.loads(out)["profile"]["name"] == "ci",
               err or out)

        code, out, err = _call(["--profile", "nope"], dict(base), tmp)
        expect("unknown --profile -> exit 2 with candidates", code == 2 and "dev" in err, err)

        # When run from a repo checkout, both shipped templates must validate and agree.
        repo_templates = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "templates")
        toml_t = os.path.join(repo_templates, "lakehouse.profile.example.toml")
        env_t = os.path.join(repo_templates, "lakehouse.profile.example.env")
        if os.path.isfile(toml_t) and os.path.isfile(env_t) and tomllib is not None:
            results = []
            for t in (toml_t, env_t):
                code, out, err = _call(["--file", t, "--allow-placeholders", "--no-env-overrides"], {}, tmp)
                results.append((code, json.loads(out) if code == 0 else err))
            same = all(c == 0 for c, _ in results) and \
                {k: v for k, v in results[0][1].items() if k != "_meta"} == \
                {k: v for k, v in results[1][1].items() if k != "_meta"}
            expect("templates/*.example.toml and *.example.env validate and normalize identically", same,
                   str(results))
        else:
            print("  skip templates parity check (templates/ not found next to this skill)")

    if failures:
        print(f"self-test FAILED ({len(failures)}):\n" + "\n".join(failures), file=sys.stderr)
        return 1
    print("self-test OK")
    return 0


def main() -> int:
    return run(sys.argv[1:], dict(os.environ), os.getcwd(), os.path.expanduser("~"))


if __name__ == "__main__":
    sys.exit(main())
