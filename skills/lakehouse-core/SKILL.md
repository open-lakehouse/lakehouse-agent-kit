---
name: lakehouse-core
description: Load FIRST for any open-source lakehouse task (Apache Spark, Delta Lake, Apache Iceberg, Unity Catalog OSS, Kafka, Airflow, MLflow, Spark Declarative Pipelines, Delta Sharing, S3-compatible object stores). Parent/entry skill — resolves and validates the lakehouse connection profile (never guesses one), resolves service endpoints (config → discover → preferred default → fail loud) via one shared resolver, checks tool version floors, explains the local/docker/k8s invocation modes, and routes to the matching product skill. All endpoints come from the profile, never hardcoded hosts.
license: Apache-2.0
compatibility: Requires python3 >= 3.8 for scripts/load_profile.py (>= 3.11 to read TOML profiles; .env profiles need no tomllib); verified on Python 3.12.3, 2026-10-21. Engine CLIs and their version floors are declared by each product skill.
metadata:
  version: "0.1.0"
allowed-tools: [Bash, Read]
---

# Lakehouse core — connection profile, version floors, routing

This is the parent skill of the Lakehouse Agent Kit. Every product skill opens with
"FIRST: use the parent skill `lakehouse-core`". Do the four steps below **in order**
before running anything against an engine, then load the product skill from the routing
table.

1. **Select and load** the lakehouse connection profile — the user chooses; you never guess.
2. **Validate** it for the sections the task needs.
3. **Resolve** each endpoint the task needs with the shared resolver (§4).
4. **Floor-check** the tool versions the product skill requires.

Every endpoint comes from the profile or the shared resolver; container/pod names and
credential references come from the profile. Do not type a host, port, or container name
into a command.
The full field reference is `docs/connection-profile.md` in the toolkit repo; the
contract templates are `templates/lakehouse.profile.example.toml` and
`templates/lakehouse.profile.example.env`.

## 1. Locate and load the profile

A profile is one `*.profile.toml` or `*.profile.env` file (or the equivalent env vars).
`scripts/load_profile.py` (path relative to this skill's directory) is the single
loader — use it rather than parsing profiles by hand.

### Selection precedence (first match wins)

| # | Source | Meaning |
|---|---|---|
| 1 | `--file PATH` | explicit profile file (`.toml` or `.env`) |
| 2 | `--profile NAME` | discovered profile whose `[profile].name` is `NAME` |
| 3 | `LAKEHOUSE_PROFILE_FILE` | env equivalent of `--file` |
| 4 | `LAKEHOUSE_PROFILE` | env equivalent of `--profile` |
| 5 | `LAKEHOUSE_PROFILE_NAME` set | env-only profile built from `LAKEHOUSE_<SECTION>_<KEY>` vars |

Discovery (used for `--profile`, `--list`, and the "no selection" message) searches
`$LAKEHOUSE_PROFILE_DIR` if set (path-separator list), otherwise the current directory
and `${XDG_CONFIG_HOME:-~/.config}/lakehouse/`. Files named `*.example.*` are ignored.

### Value precedence

After the file is read, any `LAKEHOUSE_<SECTION>_<KEY>` variable in the process
environment **overrides** that single key (e.g. `LAKEHOUSE_KAFKA_BOOTSTRAP_SERVERS`
overrides `[kafka].bootstrap_servers`). The loader reports which keys were overridden in
`_meta.env_overrides`. An override set to the empty string unsets the key.

### NEVER auto-select a profile

If the user has not named a profile (no flag, no `LAKEHOUSE_PROFILE*` variable), **stop
and ask**. Do not pick the only candidate, the one called `default`, or the most recently
modified file — even if there is exactly one. Show the user the candidates and let them
choose:

```bash
python3 -I scripts/load_profile.py --list
```

Then load the profile they chose:

```bash
python3 -I scripts/load_profile.py --profile "<name-the-user-chose>" --require spark
# or
python3 -I scripts/load_profile.py --file "<path-the-user-gave>" --require spark
```

Running the loader with no selection exits `2` with a "refusing to guess" message and the
candidate list — relay that list to the user verbatim and ask which one to use. If there
are no candidates, point the user at the templates and `docs/connection-profile.md`; do
not invent endpoint values.

Once the user has chosen, reuse that choice for the rest of the session (pass the same
`--profile`/`--file` every time). Do not switch profiles without asking.

## 2. Validate

The loader validates as it loads. Exit codes:

| Exit | Meaning | What to do |
|---|---|---|
| `0` | valid — normalized JSON (or `export` lines) on stdout | continue |
| `1` | invalid profile — every problem listed on stderr as `section.key: …` | show the list to the user; ask them to fix the file/env; do not patch values yourself |
| `2` | no / ambiguous selection, unreadable file, or usage error | ask the user to choose (see §1), or to fix the file path / permissions named in the message |

What it checks:

- `[profile]` is required: `name` and `invocation` (`local` \| `docker` \| `k8s`).
  Values are trimmed first; blank values count as unset.
- Every other section is optional, but a section that is present must have its required
  keys (e.g. `[spark].connect_uri`, `[kafka].bootstrap_servers`,
  `[unity_catalog].rest_url`). A section whose values are all empty counts as absent.
- `--require a,b` makes sections mandatory for the calling skill — always pass the
  sections the product skill needs so a missing one fails fast.
- URI shapes (`sc://` for Spark Connect, `http(s)://`, `s3://`/`s3a://` warehouses,
  `host:port` lists for Kafka) and port ranges.
- Unfilled template placeholders (`SPARK_HOST`, `UC_HOST`, …) are errors.
- No inline secrets: keys like `token`/`password`/`secret_key`, URI userinfo
  (`user:pass@`), and credential URI parameters (`?token=`, `;token=`) are rejected;
  credentials are referenced by env-var **name** via `*_env` keys.
- Invocation requirements: `docker` needs the section's `container`, `k8s` needs its
  `pod` plus `[k8s].namespace`.

Read values from the JSON, e.g. with `jq`, or export them into the shell:

```bash
eval "$(python3 -I scripts/load_profile.py --profile "$LAKEHOUSE_PROFILE" --require spark --format env)"
echo "$LAKEHOUSE_SPARK_CONNECT_URI"      # value from the profile, never typed by hand
```

`_meta.secrets` reports each referenced credential variable as `set`/`unset` without ever
printing its value. If a needed one is `unset`, ask the user to export it — never ask
them to paste the secret into the chat or a file.

## 3. Invocation modes

`[profile].invocation` says how engine CLIs run; the CLI sections (`spark`, `kafka`,
`mlflow`, `airflow`) may override it with their own `invocation`, and the loader emits
the **effective** mode per section. Wrap CLI calls instead of hand-writing one
deployment's command:

```bash
# usage: lh_exec <section> <cmd> [args...]   (after `eval "$(... --format env)"`)
lh_exec() {
  local sec; sec=$(printf '%s' "$1" | tr '[:lower:]' '[:upper:]'); shift
  local inv_var="LAKEHOUSE_${sec}_INVOCATION" ctr_var="LAKEHOUSE_${sec}_CONTAINER" pod_var="LAKEHOUSE_${sec}_POD"
  case "${!inv_var}" in
    local)  "$@" ;;
    docker) docker exec -i "${!ctr_var}" "$@" ;;
    k8s)    kubectl ${LAKEHOUSE_K8S_CONTEXT:+--context "$LAKEHOUSE_K8S_CONTEXT"} \
              exec -i -n "$LAKEHOUSE_K8S_NAMESPACE" "${!pod_var}" -- "$@" ;;
    *)      echo "unknown invocation for $sec: ${!inv_var}" >&2; return 2 ;;
  esac
}
lh_exec kafka kafka-topics.sh --bootstrap-server "$LAKEHOUSE_KAFKA_BOOTSTRAP_SERVERS" --list
```

Network clients (Spark Connect, UC REST, MLflow tracking, S3) connect to the profile
endpoint directly in every mode — the invocation mode only affects how a **CLI binary**
is launched. Endpoints in a profile must be reachable from wherever the command runs
(inside a container, an in-cluster name may differ from the host-side name).

## 4. Resolve endpoints (discover, don't hardcode; prefer, don't guess)

`scripts/resolve_endpoint.py` is the **one** implementation of the resolution order.
Product skills call it for every service endpoint they use — never re-implement
discovery and never type a host:port:

```bash
eval "$(python3 -I scripts/resolve_endpoint.py spark.connect_uri --profile "<name-the-user-chose>" --format env)"
spark-sql --remote "$LAKEHOUSE_SPARK_CONNECT_URI"
```

| Tier | What it does |
|---|---|
| 1. Config | `LAKEHOUSE_<SECTION>_<KEY>`, or the key in the profile the user selected. Authoritative — returned even if a probe fails (with a warning; do not substitute another endpoint). |
| 2. Discover | One bounded TCP probe (default 0.5 s, `--timeout`) per specific candidate: each `--candidate-env VAR` / `--candidate HOST[:PORT]` in command-line order (first reachable wins), then — when the effective invocation is `docker` — the port `docker ps` reports as published for the service's conventional container port (matched on the configured `container`, else whole name/image tokens). More than one matching published endpoint ⇒ ambiguous ⇒ fail, even if only one answers. |
| 3. Preferred default | The conventional port on the local host (table below), used **only if reachable** and reported as a preference, not a certainty. |
| 4. Fail loud | Exit `1` naming the exact env var and profile key to set. Relay it; never guess. |

It never scans port ranges or arbitrary hosts. Output: the value on stdout (`--format
text`), a JSON object with the `tier` and `source` (`--format json`), or an `export` line
(`--format env`); provenance goes to stderr. Exit `0` resolved, `1` unresolved or invalid
configured value (messages never contain the value), `2` usage error.

**`--format env` is the consumable contract** — `eval` it to set the endpoint variable:

| Winning tier | `--format env` output |
|---|---|
| config, docker discovery, preferred default | `export LAKEHOUSE_<SECTION>_<KEY>='<value>'` |
| `--candidate-env VAR` | `export LAKEHOUSE_<SECTION>_<KEY>="<scheme>://${VAR}[:<port>][<path>]"` — indirect; your shell expands `$VAR`, the resolver never prints it |
| `--candidate HOST[:PORT]` (by value) | `export LAKEHOUSE_RESOLVED_CANDIDATE=N` only — the endpoint variable is **not** set |

Candidate text is never echoed in any format. So pass a candidate you want to *use* as the
name of an exported variable (`export MY_SPARK=…; … --candidate-env MY_SPARK`). A by-value
`--candidate` win only reports its position (`candidate #N` in text, `"candidate": N,
"value": null` in JSON).

### Preferred defaults (used only after config and discovery)

| Endpoint (`section.key`) | Preferred port | Shape |
|---|---|---|
| `spark.connect_uri` | 15002 | `sc://<host>:<port>` |
| `kafka.bootstrap_servers` | 9092 | `<host>:<port>` |
| `unity_catalog.rest_url` / `iceberg_rest` | 8080 (upstream UC OSS default) | `http://<host>:<port>/api/2.1/unity-catalog[/iceberg/v1]` |
| `mlflow.tracking_uri` | 5000 | `http://<host>:<port>` |
| `mlflow.gateway_uri` | — (config or `--candidate` only) | `http://<host>:<port>` |
| `airflow.base_url` | 8080 | `http://<host>:<port>` |
| `object_store.s3_endpoint` | 9000 (MinIO / common S3-compatible; SeaweedFS's 8333 belongs in that deployment's profile) | `http://<host>:<port>` |
| `delta_sharing.endpoint` | 8443 | `https://<host>:<port>` |

These are generic upstream conventions and match `templates/lakehouse.profile.example.*`
(the resolver self-test checks this). A deployment that remaps a port — e.g. UC on 8081 to
avoid Airflow's 8080 — records it in its own profile, never here; the open-lakehouse
reference stack's profile is `templates/reference-stack.profile.{toml,env}` in the repo.
Note the generic UC and Airflow defaults share 8080: a reachable preferred default proves
only that *something* listens there.

**Topology only.** The resolver answers *where a service listens*. It never selects a
profile — with no `--profile`/`--file`/`LAKEHOUSE_PROFILE*` it reads only `LAKEHOUSE_*`
env vars, not profile files — and it refuses identity/targeting keys (`profile.name`,
`naming.*`, `object_store.bucket`/`warehouse`, any `*_env` credential reference) with
exit `2`. Those are confirmed with the user (§1). A resolved endpoint is not a reason to
pick a catalog, schema, or bucket.

In a skill body, a port literal is allowed only as a labeled preferred default next to a
call to this resolver — the linter enforces it (see `CONTRIBUTING.md`).

## 5. Tool / version floor check

Each product skill declares its floors in its `compatibility` field (e.g. "Requires
spark-submit >= X.Y"). Before the first engine command in a session:

1. Read the floor from the product skill's `compatibility`.
2. Get the installed version **through the profile's invocation mode**.
3. Compare. Absent ⇒ tell the user how to install it. Present-but-too-old ⇒ tell the user
   to upgrade and **stop** — do not work around a version gap silently.

`scripts/version_floor.py` does the comparison in stdlib Python (portable — no GNU
`sort -V`). Pass it the raw `--version` output; it extracts the version itself:

```bash
python3 -I scripts/version_floor.py "$(lh_exec spark spark-submit --version 2>&1)" "<floor from compatibility>"
# exit 0: OK (have >= floor)   1: TOO OLD — tell the user to upgrade, stop
# exit 2: no version found (tool missing / not reachable via this invocation mode) or bad floor
```

Comparison is numeric per component (`2.21` > `2.9`, `4.0` == `4.0.0`); a pre-release
suffix compares as its release.

Version commands (run via `lh_exec <section> …`):

| Tool | Command | Section |
|---|---|---|
| Spark | `spark-submit --version` (also `pyspark.__version__` for Connect clients) | `spark` |
| Kafka | `kafka-topics.sh --version` | `kafka` |
| Airflow | `airflow version` | `airflow` |
| MLflow | `mlflow --version` | `mlflow` |
| Python | `python3 --version` (this skill: >= 3.11 for TOML profiles) | local |

For HTTP-only services (Unity Catalog, Delta Sharing, object store) the product skill
documents how to read the server version from its API, if it exposes one.

## 6. Route to the product skill

After §1–§5, load the product skill that matches the task. Product skills are being
ported in Phase 2 of `IMPLEMENTATION_PLAN.md`; until a skill exists, say so rather than
improvising engine-specific commands.

| Task intent | Product skill | Profile sections to `--require` |
|---|---|---|
| Run Spark jobs, Spark Connect sessions, `spark-submit` / `spark-sql`, Spark config | `spark` | `spark` |
| Delta tables: MERGE, OPTIMIZE, VACUUM, time travel, CDF, UniForm | `delta` | `spark`, `object_store` |
| Iceberg tables: snapshots, compaction, schema/partition evolution | `iceberg` | `spark`, `unity_catalog`, `object_store` |
| Unity Catalog OSS: catalogs/schemas/tables, REST API, credential vending, engine wiring | `unity-catalog` | `unity_catalog` |
| Kafka topics, connectors, Structured Streaming read/write | `kafka` | `kafka` (+ `spark` for streaming) |
| Airflow DAG authoring, Airflow 2→3 changes, triggering Spark from DAGs | `airflow` | `airflow` |
| MLflow tracking, autologging, model registry, AI gateway | `mlflow` | `mlflow` |
| Spark Declarative Pipelines (`pyspark.pipelines`, pipeline CLI) | `sdp` | `spark`, `unity_catalog` |
| Open Delta Sharing: sharing server, recipient profiles, reading shares | `delta-sharing` | `delta_sharing` |
| S3-compatible object store (SeaweedFS, MinIO, AWS S3): buckets, paths, credentials | `object-store` | `object_store` |

Tasks that span engines (e.g. Kafka → Spark streaming → Delta on UC) load each relevant
product skill, but resolve the profile **once** here and pass `--require` with the union
of sections.

Operations bound to one concrete deployment (start/stop a compose stack, its dashboard,
its proxies) are not product skills — they live under `reference-stack/` in the repo
and are opt-in.

## Common pitfalls

- Treating the only discovered profile as "obviously the right one". It isn't — ask.
- Copying an endpoint from a previous session, a README, or a container listing instead
  of the profile or the resolver. Config is authoritative; everything else is resolved.
- Treating a preferred default as fact. If `resolve_endpoint.py` fails, report how to
  set the value — do not retry with a port you remember.
- Editing the user's profile to "fix" validation errors. Report them; the user fixes them.
- Printing or echoing credential variables while debugging. Check `_meta.secrets` instead.
- Using the host-side endpoint inside a container (or vice versa) — if a call from
  `invocation=docker`/`k8s` cannot connect, the profile likely needs the in-network name.

## See also

- `scripts/load_profile.py --help` for every flag; `--self-test` to check the loader.
- `scripts/version_floor.py --self-test` to check the version comparison.
- `scripts/resolve_endpoint.py --help`; `--self-test` to check the resolution order.
- `docs/connection-profile.md` for the field-by-field reference.
