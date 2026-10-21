# Lakehouse connection profile

The skills in this kit are stack-agnostic: they never hardcode a host, port, container
name, or credential. Everything deployment-specific comes from a **lakehouse connection
profile** — a small TOML or `.env` file, or the equivalent environment variables. The
parent skill `lakehouse-core` loads and validates the profile with
[`skills/lakehouse-core/scripts/load_profile.py`](../skills/lakehouse-core/scripts/load_profile.py)
and **refuses to guess** which profile to use.

Templates:

- [`templates/lakehouse.profile.example.toml`](../templates/lakehouse.profile.example.toml)
- [`templates/lakehouse.profile.example.env`](../templates/lakehouse.profile.example.env)

Both encode the same contract; the loader normalizes them to identical output (its
`--self-test` checks this).

## Quickstart

```bash
cp templates/lakehouse.profile.example.toml ./dev.profile.toml   # or ~/.config/lakehouse/
$EDITOR dev.profile.toml                                          # replace every *_HOST placeholder
export LAKEHOUSE_UC_TOKEN=...                                     # secrets live in env vars only

python3 -I skills/lakehouse-core/scripts/load_profile.py --list              # what's discoverable
python3 -I skills/lakehouse-core/scripts/load_profile.py --profile dev       # validate + print JSON
eval "$(python3 -I skills/lakehouse-core/scripts/load_profile.py --profile dev --format env)"
```

## Formats

**TOML** (`*.profile.toml`, Python >= 3.11): one table per section, as in the template.

**Env** (`*.profile.env`, any Python 3): `KEY=VALUE` lines, `#` comments, optional
`export ` prefix, optional surrounding quotes. Each TOML key maps to one variable:

```
[section] key = value    <=>    LAKEHOUSE_<SECTION>_<KEY>=value      (upper-cased)
[unity_catalog] rest_url  <=>    LAKEHOUSE_UNITY_CATALOG_REST_URL
```

Lists are comma-separated (`LAKEHOUSE_NAMING_SCHEMAS=bronze,silver,gold`); booleans are
`true`/`false` (`1`/`0`, `yes`/`no` also accepted).

In both formats an **empty string means unset**, and a section whose values are all
empty is treated as absent.

## Selecting a profile — and never guessing

The loader picks a profile from the first of these that is set:

| # | Source | Meaning |
|---|---|---|
| 1 | `--file PATH` | explicit profile file (`.toml` or `.env`; relative to the cwd) |
| 2 | `--profile NAME` | the discovered profile whose `[profile].name` is `NAME` |
| 3 | `LAKEHOUSE_PROFILE_FILE` | env equivalent of `--file` |
| 4 | `LAKEHOUSE_PROFILE` | env equivalent of `--profile` |
| 5 | `LAKEHOUSE_PROFILE_NAME` | env-only profile: no file, all values from `LAKEHOUSE_<SECTION>_<KEY>` |

Passing both `--file` and `--profile`, or setting both `LAKEHOUSE_PROFILE_FILE` and
`LAKEHOUSE_PROFILE` (with no flag), is an error rather than a silent tie-break. A
`--profile` name that matches zero or several discovered files is also an error.

**Discovery** (for `--profile`, `--list`, and error messages) looks for
`*.profile.toml` / `*.profile.env` in `$LAKEHOUSE_PROFILE_DIR` (an `os.pathsep`-separated
list) if set, otherwise in the current directory and
`${XDG_CONFIG_HOME:-~/.config}/lakehouse/`. Files with `.example.` in the name are skipped.

If nothing selects a profile, the loader **does not** fall back to a discovered file —
not even when there is exactly one, or one named `default`. It exits `2`, lists the
candidates, and asks for `--profile`/`--file`. Agents must relay that list and let the
user choose (the same rule the Databricks toolkit applies to CLI profiles).

## Value precedence

1. Values from the selected file (none in env-only mode).
2. **Overridden** key-by-key by `LAKEHOUSE_<SECTION>_<KEY>` variables in the process
   environment. Overridden keys are listed in `_meta.env_overrides`; `--no-env-overrides`
   disables this layer. An override set to the empty string unsets the key.

Other `LAKEHOUSE_*` variables that do not map to a profile key (e.g. secret holders such as
`LAKEHOUSE_UC_TOKEN`) are ignored by the loader. Inside an `.env` *file*, unrecognized
variables produce a warning.

## Fields

`req` = required whenever the section is present. All sections except `[profile]` are
optional; use `load_profile.py --require a,b` to make sections mandatory for a task.

### `[profile]` (always required)

| Key | Req | Type | Description |
|---|---|---|---|
| `name` | ✅ | `[A-Za-z0-9][A-Za-z0-9._-]*` | Profile name; what users pass to `--profile` / `LAKEHOUSE_PROFILE`. |
| `invocation` | ✅ | `local` \| `docker` \| `k8s` | Default way engine CLIs are launched (see [Invocation modes](#invocation-modes)). |

### `[k8s]`

| Key | Req | Type | Description |
|---|---|---|---|
| `namespace` | when any effective invocation is `k8s` | string | Namespace for `kubectl exec`. |
| `context` | | string | kubectl context; empty = current context. |

### `[spark]`

| Key | Req | Type | Description |
|---|---|---|---|
| `connect_uri` | ✅ | `sc://host[:port][/;params]` | Spark Connect endpoint. |
| `invocation` | | `local` \| `docker` \| `k8s` | Per-section override of `[profile].invocation`. |
| `container` | when effective invocation is `docker` | string | Container running `spark-submit` / `spark-sql`. |
| `pod` | when effective invocation is `k8s` | string | Pod running the Spark CLIs. |
| `image_notes` | | string | Free-text quirks of the Spark image in use. |

### `[unity_catalog]`

| Key | Req | Type | Description |
|---|---|---|---|
| `rest_url` | ✅ | `http(s)://` URI | UC OSS REST base, e.g. `https://uc.example.com/api/2.1/unity-catalog`. |
| `iceberg_rest` | | `http(s)://` URI | UC's Iceberg REST catalog endpoint. |
| `token_env` | | env-var name | Variable holding the UC bearer token. |

### `[kafka]`

| Key | Req | Type | Description |
|---|---|---|---|
| `bootstrap_servers` | ✅ | `host:port[,host:port…]` | Kafka bootstrap list (whitespace around commas is normalized away). |
| `invocation`, `container`, `pod` | | | As for `[spark]`, for `kafka-*.sh` CLIs. |

### `[object_store]`

| Key | Req | Type | Description |
|---|---|---|---|
| `kind` | ✅ | `s3` | Store API. `s3` covers any S3-compatible store (SeaweedFS, MinIO, AWS S3, …). |
| `s3_endpoint` | ✅ | `http(s)://` URI | S3 API endpoint. |
| `bucket` | ✅ | string | Default bucket. |
| `warehouse` | ✅ | `s3://` or `s3a://` URI | Warehouse root for table data. |
| `region` | | string | Signing region, if the store needs one. |
| `path_style` | | bool | Path-style addressing (usually `true` for self-hosted stores). |
| `access_key_env` | | env-var name | Variable holding the access key ID. |
| `secret_key_env` | | env-var name | Variable holding the secret access key. |

### `[mlflow]`

| Key | Req | Type | Description |
|---|---|---|---|
| `tracking_uri` | ✅ | `http(s)://` URI | MLflow tracking server. |
| `gateway_uri` | | `http(s)://` URI | MLflow AI Gateway. |
| `invocation`, `container`, `pod` | | | As for `[spark]`, for the `mlflow` CLI. |

### `[airflow]`

| Key | Req | Type | Description |
|---|---|---|---|
| `base_url` | ✅ | `http(s)://` URI | Airflow webserver / API base. |
| `invocation`, `container`, `pod` | | | As for `[spark]`, for the `airflow` CLI. |
| `username_env`, `password_env` | | env-var names | Variables holding API basic-auth credentials. |

### `[delta_sharing]`

| Key | Req | Type | Description |
|---|---|---|---|
| `endpoint` | ✅ | `http(s)://` URI | Delta Sharing server endpoint. |
| `bearer_token_env` | | env-var name | Variable holding the recipient bearer token. |

### `[naming]`

| Key | Req | Type | Description |
|---|---|---|---|
| `default_catalog` | | string | Catalog skills use when the user doesn't name one. |
| `schemas` | | list of strings | Conventional schema names (e.g. medallion layers). |

## Secrets

Profiles never contain secrets. Credential fields end in `_env` and hold the **name** of an
environment variable (must match `[A-Z_][A-Z0-9_]*`). Keys that look like inline secrets
(`token`, `password`, `secret`, `secret_key`, `access_key`, `bearer_token`, `api_key`) are
rejected. The loader reports each referenced variable as `set` / `unset` in
`_meta.secrets` and never prints a value.

## Invocation modes

`invocation` controls only how a **CLI binary** is launched; network clients (Spark
Connect, UC REST, MLflow, S3) always talk to the profile endpoints directly.

| Mode | How a CLI runs | Needs |
|---|---|---|
| `local` | binary on `PATH` of the machine running the agent | the CLI installed locally |
| `docker` | `docker exec -i "<section>.container" <cmd>` | `<section>.container` |
| `k8s` | `kubectl [--context <k8s.context>] exec -i -n <k8s.namespace> <section>.pod -- <cmd>` | `<section>.pod`, `k8s.namespace` |

The profile-level `invocation` is the default; `spark`, `kafka`, `mlflow`, and `airflow`
can each override it (e.g. Kafka CLIs in a container, Spark via a local client). The
loader resolves and emits the **effective** `invocation` for each of those sections.
`lakehouse-core` ships an `lh_exec <section> <cmd…>` shell helper implementing this table.

Endpoints must be reachable from where the command runs: an in-network name that works
inside a container may not resolve on the host, and vice versa.

## Validation

`load_profile.py` checks, and reports **all** problems at once (exit `1`):

- required sections/keys (above), plus any sections passed via `--require`;
- types and shapes: invocation/kind enums, URI schemes, `host:port` lists, port ranges,
  booleans, env-var names;
- unfilled template placeholders — hosts like `SPARK_HOST` / `UC_HOST` (downgrade to
  warnings with `--allow-placeholders`, e.g. to lint the templates themselves);
- inline secrets;
- invocation prerequisites (`container` for `docker`, `pod` + `k8s.namespace` for `k8s`).

Unknown sections and keys produce warnings (forward compatibility), not errors.

| Exit | Meaning |
|---|---|
| `0` | valid; normalized output on stdout |
| `1` | invalid profile; problems on stderr |
| `2` | no or ambiguous selection, unreadable file, or usage error |

## Output

Default `--format json`: every present section with **all** its known keys (unset keys
are `null`), lists and booleans normalized, effective `invocation` filled in for CLI
sections, plus `_meta`:

```json
{
  "_meta": {"env_overrides": [], "secrets": {"LAKEHOUSE_UC_TOKEN": "set"},
            "selected_by": "--profile", "source": "/home/me/.config/lakehouse/dev.profile.toml",
            "warnings": []},
  "kafka": {"bootstrap_servers": "k1.internal:9092,k2.internal:9092", "container": null,
            "invocation": "local", "pod": null},
  "profile": {"invocation": "local", "name": "dev"},
  "unity_catalog": {"iceberg_rest": null, "rest_url": "https://uc.internal/api/2.1/unity-catalog",
                    "token_env": "LAKEHOUSE_UC_TOKEN"}
}
```

`--format env` prints `export LAKEHOUSE_<SECTION>_<KEY>=…` lines (shell-quoted, unset keys
omitted) for `eval "$(…)"`.
