"""
Airbyte MCP Server

Exposes the Airbyte API as MCP tools for managing workspaces, sources,
destinations, connections, and sync jobs.

Authentication: Set AIRBYTE_CLIENT_ID and AIRBYTE_CLIENT_SECRET env vars.
Optionally set AIRBYTE_API_URL for self-hosted (defaults to Airbyte Cloud).
"""

import asyncio
import json
import os
from datetime import datetime, timedelta, timezone

import httpx
from mcp.server.fastmcp import FastMCP

# --- Configuration ---

AIRBYTE_API_URL = os.environ.get("AIRBYTE_API_URL", "https://api.airbyte.com/v1")
AIRBYTE_CLIENT_ID = os.environ.get("AIRBYTE_CLIENT_ID", "")
AIRBYTE_CLIENT_SECRET = os.environ.get("AIRBYTE_CLIENT_SECRET", "")
# Airbyte's internal config API. Not part of the public API, but it accepts the same
# application bearer token and is the only place that returns job failure reasons and
# runs a source check (the public POST /sources/{id}/check is 403 for app tokens).
AIRBYTE_CONFIG_API_URL = os.environ.get("AIRBYTE_CONFIG_API_URL", "https://cloud.airbyte.com/api/v1")

# --- Token Management ---

_token_cache: dict = {"token": None, "expires_at": None}


async def _get_token() -> str:
    """Get a valid bearer token, refreshing if needed."""
    now = datetime.now(timezone.utc)
    if _token_cache["token"] and _token_cache["expires_at"] and now < _token_cache["expires_at"]:
        return _token_cache["token"]

    token_url = AIRBYTE_API_URL.rstrip("/") + "/applications/token"
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            token_url,
            json={
                "client_id": AIRBYTE_CLIENT_ID,
                "client_secret": AIRBYTE_CLIENT_SECRET,
            },
        )
        resp.raise_for_status()
        data = resp.json()

    _token_cache["token"] = data["access_token"]
    # Tokens last 60 min, refresh at 55 min to be safe
    _token_cache["expires_at"] = now + timedelta(minutes=55)
    return _token_cache["token"]


async def _request(
    method: str,
    path: str,
    body: dict | None = None,
    params: dict | None = None,
    base: str | None = None,
    timeout: float = 60,
) -> dict:
    """Make an authenticated request to the Airbyte API. Retries once on 401 with a fresh token."""
    token = await _get_token()
    url = (base or AIRBYTE_API_URL).rstrip("/") + path
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.request(method, url, headers=headers, json=body, params=params)
        if resp.status_code == 401:
            # Token might be stale: clear cache and retry once
            _token_cache["token"] = None
            _token_cache["expires_at"] = None
            token = await _get_token()
            headers["Authorization"] = f"Bearer {token}"
            resp = await client.request(method, url, headers=headers, json=body, params=params)
        if resp.status_code >= 400:
            # Surface the real Airbyte error body: the message/detail tells you WHY it failed.
            try:
                detail = resp.json()
            except Exception:
                detail = resp.text
            return {"error": resp.status_code, "detail": detail}
        if resp.status_code == 204:
            return {"status": "success"}
        return resp.json()


# --- Cost guard --------------------------------------------------------------
# Streams that have repeatedly produced multi-GB, multi-day, FAILING syncs and
# real money on a client invoice. Keyed by source type, matched on stream name.
#
# Klaviyo `events`: several GB and millions of rows per client even capped to one
# year, and syncs that run for days and then fail on Airbyte's heartbeat timeout.
# This is code and not a note because a catalog PATCH that merely PRESERVED a
# dormant `events` stream reset its cursor, re-read years of history across dozens
# of failing 40-hour syncs, and multiplied that client's monthly Airbyte bill.
# Use the aggregated `campaign_values_reports` + `flow_series_reports` instead:
# same reporting, a fraction of the volume, and they attribute revenue per campaign
# (events cannot).
DENYLISTED_STREAMS: dict[str, set[str]] = {
    "klaviyo": {"events", "events_detailed"},
}


async def _source_type_of_source(source_id: str) -> str:
    """Best-effort source type ("klaviyo", "shopify", ...). Empty string if unknown."""
    data = await _request("GET", f"/sources/{source_id}")
    return str(data.get("sourceType") or "").lower()


async def _source_type_of_connection(connection_id: str) -> str:
    """Best-effort source type behind a connection. Empty string if unknown."""
    conn = await _request("GET", f"/connections/{connection_id}")
    source_id = conn.get("sourceId")
    if not source_id:
        return ""
    return await _source_type_of_source(source_id)


# Every denylisted name, regardless of source. Used to fail CLOSED when the source
# type could not be read: an API blip must not turn the guard into a suggestion.
_ALL_DENYLISTED: set[str] = {n for names in DENYLISTED_STREAMS.values() for n in names}


def _strip_denylisted(stream_configs: list, source_type: str) -> tuple[list, list]:
    """Drop cost-bomb streams for this source type. Returns (kept, removed_names).

    An empty source_type means the lookup failed, so fall back to matching every
    known dangerous name rather than waving the request through."""
    blocked = DENYLISTED_STREAMS.get(source_type) or (_ALL_DENYLISTED if not source_type else set())
    if not blocked:
        return stream_configs, []
    kept, removed = [], []
    for s in stream_configs:
        name = s.get("name") if isinstance(s, dict) else None
        (removed.append(name) if name in blocked else kept.append(s))
    return kept, removed


def _denylist_note(removed: list, source_type: str) -> str:
    where = (
        f"on this {source_type} connection" if source_type
        else "(source type unknown because the lookup failed, so blocked on stream name)"
    )
    return (
        f"BLOCKED cost-bomb stream(s) {removed} {where}. "
        "These are multi-GB, they fail on Airbyte's heartbeat timeout, and they have "
        "billed clients real money. Everything else in the request went through. "
        "For Klaviyo reporting use campaign_values_reports + flow_series_reports instead. "
        "If a client genuinely needs event-level data, re-run with "
        "allow_expensive_streams=True and tell them what it will cost."
    )


def _parse_json_arg(value, name: str):
    """Accept a native dict/list (the MCP client marshals JSON arguments as real
    objects/arrays) OR a JSON string, and return the parsed structure. This avoids
    the double-encoding trap where a JSON-in-a-string param arrives at the Airbyte
    API as a string instead of a structured object. Raises ValueError on bad JSON."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in {name}: {e}")
    return value


MASK = "**********"


def _contains_mask(value) -> bool:
    """True if a masked secret placeholder appears anywhere in a (nested) config value."""
    if isinstance(value, str):
        return MASK in value
    if isinstance(value, dict):
        return any(_contains_mask(v) for v in value.values())
    if isinstance(value, list):
        return any(_contains_mask(v) for v in value)
    return False


async def _paginate(path: str, params: dict) -> list:
    """Collect every page of a public-API list endpoint (data/next envelope)."""
    out, offset = [], 0
    while True:
        page = await _request("GET", path, params={**params, "limit": "100", "offset": str(offset)})
        if "error" in page:
            raise RuntimeError(json.dumps(page))
        data = page.get("data", [])
        out.extend(data)
        if not page.get("next") or not data:
            return out
        offset += len(data)


def _sanitize(s: dict) -> dict:
    """Airbyte returns selectedFields/mappers as [] on GET (meaning "all fields"),
    but on PATCH an empty selectedFields is rejected as "no fields selected".
    Drop empty-list keys so preserved streams round-trip cleanly."""
    return {k: v for k, v in s.items() if not (isinstance(v, list) and len(v) == 0)}


def _catalog_diff(old: list, new: list) -> dict:
    """What a catalog PATCH changes. A changed sync mode, cursor or primary key makes
    Airbyte re-read that stream from scratch (and can wipe its destination table), so
    those are called out separately."""
    o = {s.get("name"): s for s in old if isinstance(s, dict)}
    n = {s.get("name"): s for s in new if isinstance(s, dict)}
    diff = {
        "added": sorted(set(n) - set(o)),
        "removed": sorted(set(o) - set(n)),
        "changed": sorted(
            k for k in set(o) & set(n)
            # Only compare fields the caller actually set, GET returns defaults we don't send.
            if any(
                n[k].get(f) is not None and n[k].get(f) != o[k].get(f)
                for f in ("syncMode", "cursorField", "primaryKey")
            )
        ),
    }
    unset = sorted(k for k in set(o) & set(n) if n[k].get("syncMode") is None)
    if unset:
        diff["unset_sync_mode"] = unset
        diff["unset_warning"] = (
            f"Existing stream(s) {unset} sent without syncMode. On a full stream-list replace "
            "Airbyte may fall back to its default mode and re-read them. Pass the current syncMode."
        )
    if diff["changed"]:
        diff["warning"] = (
            f"Sync mode / cursor / primary key changed on existing stream(s) {diff['changed']}. "
            "Airbyte treats that as a new stream: it re-reads it from the source start date and "
            "may overwrite the destination table. Check that this is intended and what it costs."
        )
    return diff


# --- MCP Server ---

mcp = FastMCP(
    "airbyte",
    instructions=(
        "Airbyte MCP server for managing data pipelines. "
        "Use these tools to create workspaces, sources (Shopify, Google Ads, Meta Ads, Klaviyo, etc.), "
        "destinations (BigQuery, etc.), connections with stream/schedule config, and trigger syncs. "
        "Diagnostics: fleet_status for health across workspaces, get_job_failure for why a job failed, "
        "check_source to test a source's credentials without syncing."
    ),
)


# === Workspaces ===


@mcp.tool()
async def list_workspaces() -> str:
    """List all Airbyte workspaces you have access to."""
    data = await _request("GET", "/workspaces")
    return json.dumps(data, indent=2)


@mcp.tool()
async def create_workspace(name: str) -> str:
    """Create a new Airbyte workspace.

    Args:
        name: Name for the new workspace (e.g. client name)
    """
    data = await _request("POST", "/workspaces", body={"name": name})
    return json.dumps(data, indent=2)


@mcp.tool()
async def get_workspace(workspace_id: str) -> str:
    """Get details of a specific workspace.

    Args:
        workspace_id: The workspace ID
    """
    data = await _request("GET", f"/workspaces/{workspace_id}")
    return json.dumps(data, indent=2)


@mcp.tool()
async def delete_workspace(workspace_id: str) -> str:
    """Delete a workspace.

    Args:
        workspace_id: The workspace ID to delete
    """
    data = await _request("DELETE", f"/workspaces/{workspace_id}")
    return json.dumps(data, indent=2)


# === Sources ===


@mcp.tool()
async def list_sources(workspace_id: str) -> str:
    """List all sources in a workspace.

    Args:
        workspace_id: The workspace ID to list sources for
    """
    data = await _request("GET", "/sources", params={"workspaceIds": workspace_id})
    return json.dumps(data, indent=2)


@mcp.tool()
async def create_source(workspace_id: str, name: str, source_type: str, configuration: dict | str) -> str:
    """Create a new source connector in a workspace.

    Args:
        workspace_id: The workspace ID
        name: Display name for this source (e.g. "Acme Shopify")
        source_type: The source type identifier (e.g. "shopify", "google-ads", "facebook-marketing", "klaviyo")
        configuration: Source-specific config as a JSON object (preferred) or JSON string. Examples:
            Shopify: {"shop": "mystore.myshopify.com", "credentials": {"api_password": "shpat_xxx"}}
            Google Ads: {"customer_id": "1234567890", "credentials": {"developer_token": "xxx", "client_id": "xxx", "client_secret": "xxx", "refresh_token": "xxx"}}
            Facebook Marketing: {"account_ids": ["act_123"], "credentials": {"access_token": "xxx"}}
            Klaviyo: {"api_key": "pk_xxx"}
    """
    try:
        config = _parse_json_arg(configuration, "configuration")
    except ValueError as e:
        return json.dumps({"error": str(e)})
    if not isinstance(config, dict):
        return json.dumps({"error": "configuration must be a JSON object"})
    config["sourceType"] = source_type
    body = {
        "name": name,
        "workspaceId": workspace_id,
        "configuration": config,
    }
    data = await _request("POST", "/sources", body=body)
    return json.dumps(data, indent=2)


@mcp.tool()
async def update_source(
    source_id: str,
    name: str | None = None,
    configuration: dict | str | None = None,
    verify: bool = True,
) -> str:
    """Update an existing source's name and/or configuration.

    The configuration is a PARTIAL update: pass ONLY the keys you want to change
    (e.g. {"start_date": "2025-01-01T00:00:00Z"}). Airbyte deep-merges them into the
    stored config, nested objects included, and keeps the secrets you leave out.
    Never send masked values ("**********") back: Airbyte stores them literally inside
    nested objects and the source breaks (verified 22-11-2026 on a GA4 service-account
    source: discover then fails with "OAuth access token refresh request failed").
    This tool refuses any configuration that contains the mask.

    Prefer the UI for OAuth sources (GA4, Google Ads, Facebook) if you need to touch
    their credentials; a re-auth only works there.

    Args:
        source_id: The source ID to update
        name: New display name (optional)
        configuration: JSON object (preferred) or JSON string with only the config keys
            to change (optional), e.g. {"conversion_window_days": 60}.
        verify: After a configuration change, run discover to prove the source still
            authenticates (default True). Takes up to a few minutes on big sources.
    """
    body: dict = {}
    if name:
        body["name"] = name
    if configuration is not None:
        try:
            partial = _parse_json_arg(configuration, "configuration")
        except ValueError as e:
            return json.dumps({"error": str(e)})
        if not isinstance(partial, dict):
            return json.dumps({"error": "configuration must be a JSON object of keys to change"})
        if _contains_mask(partial):
            return json.dumps({
                "error": f"configuration contains the masked placeholder {MASK!r}. Leave secrets "
                "out entirely (Airbyte keeps them) or send the real value. Sending the mask "
                "overwrites the stored secret and breaks the source."
            })
        body["configuration"] = partial
    if not body:
        return json.dumps({"error": "Nothing to update: provide name and/or configuration"})
    data = await _request("PATCH", f"/sources/{source_id}", body=body)
    if configuration is not None and verify and "error" not in data:
        # ignoreCache: otherwise Airbyte may serve the cached catalog and never authenticate.
        try:
            disc = await _request("GET", "/streams", params={"sourceId": source_id, "ignoreCache": "true"}, timeout=600)
        except (httpx.HTTPError, ValueError) as e:
            disc = {"error": type(e).__name__, "detail": str(e)[:300]}
        if isinstance(disc, dict) and "error" in disc:
            data = {
                "warning": "Update saved, but discover now FAILS. The source may be broken; "
                "check the credentials (UI 'Retest saved source').",
                "discover_error": disc,
                "source": data,
            }
        elif isinstance(disc, list) and disc:
            data = {"verified": f"discover ok, {len(disc)} streams", "source": data}
        else:
            data = {"warning": "Update saved, but discover returned no streams. Not verified.", "source": data}
    return json.dumps(data, indent=2)


@mcp.tool()
async def get_source(source_id: str) -> str:
    """Get details of a specific source.

    Args:
        source_id: The source ID
    """
    data = await _request("GET", f"/sources/{source_id}")
    return json.dumps(data, indent=2)


@mcp.tool()
async def delete_source(source_id: str) -> str:
    """Delete a source.

    Args:
        source_id: The source ID to delete
    """
    data = await _request("DELETE", f"/sources/{source_id}")
    return json.dumps(data, indent=2)


# === Destinations ===


@mcp.tool()
async def list_destinations(workspace_id: str) -> str:
    """List all destinations in a workspace.

    Args:
        workspace_id: The workspace ID
    """
    data = await _request("GET", "/destinations", params={"workspaceIds": workspace_id})
    return json.dumps(data, indent=2)


@mcp.tool()
async def create_destination(workspace_id: str, name: str, destination_type: str, configuration: dict | str) -> str:
    """Create a new destination connector in a workspace.

    Args:
        workspace_id: The workspace ID
        name: Display name (e.g. "Acme BigQuery")
        destination_type: The destination type (e.g. "bigquery", "snowflake", "postgres")
        configuration: Destination-specific config as a JSON object (preferred) or JSON string. Example for BigQuery:
            {"project_id": "my-gcp-project", "dataset_id": "raw", "credentials_json": "{...service account json...}", "dataset_location": "EU"}
    """
    try:
        config = _parse_json_arg(configuration, "configuration")
    except ValueError as e:
        return json.dumps({"error": str(e)})
    if not isinstance(config, dict):
        return json.dumps({"error": "configuration must be a JSON object"})
    config["destinationType"] = destination_type
    body = {
        "name": name,
        "workspaceId": workspace_id,
        "configuration": config,
    }
    data = await _request("POST", "/destinations", body=body)
    return json.dumps(data, indent=2)


@mcp.tool()
async def get_destination(destination_id: str) -> str:
    """Get details of a specific destination.

    Args:
        destination_id: The destination ID
    """
    data = await _request("GET", f"/destinations/{destination_id}")
    return json.dumps(data, indent=2)


@mcp.tool()
async def delete_destination(destination_id: str) -> str:
    """Delete a destination.

    Args:
        destination_id: The destination ID to delete
    """
    data = await _request("DELETE", f"/destinations/{destination_id}")
    return json.dumps(data, indent=2)


# === Streams (discovery) ===


@mcp.tool()
async def discover_streams(source_id: str) -> str:
    """Discover available streams (tables) for a source. Use this to see what data you can sync before creating a connection.

    Args:
        source_id: The source ID to discover streams for
    """
    data = await _request("GET", "/streams", params={"sourceId": source_id})
    return json.dumps(data, indent=2)


# === Connections ===


@mcp.tool()
async def list_connections(workspace_id: str) -> str:
    """List all connections in a workspace.

    Args:
        workspace_id: The workspace ID
    """
    data = await _request("GET", "/connections", params={"workspaceIds": workspace_id})
    return json.dumps(data, indent=2)


@mcp.tool()
async def create_connection(
    source_id: str,
    destination_id: str,
    name: str,
    streams: list | str,
    schedule_type: str = "cron",
    cron_expression: str = "0 0 6 * * ?",
    allow_expensive_streams: bool = False,
    namespace_format: str | None = None,
) -> str:
    """Create a connection (sync pipeline) between a source and destination.

    Args:
        source_id: The source ID
        destination_id: The destination ID
        name: Display name (e.g. "Acme Shopify → BigQuery")
        streams: JSON array (preferred) or JSON string of stream configurations. Example:
            [
                {"name": "orders", "syncMode": "incremental_append"},
                {"name": "products", "syncMode": "full_refresh_overwrite"},
                {"name": "customers", "syncMode": "incremental_append"}
            ]
            Valid sync modes: full_refresh_overwrite, full_refresh_append, incremental_append, incremental_deduped_history
        schedule_type: "cron" (default), or "manual"
        cron_expression: Quartz cron expression in UTC (default: "0 0 6 * * ?" = daily at 06:00 UTC). Only used when schedule_type is "cron".
        allow_expensive_streams: Escape hatch for DENYLISTED_STREAMS (e.g. Klaviyo `events`). Default False,
            which silently drops them and reports what was dropped. Only set True on an explicit, costed
            client decision. These streams have produced multi-day failing syncs and 10x invoices.
        namespace_format: Optional target dataset/schema for this connection (Airbyte custom_format
            namespace), e.g. "raw_amazon_fr". Lets several connections share one destination (and its
            credentials) while each writes to its own dataset. Default: the destination's own dataset.
    """
    try:
        stream_configs = _parse_json_arg(streams, "streams")
    except ValueError as e:
        return json.dumps({"error": str(e)})

    note = None
    if not allow_expensive_streams:
        source_type = await _source_type_of_source(source_id)
        stream_configs, removed = _strip_denylisted(stream_configs, source_type)
        if removed:
            note = _denylist_note(removed, source_type)
        if not stream_configs:
            return json.dumps({"error": "No streams left after the cost guard", "guard": note}, indent=2)

    body = {
        "sourceId": source_id,
        "destinationId": destination_id,
        "name": name,
        "configurations": {"streams": stream_configs},
    }
    if schedule_type == "cron":
        body["schedule"] = {
            "scheduleType": "cron",
            "cronExpression": cron_expression,
        }
    else:
        body["schedule"] = {"scheduleType": "manual"}
    if namespace_format:
        body["namespaceDefinition"] = "custom_format"
        body["namespaceFormat"] = namespace_format

    data = await _request("POST", "/connections", body=body)
    if note:
        data = {"guard": note, "connection": data}
    return json.dumps(data, indent=2)


@mcp.tool()
async def update_connection(
    connection_id: str,
    name: str | None = None,
    streams: list | str | None = None,
    schedule_type: str | None = None,
    cron_expression: str | None = None,
    status: str | None = None,
    allow_expensive_streams: bool = False,
) -> str:
    """Update an existing connection.

    NOTE: `streams` REPLACES the connection's full stream list (it is not a merge).
    To flip on / change a single stream while preserving the rest, use enable_stream.

    Args:
        connection_id: The connection ID to update
        name: New display name (optional)
        streams: New full stream configuration as a JSON array (preferred) or JSON string (optional)
        schedule_type: New schedule type: "cron" or "manual" (optional)
        cron_expression: New cron expression (optional, only with schedule_type "cron")
        status: Set connection status: "active" or "inactive" (optional)
        allow_expensive_streams: Escape hatch for DENYLISTED_STREAMS (e.g. Klaviyo `events`). Default False,
            which drops them from the new stream list and reports it. Only set True on an explicit,
            costed client decision.
    """
    body = {}
    note = None
    if name:
        body["name"] = name
    if streams is not None:
        try:
            stream_configs = _parse_json_arg(streams, "streams")
        except ValueError as e:
            return json.dumps({"error": str(e)})
        if not isinstance(stream_configs, list):
            return json.dumps({"error": "streams must be a JSON array of stream configs"})
        if not allow_expensive_streams:
            source_type = await _source_type_of_connection(connection_id)
            stream_configs, removed = _strip_denylisted(stream_configs, source_type)
            if removed:
                note = _denylist_note(removed, source_type)
            if not stream_configs:
                return json.dumps({"error": "No streams left after the cost guard", "guard": note}, indent=2)
        body["configurations"] = {"streams": stream_configs}
        current = await _request("GET", f"/connections/{connection_id}")
        diff = _catalog_diff((current.get("configurations") or {}).get("streams") or [], stream_configs) if "error" not in current else None
    if schedule_type:
        if schedule_type == "cron":
            body["schedule"] = {
                "scheduleType": "cron",
                "cronExpression": cron_expression or "0 0 6 * * ?",
            }
        else:
            body["schedule"] = {"scheduleType": "manual"}
    if status:
        body["status"] = status

    data = await _request("PATCH", f"/connections/{connection_id}", body=body)
    if streams is not None and diff is not None and "error" not in data:
        data = {"catalog_diff": diff, "connection": data}
    if note:
        data = {"guard": note, **(data if "catalog_diff" in data else {"connection": data})}
    return json.dumps(data, indent=2)


@mcp.tool()
async def enable_stream(
    connection_id: str,
    stream_name: str,
    sync_mode: str,
    cursor_field: list | None = None,
    primary_key: list | None = None,
    allow_expensive_streams: bool = False,
) -> str:
    """Enable (add) or update a SINGLE stream on an existing connection, preserving every
    other stream. Reads the connection's current streams, upserts the named one with the
    given sync mode, and PATCHes. Use this to flip on a newly-discovered stream (run
    discover_streams first to confirm the source exposes it) without hand-assembling the
    full stream list.

    Args:
        connection_id: The connection ID
        stream_name: The stream (table) name to enable, e.g. "campaign_performance_by_conv_date"
        sync_mode: One of full_refresh_overwrite, full_refresh_append, incremental_append, incremental_deduped_history
        cursor_field: Optional cursor field path(s) for incremental modes, e.g. ["segments.date"]
        primary_key: Optional primary-key field paths for deduped modes, e.g. [["segments.date"], ["campaign.id"]]
        allow_expensive_streams: Escape hatch for DENYLISTED_STREAMS (e.g. Klaviyo `events`). Default False.
            Note this tool also strips denylisted streams it finds on the EXISTING connection: carrying one
            through a catalog PATCH is what reset a dormant Klaviyo cursor and cost a client 10x their
            monthly bill. Only set True on an explicit, costed client decision.
    """
    current = await _request("GET", f"/connections/{connection_id}")
    if "error" in current:
        return json.dumps({"error": "Could not read current connection", "detail": current})
    streams = (current.get("configurations") or {}).get("streams") or []

    source_type = ""
    if not allow_expensive_streams:
        source_type = await _source_type_of_source(current.get("sourceId", ""))
        if stream_name in (DENYLISTED_STREAMS.get(source_type) or (_ALL_DENYLISTED if not source_type else set())):
            return json.dumps({"error": _denylist_note([stream_name], source_type)}, indent=2)

    # Keep an existing stream's cursor / primary key when the caller omits them:
    # dropping a custom cursor makes Airbyte fall back to the default and re-read
    # the whole stream.
    existing = next((s for s in streams if s.get("name") == stream_name), {})
    # Start from the existing stream so selectedFields / mappers (column subsets,
    # PII hashing) survive a sync-mode change.
    entry: dict = {**_sanitize(existing), "name": stream_name, "syncMode": sync_mode}
    cf = cursor_field if cursor_field is not None else (existing.get("cursorField") or None)
    pk = primary_key if primary_key is not None else (existing.get("primaryKey") or None)
    if cf is not None:
        entry["cursorField"] = cf
    if pk is not None:
        entry["primaryKey"] = pk

    new_streams = []
    replaced = False
    for s in streams:
        if s.get("name") == stream_name:
            new_streams.append(entry)
            replaced = True
        else:
            new_streams.append(_sanitize(s))
    if not replaced:
        new_streams.append(entry)

    note = None
    if not allow_expensive_streams:
        new_streams, removed = _strip_denylisted(new_streams, source_type)
        if removed:
            note = _denylist_note(removed, source_type)

    body = {"configurations": {"streams": new_streams}}
    data = await _request("PATCH", f"/connections/{connection_id}", body=body)
    if "error" not in data:
        data = {"catalog_diff": _catalog_diff(streams, new_streams), "connection": data}
    if note:
        data = {"guard": note, **data}
    return json.dumps(data, indent=2)


@mcp.tool()
async def get_connection(connection_id: str) -> str:
    """Get details of a specific connection.

    Args:
        connection_id: The connection ID
    """
    data = await _request("GET", f"/connections/{connection_id}")
    return json.dumps(data, indent=2)


@mcp.tool()
async def delete_connection(connection_id: str) -> str:
    """Delete a connection.

    Args:
        connection_id: The connection ID to delete
    """
    data = await _request("DELETE", f"/connections/{connection_id}")
    return json.dumps(data, indent=2)


# === Jobs (syncs) ===


@mcp.tool()
async def trigger_sync(connection_id: str) -> str:
    """Trigger an immediate sync for a connection.

    Args:
        connection_id: The connection ID to sync
    """
    data = await _request("POST", "/jobs", body={"connectionId": connection_id, "jobType": "sync"})
    return json.dumps(data, indent=2)


@mcp.tool()
async def trigger_reset(connection_id: str) -> str:
    """Trigger a reset (full re-sync) for a connection. This clears all data and re-syncs from scratch.

    Args:
        connection_id: The connection ID to reset
    """
    data = await _request("POST", "/jobs", body={"connectionId": connection_id, "jobType": "reset"})
    return json.dumps(data, indent=2)


@mcp.tool()
async def get_job(job_id: str) -> str:
    """Get the status and details of a specific sync job.

    Args:
        job_id: The job ID
    """
    data = await _request("GET", f"/jobs/{job_id}")
    return json.dumps(data, indent=2)


@mcp.tool()
async def list_jobs(connection_id: str, job_type: str = "sync", limit: int = 10) -> str:
    """List recent jobs for a connection.

    Args:
        connection_id: The connection ID
        job_type: Filter by job type: "sync" or "reset" (default: "sync")
        limit: Max number of jobs to return (default: 10)
    """
    data = await _request(
        "GET",
        "/jobs",
        params={"connectionId": connection_id, "jobType": job_type, "limit": str(limit)},
    )
    return json.dumps(data, indent=2)


# === Diagnostics ===


def _first(*values):
    return next((v for v in values if v is not None), None)


@mcp.tool()
async def get_job_failure(job_id: str) -> str:
    """Why did a sync job fail? Returns per attempt the failure origin, type and the real
    error message (e.g. expired refresh token, secret not found, heartbeat timeout), plus
    rows/bytes per attempt. list_jobs/get_job only say "failed" with 0 rows.

    Uses Airbyte's internal config API (same token as the public API).

    Args:
        job_id: The job ID (from list_jobs)
    """
    try:
        jid = int(job_id)
    except ValueError:
        return json.dumps({"error": "job_id must be numeric"})
    data = await _request("POST", "/jobs/get_debug_info", body={"id": jid}, base=AIRBYTE_CONFIG_API_URL)
    if "error" in data:
        return json.dumps(data, indent=2)
    job = data.get("job") or {}
    attempts = []
    for a in data.get("attempts") or []:
        att = a.get("attempt") or {}
        stats = att.get("totalStats") or {}
        attempts.append({
            "attempt": att.get("id"),
            "status": att.get("status"),
            "started": att.get("createdAt"),
            "ended": att.get("endedAt"),
            "rows": _first(stats.get("recordsCommitted"), stats.get("recordsEmitted")),
            "bytes": _first(stats.get("bytesCommitted"), stats.get("bytesEmitted")),
            "failures": [
                {
                    "origin": f.get("failureOrigin"),
                    "type": f.get("failureType"),
                    "stream": (f.get("streamDescriptor") or {}).get("name"),
                    "message": f.get("externalMessage"),
                    "internal": (f.get("internalMessage") or "")[:1000] or None,
                }
                for f in (att.get("failureSummary") or {}).get("failures", [])
            ],
        })
    return json.dumps({
        "job_id": job.get("id"),
        "status": job.get("status"),
        "connection_id": job.get("configId"),
        "source": (job.get("sourceDefinition") or {}).get("name"),
        "source_version": (job.get("sourceDefinition") or {}).get("dockerImageTag"),
        "attempts": attempts,
    }, indent=2)


@mcp.tool()
async def check_source(source_id: str) -> str:
    """Run the connector's own connection check on a saved source (same as "Retest saved
    source" in the UI). Writes no rows and changes nothing. Use it to see whether a token
    or credential still works, with the real error message.

    Caveat: some connectors pass the check without touching the API (a GA4 source with a
    fake service-account key passed, 22-11-2026). If the check passes but syncs fail, run
    discover_streams too: discover does authenticate.

    Args:
        source_id: The source ID
    """
    data = await _request(
        "POST", "/sources/check_connection", body={"sourceId": source_id},
        base=AIRBYTE_CONFIG_API_URL, timeout=300,
    )
    if "error" in data:
        return json.dumps(data, indent=2)
    info = data.get("jobInfo") or {}
    events = (info.get("logs") or {}).get("events", [])
    problems = [(e.get("message") or "")[:500] for e in events if (e.get("level") or "").lower() in ("error", "warn", "fatal")]
    return json.dumps({
        "status": data.get("status"),
        "message": data.get("message"),
        "job_succeeded": info.get("succeeded"),
        "log_errors_and_warnings": problems[-20:],
    }, indent=2)


def _duration_s(iso: str | None) -> int | None:
    """Parse an ISO-8601 duration like PT1H3M27S into seconds."""
    if not iso or not iso.startswith("P"):
        return None
    date_part, _, time_part = iso[1:].partition("T")
    total = 0.0
    for part, units in ((date_part, {"W": 604800, "D": 86400}), (time_part, {"H": 3600, "M": 60, "S": 1})):
        num = ""
        for ch in part:
            if ch.isdigit() or ch == ".":
                num += ch
            else:
                total += float(num or 0) * units.get(ch, 0)
                num = ""
    return int(total)


@mcp.tool()
async def fleet_status(workspace_id: str | None = None, jobs_per_connection: int = 5) -> str:
    """Health of every connection across all workspaces (or one): status, schedule, the
    last job, the last successful sync, failures since then and recent durations. Use it
    after an Airbyte outage (Airbyte auto-disables a connection after a run of failures)
    and before re-activating a connection someone turned off on purpose.

    Args:
        workspace_id: Limit to one workspace (optional, default all)
        jobs_per_connection: How many recent sync jobs to read per connection (default 5)
    """
    try:
        workspaces = await _paginate("/workspaces", {})
        if workspace_id:
            workspaces = [w for w in workspaces if w.get("workspaceId") == workspace_id]
        names = {w["workspaceId"]: w.get("name") for w in workspaces}
        if not names:
            return json.dumps({"error": "No workspaces found"})
        conns = await _paginate("/connections", {"workspaceIds": ",".join(names)})
    except RuntimeError as e:
        return json.dumps({"error": "Could not list workspaces/connections", "detail": str(e)})

    sem = asyncio.Semaphore(8)

    async def one(c: dict) -> dict:
        async with sem:
            try:
                jobs = await _request("GET", "/jobs", params={
                    "connectionId": c["connectionId"], "jobType": "sync",
                    "limit": str(jobs_per_connection), "orderBy": "createdAt|DESC",
                })
            except Exception as e:  # one bad connection must not sink the whole report
                jobs = {"error": type(e).__name__, "detail": str(e)[:300]}
        rows = jobs.get("data", []) if "error" not in jobs else []
        last = rows[0] if rows else {}
        last_ok = next((j for j in rows if j.get("status") == "succeeded"), None)
        # Failures since the last success. A running/pending/cancelled job on top
        # must not hide the failed ones under it (the typical post-outage retry).
        fails = 0
        for j in rows:
            if j.get("status") == "succeeded":
                break
            if j.get("status") in ("failed", "incomplete"):
                fails += 1
        sched = c.get("schedule") or {}
        return {
            "workspace": names.get(c.get("workspaceId")),
            "connection": c.get("name"),
            "connection_id": c["connectionId"],
            "status": c.get("status"),
            "schedule": sched.get("cronExpression") or sched.get("scheduleType"),
            "last_job": {k: last.get(k) for k in ("jobId", "status", "startTime", "duration", "rowsSynced")} if last else None,
            "last_success": last_ok.get("startTime") if last_ok else f"none in last {len(rows)} jobs",
            "failures_since_last_success": fails,
            "recent_durations_s": [_duration_s(j.get("duration")) for j in rows],
            "jobs_error": jobs if "error" in jobs else None,
        }

    results = await asyncio.gather(*(one(c) for c in conns))
    # Problems first: active-but-failing, then active-with-no-jobs, then inactive, then healthy.
    def rank(r):
        failing = r["failures_since_last_success"] > 0
        return (0 if failing and r["status"] == "active" else 1 if failing else 2 if r["status"] == "active" and not r["last_job"] else 3 if r["status"] != "active" else 4, r["workspace"] or "", r["connection"] or "")
    results.sort(key=rank)
    summary = {
        "connections": len(results),
        "active": sum(r["status"] == "active" for r in results),
        "inactive": sum(r["status"] != "active" for r in results),
        "with_failures_since_last_success": sum(r["failures_since_last_success"] > 0 for r in results),
    }
    return json.dumps({"summary": summary, "connections": results}, indent=2)


# === Health ===


@mcp.tool()
async def health_check() -> str:
    """Check if the Airbyte API is reachable and healthy."""
    token = await _get_token()
    url = AIRBYTE_API_URL.rstrip("/").replace("/v1", "") + "/health"
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(url, headers={"Authorization": f"Bearer {token}"})
        return json.dumps({"status": resp.status_code, "body": resp.text}, indent=2)


# === Run ===

if __name__ == "__main__":
    mcp.run()
