"""
Airbyte MCP Server

Exposes the Airbyte API as MCP tools for managing workspaces, sources,
destinations, connections, and sync jobs.

Authentication: Set AIRBYTE_CLIENT_ID and AIRBYTE_CLIENT_SECRET env vars.
Optionally set AIRBYTE_API_URL for self-hosted (defaults to Airbyte Cloud).
"""

import json
import os
from datetime import datetime, timedelta, timezone

import httpx
from mcp.server.fastmcp import FastMCP

# --- Configuration ---

AIRBYTE_API_URL = os.environ.get("AIRBYTE_API_URL", "https://api.airbyte.com/v1")
AIRBYTE_CLIENT_ID = os.environ.get("AIRBYTE_CLIENT_ID", "")
AIRBYTE_CLIENT_SECRET = os.environ.get("AIRBYTE_CLIENT_SECRET", "")

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


async def _request(method: str, path: str, body: dict | None = None, params: dict | None = None) -> dict:
    """Make an authenticated request to the Airbyte API."""
    token = await _get_token()
    url = AIRBYTE_API_URL.rstrip("/") + path
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.request(method, url, headers=headers, json=body, params=params)
        if resp.status_code >= 400:
            return {"error": resp.status_code, "detail": resp.text}
        if resp.status_code == 204:
            return {"status": "success"}
        return resp.json()


# --- MCP Server ---

mcp = FastMCP(
    "airbyte",
    instructions=(
        "Airbyte MCP server for managing data pipelines. "
        "Use these tools to create workspaces, sources (Shopify, Google Ads, Meta Ads, Klaviyo, etc.), "
        "destinations (BigQuery, etc.), connections with stream/schedule config, and trigger syncs."
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
async def create_source(workspace_id: str, name: str, source_type: str, configuration: str) -> str:
    """Create a new source connector in a workspace.

    Args:
        workspace_id: The workspace ID
        name: Display name for this source (e.g. "BYLOTTE Shopify")
        source_type: The source type identifier (e.g. "shopify", "google-ads", "facebook-marketing", "klaviyo")
        configuration: JSON string with source-specific config. Examples:
            Shopify: {"shop": "mystore.myshopify.com", "credentials": {"api_password": "shpat_xxx"}}
            Google Ads: {"customer_id": "1234567890", "credentials": {"developer_token": "xxx", "client_id": "xxx", "client_secret": "xxx", "refresh_token": "xxx"}}
            Facebook Marketing: {"account_ids": ["act_123"], "credentials": {"access_token": "xxx"}}
            Klaviyo: {"api_key": "pk_xxx"}
    """
    config = json.loads(configuration)
    config["sourceType"] = source_type
    body = {
        "name": name,
        "workspaceId": workspace_id,
        "configuration": config,
    }
    data = await _request("POST", "/sources", body=body)
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
async def create_destination(workspace_id: str, name: str, destination_type: str, configuration: str) -> str:
    """Create a new destination connector in a workspace.

    Args:
        workspace_id: The workspace ID
        name: Display name (e.g. "BYLOTTE BigQuery")
        destination_type: The destination type (e.g. "bigquery", "snowflake", "postgres")
        configuration: JSON string with destination-specific config. Example for BigQuery:
            {"project_id": "my-gcp-project", "dataset_id": "raw", "credentials_json": "{...service account json...}", "dataset_location": "EU"}
    """
    config = json.loads(configuration)
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
    streams: str,
    schedule_type: str = "cron",
    cron_expression: str = "0 0 6 * * ?",
) -> str:
    """Create a connection (sync pipeline) between a source and destination.

    Args:
        source_id: The source ID
        destination_id: The destination ID
        name: Display name (e.g. "BYLOTTE Shopify → BigQuery")
        streams: JSON string with stream configurations. Example:
            [
                {"name": "orders", "syncMode": "incremental_append"},
                {"name": "products", "syncMode": "full_refresh_overwrite"},
                {"name": "customers", "syncMode": "incremental_append"}
            ]
            Valid sync modes: full_refresh_overwrite, full_refresh_append, incremental_append, incremental_deduped_history
        schedule_type: "cron" (default), or "manual"
        cron_expression: Quartz cron expression in UTC (default: "0 0 6 * * ?" = daily at 06:00 UTC). Only used when schedule_type is "cron".
    """
    stream_configs = json.loads(streams)
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

    data = await _request("POST", "/connections", body=body)
    return json.dumps(data, indent=2)


@mcp.tool()
async def update_connection(
    connection_id: str,
    name: str | None = None,
    streams: str | None = None,
    schedule_type: str | None = None,
    cron_expression: str | None = None,
    status: str | None = None,
) -> str:
    """Update an existing connection.

    Args:
        connection_id: The connection ID to update
        name: New display name (optional)
        streams: New stream configuration as JSON string (optional)
        schedule_type: New schedule type: "cron" or "manual" (optional)
        cron_expression: New cron expression (optional, only with schedule_type "cron")
        status: Set connection status: "active" or "inactive" (optional)
    """
    body = {}
    if name:
        body["name"] = name
    if streams:
        body["configurations"] = {"streams": json.loads(streams)}
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
