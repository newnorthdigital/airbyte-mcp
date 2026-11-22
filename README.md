# Airbyte MCP Server

An MCP (Model Context Protocol) server that wraps the [Airbyte API](https://reference.airbyte.com), giving AI agents like Claude direct access to manage data pipelines.

## Tools (28)

| Category | Tools |
|---|---|
| Workspaces | `list_workspaces`, `create_workspace`, `get_workspace`, `delete_workspace` |
| Sources | `list_sources`, `create_source`, `update_source`, `get_source`, `delete_source` |
| Destinations | `list_destinations`, `create_destination`, `get_destination`, `delete_destination` |
| Streams | `discover_streams` |
| Connections | `list_connections`, `create_connection`, `update_connection`, `enable_stream`, `get_connection`, `delete_connection` |
| Jobs | `trigger_sync`, `trigger_reset`, `get_job`, `list_jobs` |
| Diagnostics | `fleet_status`, `get_job_failure`, `check_source` |
| Health | `health_check` |

## Guards

- **Cost guard.** `create_connection`, `update_connection` and `enable_stream` drop the Klaviyo `events` and `events_detailed` streams (`DENYLISTED_STREAMS` in `server.py`), also when they are already on the connection. These streams have caused multi-day failing syncs and a 10x monthly bill. `allow_expensive_streams=True` overrides it; use that only on an explicit, costed client decision. If the source type can't be read, the guard blocks on name (fail closed).
- **Catalog diff.** `update_connection` (with `streams`) and `enable_stream` return `catalog_diff`: added, removed and changed streams. A changed sync mode, cursor or primary key re-reads that stream from the start date, so it comes with a warning.
- **`update_source` sends only the keys you pass.** Airbyte deep-merges them server-side and keeps secrets you leave out. The tool refuses any value containing the mask `**********`: sending the mask back overwrites a nested secret (reproduced 22-11-2026 on a GA4 service-account source). After a config change it runs discover to prove the source still authenticates (`verify=False` skips it).

## Diagnostics

`get_job_failure` and `check_source` call Airbyte's internal config API (`AIRBYTE_CONFIG_API_URL`, default `https://cloud.airbyte.com/api/v1`) with the same application token. The public API only reports `failed, rowsSynced: 0` and returns 403 on `/sources/{id}/check` for application tokens. These endpoints are not part of Airbyte's public contract, so they can change without notice.

- `fleet_status`: every connection across workspaces with status, last job, last success and failures since the last success, problems first. Use it after an outage (Airbyte auto-disables connections after a run of failures) and before re-activating one.
- `get_job_failure`: the real failure message per attempt.
- `check_source`: the connector's own check, no rows written. Some connectors pass without calling the API (GA4 with a fake key passed), so run `discover_streams` too when syncs keep failing.

## Setup

### 1. Get Airbyte credentials

In the Airbyte UI, go to **User Settings > Applications > Create an application** to get a `client_id` and `client_secret`.

### 2. Set environment variables

```bash
export AIRBYTE_CLIENT_ID="your_client_id"
export AIRBYTE_CLIENT_SECRET="your_client_secret"

# Optional: for self-hosted Airbyte (defaults to Airbyte Cloud)
export AIRBYTE_API_URL="http://localhost:8000/api/public/v1"
```

### 3. Install dependencies

```bash
cd airbyte-mcp
uv venv && uv pip install "mcp[cli]>=1.0.0" "airbyte-api>=0.53.0" "httpx>=0.27.0"
```

### 4. Add to Claude Code

```bash
claude mcp add airbyte --transport stdio -s user -- /path/to/airbyte-mcp/.venv/bin/python /path/to/airbyte-mcp/server.py
```

### 5. Restart Claude Code

The 28 Airbyte tools will be available in your next session.

## Usage examples

Once configured, you can ask Claude things like:

- "List all my Airbyte workspaces"
- "Create a new workspace for Acme"
- "Add a Shopify source with this API key"
- "Create a BigQuery destination"
- "Connect Shopify to BigQuery with incremental sync on orders, products, and customers"
- "Trigger a sync and check the status"

## Supported sources

Any source available in Airbyte works. Common ecommerce ones include:

- **Shopify**: orders, products, customers, inventory
- **Google Ads**: campaigns, ad groups, performance reports
- **Meta/Facebook Ads**: campaigns, adsets, ads, insights
- **Klaviyo**: campaigns, flows, lists, profiles, events
- **Stripe / Mollie**: payments, refunds, customers
- **Google Analytics 4**: via BigQuery Export (recommended) or Airbyte connector

## License

MIT
