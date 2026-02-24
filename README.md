# Airbyte MCP Server

An MCP (Model Context Protocol) server that wraps the [Airbyte API](https://reference.airbyte.com), giving AI agents like Claude direct access to manage data pipelines.

## Tools (23)

| Category | Tools |
|---|---|
| Workspaces | `list_workspaces`, `create_workspace`, `get_workspace`, `delete_workspace` |
| Sources | `list_sources`, `create_source`, `get_source`, `delete_source` |
| Destinations | `list_destinations`, `create_destination`, `get_destination`, `delete_destination` |
| Streams | `discover_streams` |
| Connections | `list_connections`, `create_connection`, `update_connection`, `get_connection`, `delete_connection` |
| Jobs | `trigger_sync`, `trigger_reset`, `get_job`, `list_jobs` |
| Health | `health_check` |

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

The 23 Airbyte tools will be available in your next session.

## Usage examples

Once configured, you can ask Claude things like:

- "List all my Airbyte workspaces"
- "Create a new workspace for BYLOTTE"
- "Add a Shopify source with this API key"
- "Create a BigQuery destination"
- "Connect Shopify to BigQuery with incremental sync on orders, products, and customers"
- "Trigger a sync and check the status"

## Supported sources

Any source available in Airbyte works — common ecommerce ones include:

- **Shopify** — orders, products, customers, inventory
- **Google Ads** — campaigns, ad groups, performance reports
- **Meta/Facebook Ads** — campaigns, adsets, ads, insights
- **Klaviyo** — campaigns, flows, lists, profiles, events
- **Stripe / Mollie** — payments, refunds, customers
- **Google Analytics 4** — via BigQuery Export (recommended) or Airbyte connector

## License

MIT
