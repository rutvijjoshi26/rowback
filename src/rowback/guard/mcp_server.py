"""MCP server exposing the guard gateway to AI agents.

Agents get two tools: run_sql and request_status. Approving held statements is
deliberately not exposed; a human runs `rowback guard approve <id>`.
Start with: rowback guard mcp --agent <name>   (stdio transport)
"""
from __future__ import annotations

from typing import Any

from mcp.server.mcpserver import MCPServer

from rowback.guard.gateway import Gateway


def build_server(gateway: Gateway, agent: str) -> MCPServer:
    server = MCPServer("rowback-guard")

    @server.tool()
    def run_sql(sql: str) -> dict[str, Any]:
        """Run SQL against the production database under rowback's safety policy.

        DROP, TRUNCATE, ALTER and privilege changes are blocked. DELETE or UPDATE
        without WHERE, and anything that would change more rows than the configured
        limit, is held for human approval and returns status "pending" with a
        request_id. Poll request_status to learn the outcome.
        """
        return gateway.execute(sql, agent=agent).as_dict()

    @server.tool()
    def request_status(request_id: int) -> dict[str, Any]:
        """Return the status of a held statement: pending, executed, rejected or failed."""
        req = gateway.request(request_id)
        return {k: (str(v) if k in ("created_at", "decided_at") and v else v)
                for k, v in req.items()}

    return server
