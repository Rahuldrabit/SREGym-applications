# Agentic Retry Platform

An enterprise-grade autonomous agent platform designed for SREGym to model compounded multi-layer retry amplification and metastable overload.

## Architecture

The system features:
- **Agent Orchestrator** (2 replicas): High-level planner & supervisor that schedules tasks, manages persistent workflow state in Redis, enforces deadlines, and replans ($R_p = 3$) on tool failure.
- **Tool Gateway** (2 replicas): Manages tool execution, enforces deadlines ($600\text{ms}$), executes tool retries ($R_t = 2$), HTTP transport retries ($R_h = 2$), and circuit breaking.
- **Data API** (1 replica by default): Knowledge base query service exposing administrative fault injection (`POST /admin/fault`), queueing, and metrics.
- **PgBouncer & PostgreSQL**: Models physical database connection pool saturation ($C = 25$).
- **Redis**: Durable workflow metadata and the authoritative per-workflow retry budget.

## Metastable Overload Dynamics

$$\text{Amplification } A(t) = R_{\text{planner}} \times R_{\text{tool}} \times R_{\text{transport}} = 3 \times 2 \times 2 = 12\times$$

When transient latency hits the database ($100\text{ms} \rightarrow 1500\text{ms}$ for $10\text{s}$), downstream timeouts trigger cascading retries across all three layers. Arrival rate spikes from $10\text{ req/s}$ to $>100\text{ attempts/s}$, saturating PgBouncer's 25-connection pool and filling Redis backlogs. Once the transient fault is removed, the accumulated backlog and uncoordinated retries sustain the overload indefinitely ($A(t) \gg 1$, $p95 > 5\text{s}$, $Q > 50$).
