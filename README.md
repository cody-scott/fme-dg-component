# dagster-fme

A Dagster Component that represents FME Flow workspaces as Dagster assets and materializes them
from successful FME Flow jobs via a sensor.

Full documentation lands in Phase 6 of [`PLAN.md`](PLAN.md).

## Development

```bash
uv sync
uv run pytest
uv run ruff check . && uv run ruff format --check .
uv run pyright
```
