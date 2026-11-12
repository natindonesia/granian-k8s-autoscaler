# Contributing

- `uv venv && uv pip install -e ".[dev]"`, then `pytest` and `ruff check . && ruff format --check .`.
- Changes under `core/` need a test that fails without them. Prefer a property or a scenario in
  `tests/test_sim_scenarios.py`; if you add a safety mechanism, add the negative control that shows
  the failure it prevents.
- Keep `core/` free of I/O and clocks.
- Do not add dependencies without a strong reason; the tool runs as a tiny sidecar-like Deployment.
