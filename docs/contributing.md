# Contributing

Thank you for considering a contribution to SyncDB! Please read the full guide in [CONTRIBUTING.md](../CONTRIBUTING.md) at the project root.

## Quick reference

### Set up a development environment

```bash
git clone https://github.com/qubdi/syncdb.git
cd syncdb
pip install -e ".[dev]"
pre-commit install   # quality gates on commit (ruff, mypy, message format) and push (tests)
```

Commit subjects follow [Conventional Commits](https://www.conventionalcommits.org/)
(`fix(sync): ...`, `feat(connectors): ...`) — see the
[full guide](../CONTRIBUTING.md) for the type list and examples.

### Run unit tests

```bash
pytest
```

### Run integration tests (requires Docker)

```bash
cd Tests/DataBase
docker compose up -d --build
cd ../..
pytest Tests/Library/DatabaseToDatabase
```

### Code style

```bash
ruff check .
ruff format .
mypy Library/
```

### Adding a new database engine

Five files need to be updated:

1. Create `Library/connectors/<engine>.py` implementing `BaseConnector`
2. Register the connector in `Library/connectors/__init__.py`
3. Add it to the `create_connector` factory in `Library/connections.py`
4. Add engine aliases in `Library/config.py`
5. Add type mappings in `Library/type_mapping.py`

### Building the docs locally

```bash
pip install -r docs/requirements.txt
sphinx-build -b html docs docs/_build/html
open docs/_build/html/index.html
```
