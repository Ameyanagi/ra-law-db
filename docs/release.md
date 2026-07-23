# Release Plan

`ra-law-db` is published to PyPI as a public runtime library and bundled
Japanese chemical law screening dataset. Service wrappers, scrapers, and
deployment-specific projects stay GitHub-only.

## Repository Cleanup Policy

- Do not commit generated distributions from `dist/`.
- Do not commit virtual environments, test caches, lint caches, or coverage files.
- Keep only source files and intended runtime data under `src/ra_law_db/data/`.
- Keep the README install command aligned with the current PyPI package name.
- Run the local checks before tagging a release.

## Local Checks

```bash
uv sync --group dev
uv run pre-commit run --all-files
uv run pytest -q
uv run python -m build --sdist --wheel
uv run twine check dist/*
```

Install hooks once per checkout:

```bash
uv run pre-commit install
```

## PyPI Trusted Publishing Setup

Configure a Trusted Publisher on PyPI before pushing the first release tag:

- PyPI project: `ra-law-db`
- GitHub owner: `Ameyanagi`
- GitHub repository: `ra-law-db`
- Workflow name: `publish.yml`
- Environment name: `pypi`

The workflow uses GitHub OIDC through `pypa/gh-action-pypi-publish`, so no PyPI
API token is needed in GitHub secrets.

## Tag-Only Release Flow

1. Update `project.version` in `pyproject.toml`.
2. Refresh `src/ra_law_db/data/regulatory.sqlite3` when releasing a data update.
3. Run the local checks.
4. Commit the release changes and push `main`.
5. Create and push a matching tag:

```bash
git tag v0.2.1
git push origin v0.2.1
```

The publish workflow only runs on tags matching `v*`. It also verifies that the
tag exactly matches `project.version`, runs tests, builds fresh distributions,
checks them with Twine, and publishes to PyPI.
