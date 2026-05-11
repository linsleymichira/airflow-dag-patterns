"""Repo-root conftest. Puts repo root and include/ on sys.path so DAGs that do
`from include.callbacks import ...` resolve under pytest outside the Astro container.

Astro Runtime adds `/usr/local/airflow` (which contains include/) to PYTHONPATH automatically;
a bare GitHub runner doesn't, so CI fails without this shim.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
for path in (REPO_ROOT, REPO_ROOT / "include"):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)
