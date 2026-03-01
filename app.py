"""Convenience entrypoint for snapcore.

This file exists for aesthetics and ease of use:

- `python app.py` behaves like running the unified launcher.
- The actual implementation lives in `backend/scripts/run.py`.

Keeping the launcher inside `backend/` avoids mixing web-server code and
CLI/job orchestration in the same module.
"""

from __future__ import annotations

from backend.scripts.run import main


if __name__ == "__main__":
    main()
