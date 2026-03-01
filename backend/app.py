"""Compat wrapper for the API application.

This file keeps the historical import path ``backend.app:app`` used by
uvicorn and scripts, while the real FastAPI app implementation lives in
``backend.api_app`` with a more descriptive module name.
"""

from .api_app import app  # re-export FastAPI instance for uvicorn

