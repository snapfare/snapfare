#!/usr/bin/env python
"""Recalcula la columna `scoring` de deals usando la lógica del pipeline.

Uso (desde la raíz del repo):

    python -m backend.scripts.recompute_scoring_deals_all
"""

from __future__ import annotations

from dotenv import load_dotenv, find_dotenv

import sys
from pathlib import Path
from typing import Any, Dict, List


THIS_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = THIS_DIR.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

# Cargar .env
load_dotenv(find_dotenv(), override=True)

from database.supabase_db import _client  # type: ignore
from services.deals_pipeline import _load_amadeus_benchmarks, score_deals  # type: ignore


def main() -> None:
    if not _client:
        print("[recompute_scoring] Supabase client no configurado")
        return

    try:
        rsp = (
            _client.table("deals")
            .select("id,title,price,currency,origin_iata,destination_iata,date_out")
            .execute()
        )
    except Exception as e:
        print("[recompute_scoring] Error leyendo deals_all:", repr(e))
        return

    rows: List[Dict[str, Any]] = getattr(rsp, "data", []) or []
    if not rows:
        print("[recompute_scoring] deals vacío; nada que hacer")
        return

    print(f"[recompute_scoring] Recalculando scoring para {len(rows)} filas de deals…")

    deals_for_scoring: List[Dict[str, Any]] = []
    id_list: List[int] = []

    for r in rows:
        try:
            deal_id = int(r["id"])
        except Exception:
            continue
        id_list.append(deal_id)
        deals_for_scoring.append(
            {
                "id": deal_id,
                "title": r.get("title"),
                "price": r.get("price"),
                "currency": r.get("currency"),
                "origin_iata": r.get("origin_iata"),
                "destination_iata": r.get("destination_iata"),
                # score_deals mira `departure_date` o `date_out` para extraer el mes
                "date_out": r.get("date_out"),
                "departure_date": r.get("date_out"),
            }
        )

    benchmarks = _load_amadeus_benchmarks()
    scored = score_deals(deals_for_scoring, benchmarks)

    updates: List[Dict[str, Any]] = []
    for d in scored:
        deal_id = d.get("id")
        if deal_id is None:
            continue
        updates.append({"id": deal_id, "scoring": d.get("score")})

    if not updates:
        print("[recompute_scoring] No se generaron updates de scoring")
        return

    try:
        rsp_upd = _client.table("deals").upsert(updates, on_conflict="id").execute()
    except Exception as e:
        print("[recompute_scoring] Error actualizando deals:", repr(e))
        return

    updated_count = len(getattr(rsp_upd, "data", []) or [])
    print(f"[recompute_scoring] Updates aplicados en deals: {updated_count}")


if __name__ == "__main__":  # pragma: no cover
    main()
