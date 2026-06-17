from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from src.pipelines.common.normalize import _enrich_external_order_id, normalize_movimientos


class MovimientosLookupEnrichmentTest(unittest.TestCase):
    def test_enriches_external_order_for_pi_movements(self) -> None:
        raw = pd.DataFrame(
            {
                "tipo_movimiento": ["PI"],
                "fecha_inicio": ["03/01/2022 08:31:38"],
                "fecha_finalizacion": ["03/01/2022 08:49:05"],
                "articulo": ["022043"],
                "denominacion_articulo": ["Articulo test"],
                "cantidad": [1],
                "propietario": ["MAHOU"],
                "ubicacion": ["A01"],
                "paleta": ["400000010002700981"],
                "pedido_externo": [pd.NA],
            }
        )
        lookup = pd.DataFrame(
            {
                "pallet": ["400000010002700981"],
                "sku": ["22043"],
                "completed_at": [pd.Timestamp("2022-01-03 08:49:05")],
                "quantity": [1],
                "owner": ["MAHOU"],
                "order_id": ["12345"],
                "external_order_id": ["SGE202205972"],
                "client": ["C001"],
                "external_client": ["EC001"],
            }
        )

        with patch("src.pipelines.common.normalize._load_external_order_lookup", return_value=None):
            output = normalize_movimientos(raw, aliases={})
        self.assertTrue(output.loc[0, "external_order_id"] is pd.NA or pd.isna(output.loc[0, "external_order_id"]))

        enriched = _enrich_external_order_id(output, lookup)
        self.assertEqual(enriched.loc[0, "external_order_id"], "SGE202205972")
        self.assertEqual(enriched.loc[0, "external_order_id_enriched"], 1)
        self.assertEqual(enriched.loc[0, "external_order_id_source"], "lookup")


if __name__ == "__main__":
    unittest.main()
