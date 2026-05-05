from __future__ import annotations

import unittest

import pandas as pd

from src.data.clean_solicitudes import clean_solicitudes
from src.paths import CONFIG_DIR
from src.utils.io_utils import load_yaml


def _load_rules() -> tuple[dict, dict]:
    return load_yaml(CONFIG_DIR / "column_aliases.yaml"), load_yaml(CONFIG_DIR / "regex_rules.yaml")


class CleanSolicitudesSchemaTests(unittest.TestCase):
    def test_new_schema_uses_fecha_servicio_and_action(self) -> None:
        aliases, regex_rules = _load_rules()
        raw = pd.DataFrame(
            {
                "Solicitud": [
                    "PORTE ENVIO MATERIAL",
                    "PORTE ENTREGA EVENTO",
                    "PORTE ENTREGA EVENTO",
                    "PORTE ENVIO PRODUCTO",
                ],
                "Fecha de servicio": [
                    "01/04/2026 13:00:00",
                    "02/04/2026 09:00:00",
                    "03/04/2026 09:00:00",
                    "04/04/2026 10:00:00",
                ],
                "Creacion solicitud": [
                    "25/03/2026 10:00:00",
                    "25/03/2026 10:00:00",
                    "25/03/2026 10:00:00",
                    "25/03/2026 10:00:00",
                ],
                "Pedido": ["SG06202625794-01", "EG22202625747-02", "DEGE202523889-01", "SGP202624836-01"],
                "Articulo": ["100", "101", "102", "103"],
                "Cant. solicitada": [1, 1, 1, 1],
                "Codigo Generico": ["SG06202625794", "EG22202625747", "DEGE202523889", "SGP202624836"],
                "Accion": ["ENVIO", "RECOGIDA", "RECOGIDA", "ENVIO"],
            }
        )

        cleaned, qa = clean_solicitudes(raw, aliases, regex_rules)

        self.assertEqual(cleaned["tipo_servicio"].tolist(), ["SGE", "EGE", "EGE", "SGP"])
        self.assertEqual(cleaned["clase_servicio"].tolist(), ["entrega", "recogida", "recogida", "entrega"])
        self.assertEqual(cleaned["campo_fecha_objetiva_usado"].unique().tolist(), ["fecha_servicio"])
        self.assertEqual(cleaned["flag_fecha_objetiva_fallback"].sum(), 0)
        self.assertEqual(cleaned.loc[0, "fecha_servicio_objetiva"], pd.Timestamp("2026-04-01"))
        self.assertEqual(qa["service_date_logic"]["pct_missing"], 0.0)

    def test_old_schema_keeps_pickup_fallback_to_fin_evento(self) -> None:
        aliases, regex_rules = _load_rules()
        raw = pd.DataFrame(
            {
                "Solicitud": ["PORTE RECOGIDA MATERIAL"],
                "Inicio Evento": ["01/04/2026 09:00:00"],
                "Fin evento": ["03/04/2026 20:00:00"],
                "Creacion solicitud": ["25/03/2026 10:00:00"],
                "Pedido": ["EGE202600001-01"],
                "Articulo": ["100"],
                "Cant. solicitada": [1],
                "Codigo Generico": ["EGE202600001"],
            }
        )

        cleaned, _ = clean_solicitudes(raw, aliases, regex_rules)

        self.assertEqual(cleaned.loc[0, "tipo_servicio"], "EGE")
        self.assertEqual(cleaned.loc[0, "clase_servicio"], "recogida")
        self.assertEqual(cleaned.loc[0, "campo_fecha_objetiva_usado"], "fecha_fin_evento")
        self.assertEqual(cleaned.loc[0, "fecha_servicio_objetiva"], pd.Timestamp("2026-04-03"))


if __name__ == "__main__":
    unittest.main()
