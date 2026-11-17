import json
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from zelai_data import enrichment, pipeline, postgres, source


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = self.enterContext(tempfile.TemporaryDirectory())
        self.panel = Path(self.temp) / "painel.html"
        self.enterContext(patch.dict("os.environ", {
            "ZELAI_SOURCE": "demo", "ZELAI_DATA_DIR": self.temp,
            "ZELAI_PANEL_PATH": str(self.panel), "ZELAI_POSTGRES_DSN": "",
        }))

    def test_repeatable_snapshot_and_panel(self):
        for _ in range(2):
            for step in (pipeline.extract, pipeline.transform, pipeline.load,
                         pipeline.validate, pipeline.publish):
                step("demo")
        with closing(pipeline.connect_batch("demo")) as db:
            self.assertEqual(db.execute(
                "SELECT paginas, recebidos, publicados, rejeitados FROM controle_lote"
            ).fetchone(), (3, 240, 240, 0))
            self.assertEqual(db.execute(
                "SELECT COUNT(*), SUM(ocorrencias) FROM prioridade_por_bairro"
            ).fetchone(), (6, 240))
        self.assertIn('"mode": "demo"', self.panel.read_text())
        raw = (pipeline.batch("demo") / "ingestao.json").read_text()
        self.assertNotIn('"id"', raw)
        self.assertNotIn('"endereco"', raw)

    def test_changing_offset_total_fails(self):
        fetch = source.demo_fetcher()

        def changing(page):
            payload = fetch(page)
            if page == 2:
                payload["total"] += 1
            return payload

        with self.assertRaisesRegex(ValueError, "alterado"):
            list(source.pages(changing))

    def test_invalid_record_blocks_publication(self):
        pipeline.extract("bad")
        path = pipeline.batch("bad") / "ingestao.json"
        payload = json.loads(path.read_text())
        payload["rows"][0]["status"] = "desconhecido"
        path.write_text(json.dumps(payload))
        pipeline.transform("bad")
        pipeline.load("bad")
        with self.assertRaisesRegex(ValueError, "registros rejeitados"):
            pipeline.validate("bad")
        self.assertFalse(self.panel.exists())

    def test_rank_missing_and_zero_district_inputs_are_distinct(self):
        self.assertGreater(enrichment.score(20, 40, None, None),
                           enrichment.score(20, 40, 0, 0))
        self.assertEqual(enrichment.score(80, 350, 26000, 1100), 100)

    def test_demo_cannot_write_to_postgres(self):
        with patch.dict("os.environ", {"ZELAI_POSTGRES_DSN": "postgresql://localhost/demo"}):
            with self.assertRaisesRegex(ValueError, "exige ZELAI_SOURCE=api"):
                postgres.publish_priority([], "demo")


if __name__ == "__main__":
    unittest.main()
