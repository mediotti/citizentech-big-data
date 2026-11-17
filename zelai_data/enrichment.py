"""Enriquecimento distrital com entradas normalizadas e ranking interpretável."""

import csv
import logging
import os
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from unicodedata import combining, normalize

LOG = logging.getLogger(__name__)
DEMO_DISTRICTS = (
    ("Bela Vista", "demo-01", 12300, 510),
    ("Ipiranga", "demo-02", 16700, 680),
    ("Lapa", "demo-03", 14100, 790),
    ("Mooca", "demo-04", 18400, 380),
    ("Pinheiros", "demo-05", 10900, 630),
    ("Santana", "demo-06", 21100, 880),
)


def join_key(value):
    decomposed = normalize("NFKD", value.casefold())
    return " ".join("".join(c for c in decomposed if not combining(c)).split())


def read_lookup(path, key, value, convert):
    result = {}
    with path.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        if set(reader.fieldnames or []) != {key, value}:
            raise ValueError(f"Cabeçalho inválido: {path.name}")
        for row in reader:
            identifier = row[key].strip()
            identifier = join_key(identifier) if key == "bairro" else identifier
            entry = convert(row[value].strip())
            if not identifier or identifier in result or entry == "":
                raise ValueError(f"Chave inválida ou duplicada: {path.name}")
            if isinstance(entry, int) and not 0 <= entry <= 2_147_483_647:
                raise ValueError(f"Contagem fora da faixa: {path.name}")
            result[identifier] = entry
    if not result:
        raise ValueError(f"Dimensão vazia: {path.name}")
    return result


def load_context(mode):
    if mode == "demo":
        return (
            {join_key(b): d for b, d, _, _ in DEMO_DISTRICTS},
            {d: p for _, d, p, _ in DEMO_DISTRICTS},
            {d: s for _, d, _, s in DEMO_DISTRICTS},
        )
    directory = os.getenv("ZELAI_ENRICHMENT_DIR")
    if not directory:
        return {}, {}, {}
    folder = Path(directory)
    return (
        read_lookup(folder / "geosampa.csv", "bairro", "distrito_id", str),
        read_lookup(folder / "ibge.csv", "distrito_id", "populacao_idosa", int),
        read_lookup(folder / "sp156.csv", "distrito_id", "solicitacoes", int),
    )


def score(incidents, supports, age, requests):
    inputs = [(incidents, 50, 45), (supports, 300, 25)]
    if age is not None:
        inputs.append((age, 25000, 20))
    if requests is not None:
        inputs.append((requests, 1000, 10))
    points = sum(min(Decimal(value) / ceiling, Decimal(1)) * weight
                 for value, ceiling, weight in inputs)
    return (100 * points / sum(weight for _, _, weight in inputs)).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )


def build_priority(db, mode, collected):
    geography, population, sp156 = load_context(mode)
    neighborhoods = db.execute("""
        SELECT bairro, COUNT(*), SUM(apoios) FROM fato_ocorrencia
        WHERE cidade IN ('são paulo', 'sao paulo') GROUP BY bairro ORDER BY bairro
    """)
    result = []
    joined = 0
    for neighborhood, incidents, supports in neighborhoods:
        district = geography.get(join_key(neighborhood))
        age, requests = population.get(district), sp156.get(district)
        joined += age is not None and requests is not None
        result.append((neighborhood, age, incidents, supports,
                       str(score(incidents, supports, age, requests)), collected))
    LOG.info("Ranking: bairros=%d com contexto distrital completo=%d", len(result), joined)
    return result
