"""ETL por lote, com publicação atômica do painel após controles de qualidade."""

import hashlib
import json
import logging
import os
import sqlite3
import tempfile
import unicodedata
from collections import Counter
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from . import enrichment, postgres, source
from .panel import render

LOG = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE fato_ocorrencia (
    ocorrencia_chave TEXT PRIMARY KEY,
    criada_utc TEXT NOT NULL,
    dia_local TEXT NOT NULL,
    cidade TEXT NOT NULL,
    bairro TEXT NOT NULL,
    categoria TEXT NOT NULL,
    status TEXT NOT NULL,
    urgencia TEXT NOT NULL,
    apoios INTEGER NOT NULL CHECK (apoios >= 0)
);
CREATE TABLE mart_ocorrencias (
    dia_local TEXT NOT NULL,
    cidade TEXT NOT NULL,
    bairro TEXT NOT NULL,
    categoria TEXT NOT NULL,
    status TEXT NOT NULL,
    urgencia TEXT NOT NULL,
    total INTEGER NOT NULL CHECK (total > 0),
    apoios INTEGER NOT NULL CHECK (apoios >= 0),
    PRIMARY KEY (dia_local, cidade, bairro, categoria, status, urgencia)
) WITHOUT ROWID;
CREATE TABLE prioridade_por_bairro (
    bairro TEXT PRIMARY KEY,
    populacao_idosa INTEGER,
    ocorrencias INTEGER NOT NULL,
    apoios INTEGER NOT NULL,
    score_prioridade REAL NOT NULL CHECK (score_prioridade BETWEEN 0 AND 100),
    atualizado_em TEXT NOT NULL
);
CREATE TABLE controle_lote (
    lote TEXT PRIMARY KEY,
    origem TEXT NOT NULL CHECK (origem IN ('demo', 'api')),
    coletado_utc TEXT NOT NULL,
    paginas INTEGER NOT NULL,
    recebidos INTEGER NOT NULL,
    publicados INTEGER NOT NULL,
    rejeitados INTEGER NOT NULL
);
"""


def batch(run_id):
    root = Path(os.getenv("ZELAI_DATA_DIR", "data")).resolve()
    return root / "lotes" / hashlib.sha256(run_id.encode()).hexdigest()[:24]


def atomic_text(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False
    ) as temporary:
        tmp = Path(temporary.name)
        try:
            temporary.write(content)
            temporary.close()
            tmp.replace(path)
        finally:
            tmp.unlink(missing_ok=True)


def extract(run_id):
    mode = os.getenv("ZELAI_SOURCE", "demo")
    fetchers = {"demo": source.demo_fetcher, "api": source.api_fetcher}
    if mode not in fetchers:
        raise ValueError("ZELAI_SOURCE deve ser demo ou api")
    rows, page_count = [], 0
    for page_count, items in enumerate(source.pages(fetchers[mode]()), 1):
        for item in items:
            if not isinstance(item, dict):
                rows.append({})
                continue
            address = item.get("endereco")
            address = address if isinstance(address, dict) else {}
            # Minimização antes da persistência. O identificador permanece apenas como hash.
            identifier = item.get("id")
            rows.append({
                "chave": hashlib.sha256(identifier.strip().lower().encode()).hexdigest()
                if isinstance(identifier, str) and identifier.strip() else None,
                "cidade": address.get("cidade"),
                "bairro": address.get("bairro"),
                **{field: item.get(field) for field in
                   ("categoria", "status", "urgencia", "apoios", "criadaEm")},
            })
    payload = {"origem": mode, "paginas": page_count,
               "coletado_utc": datetime.now(timezone.utc).isoformat(timespec="microseconds"), "rows": rows}
    atomic_text(batch(run_id) / "ingestao.json", json.dumps(payload, ensure_ascii=False))
    LOG.info("Ingestão concluída: origem=%s páginas=%d recebidos=%d", mode, page_count, len(rows))


def locality(value):
    if not isinstance(value, str):
        raise ValueError("localidade")
    normalized = " ".join(unicodedata.normalize("NFKC", value).casefold().split())
    if not normalized or len(normalized) > 100:
        raise ValueError("localidade")
    return normalized


def enum_value(value, allowed, field):
    if value not in allowed:
        raise ValueError(field)
    return value


def normalize(row):
    from zoneinfo import ZoneInfo

    try:
        created = datetime.fromisoformat(row["criadaEm"])
        if created.tzinfo is None:
            raise ValueError
    except (TypeError, KeyError, ValueError):
        raise ValueError("criadaEm") from None
    if type(row["apoios"]) is not int or row["apoios"] < 0:
        raise ValueError("apoios")
    return (
        row["chave"],
        created.astimezone(timezone.utc).isoformat(timespec="milliseconds"),
        created.astimezone(ZoneInfo("America/Sao_Paulo")).date().isoformat(),
        locality(row["cidade"] if row["cidade"] is not None else "São Paulo"),
        locality(row["bairro"]),
        enum_value(row["categoria"], source.CATEGORIES, "categoria"),
        enum_value(row["status"], source.STATUSES, "status"),
        enum_value(row["urgencia"], source.URGENCIES, "urgencia"),
        row["apoios"],
    )


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def transform(run_id):
    folder = batch(run_id)
    payload = read_json(folder / "ingestao.json")
    raw = payload.pop("rows")
    valid, rejected, seen = [], Counter(), set()
    for row in raw:
        key = row.get("chave")
        if not key:
            rejected["id"] += 1
            continue
        if key in seen:
            # A API pagina por offset. Duplicata pode indicar deslocamento do lote.
            raise ValueError("ID repetido no snapshot. Reexecute a coleta.")
        seen.add(key)
        try:
            valid.append(normalize(row))
        except ValueError as exc:
            rejected[str(exc)] += 1
    payload.update({"recebidos": len(raw), "rejeitados": dict(rejected), "rows": valid})
    atomic_text(folder / "tratamento.json", json.dumps(payload, ensure_ascii=False))
    LOG.info("Tratamento concluído: válidos=%d rejeitados=%d motivos=%s",
             len(valid), sum(rejected.values()), dict(rejected))


def load(run_id):
    folder = batch(run_id)
    payload = read_json(folder / "tratamento.json")
    output = folder / "zelai.sqlite"
    temporary = folder / "carga.sqlite"
    temporary.unlink(missing_ok=True)
    db = sqlite3.connect(temporary)
    try:
        with db:
            db.executescript(SCHEMA)
            db.executemany("INSERT INTO fato_ocorrencia VALUES (?,?,?,?,?,?,?,?,?)", payload["rows"])
            db.execute("""
                INSERT INTO mart_ocorrencias
                SELECT dia_local, cidade, bairro, categoria, status, urgencia, COUNT(*), SUM(apoios)
                FROM fato_ocorrencia GROUP BY dia_local, cidade, bairro, categoria, status, urgencia
            """)
            ranking = enrichment.build_priority(db, payload["origem"], payload["coletado_utc"])
            db.executemany("INSERT INTO prioridade_por_bairro VALUES (?,?,?,?,?,?)", ranking)
            db.execute("INSERT INTO controle_lote VALUES (?,?,?,?,?,?,?)", (
                run_id, payload["origem"], payload["coletado_utc"], payload["paginas"],
                payload["recebidos"], len(payload["rows"]), sum(payload["rejeitados"].values()),
            ))
    finally:
        db.close()
    temporary.replace(output)
    LOG.info("Carga SQLite concluída: fato_ocorrencia=%d", len(payload["rows"]))


def connect_batch(run_id):
    return sqlite3.connect((batch(run_id) / "zelai.sqlite").as_uri() + "?mode=ro", uri=True)


def validate(run_id):
    with closing(connect_batch(run_id)) as db:
        count, supports = db.execute(
            "SELECT COUNT(*), COALESCE(SUM(apoios), 0) FROM fato_ocorrencia"
        ).fetchone()
        mart = db.execute(
            "SELECT COALESCE(SUM(total), 0), COALESCE(SUM(apoios), 0) FROM mart_ocorrencias"
        ).fetchone()
        received, published, rejected = db.execute(
            "SELECT recebidos, publicados, rejeitados FROM controle_lote"
        ).fetchone()
        if mart != (count, supports) or received != count + rejected or published != count:
            raise ValueError("Falha de reconciliação do lote")
        ranking = db.execute(
            "SELECT COALESCE(SUM(ocorrencias), 0), COALESCE(SUM(apoios), 0) FROM prioridade_por_bairro"
        ).fetchone()
        city = db.execute("""
            SELECT COUNT(*), COALESCE(SUM(apoios), 0) FROM fato_ocorrencia
            WHERE cidade IN ('são paulo', 'sao paulo')
        """).fetchone()
        if ranking != city:
            raise ValueError("Falha de reconciliação da prioridade por bairro")
        if rejected:
            raise ValueError(f"Publicação bloqueada: {rejected} registros rejeitados")
        if db.execute("PRAGMA quick_check").fetchone() != ("ok",):
            raise ValueError("Falha de integridade SQLite")
    LOG.info("Qualidade aprovada: recebidos=%d publicados=%d apoios=%d", received, count, supports)


def publish(run_id):
    with closing(connect_batch(run_id)) as db:
        mode, timestamp = db.execute("SELECT origem, coletado_utc FROM controle_lote").fetchone()
        rows = db.execute("""
            SELECT cidade, bairro, categoria, status, urgencia, SUM(total), SUM(apoios)
            FROM mart_ocorrencias GROUP BY cidade, bairro, categoria, status, urgencia
            ORDER BY cidade, bairro, categoria, status, urgencia
        """).fetchall()
        ranking = db.execute("""
            SELECT bairro, populacao_idosa, ocorrencias, apoios, score_prioridade, atualizado_em
            FROM prioridade_por_bairro ORDER BY score_prioridade DESC, bairro
        """).fetchall()
    output = Path(os.getenv("ZELAI_PANEL_PATH", "data/public/index.html"))
    rendered = render(mode, timestamp, rows, ranking)
    postgres.publish_priority(ranking, mode)
    atomic_text(output, rendered)
    LOG.info("Painel publicado: origem=%s grupos=%d", mode, len(rows))
