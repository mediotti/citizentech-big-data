"""Publica o snapshot gold no contrato PostgreSQL do backend ZelaAí."""

import logging
import os
from datetime import datetime

LOG = logging.getLogger(__name__)


def publish_priority(rows, mode):
    dsn = os.getenv("ZELAI_POSTGRES_DSN")
    if not dsn:
        LOG.info("PostgreSQL opcional: destino não configurado")
        return
    if mode != "api":
        raise ValueError("A publicação PostgreSQL exige ZELAI_SOURCE=api")

    import psycopg
    from psycopg.conninfo import conninfo_to_dict

    settings = conninfo_to_dict(dsn)
    host = settings.get("host", "")
    local = host in {"127.0.0.1", "localhost", "::1"} and not settings.get("hostaddr")
    if not local and (not host or settings.get("sslmode") != "verify-full"):
        raise ValueError("Destino remoto exige TLS com sslmode=verify-full e hostname")
    if not rows:
        raise ValueError("Snapshot de prioridade vazio. A publicação existente foi preservada.")

    with psycopg.connect(dsn, connect_timeout=15) as conn, conn.cursor() as cur:
        cur.execute("SET LOCAL lock_timeout = '10s'")
        cur.execute("SET LOCAL statement_timeout = '90s'")
        cur.execute("LOCK TABLE prioridade_por_bairro IN SHARE ROW EXCLUSIVE MODE")
        # Um lote mais antigo não deve substituir um lote mais recente.
        cur.execute("SELECT MAX(atualizado_em) FROM prioridade_por_bairro")
        latest = cur.fetchone()[0]
        if latest is not None and latest >= datetime.fromisoformat(rows[0][-1]):
            LOG.info("Snapshot PostgreSQL já publicado ou superado")
            return
        cur.execute("DELETE FROM prioridade_por_bairro")
        cur.executemany("""
            INSERT INTO prioridade_por_bairro
                (bairro, populacao_idosa, ocorrencias, apoios, score_prioridade, atualizado_em)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, rows)
    LOG.info("PostgreSQL: prioridade_por_bairro atualizada com %d bairros", len(rows))
