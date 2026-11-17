"""Fonte REST do ZelaAí e gerador determinístico de páginas de demonstração."""

import json
import math
import os
import random
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener
from uuid import UUID

CATEGORIES = (
    "calcada", "iluminacao", "semaforo", "ponto_onibus",
    "acessibilidade", "buraco", "lixo", "outros",
)
STATUSES = ("aguardando_aprovacao", "aprovada", "rejeitada", "protocolada", "resolvida")
URGENCIES = ("baixa", "media", "alta")

DEMO_SIZE = 240
PAGE_SIZE = 100
MAX_PAGES = 1000
API_URL = "https://zelai-api.azurewebsites.net"


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def request_json(base, path, *, body=None, token=None):
    headers = {"Accept": "application/json"}
    if body is not None:
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = Request(
        base + path, headers=headers,
        data=json.dumps(body).encode() if body is not None else None,
    )
    try:
        with build_opener(NoRedirect()).open(request, timeout=30) as response:
            return json.load(response)
    except HTTPError as exc:
        # A resposta de erro pode conter dados da requisição.
        raise RuntimeError(f"ZelaAí respondeu HTTP {exc.code} em {path.split('?')[0]}") from None


def api_fetcher():
    base = os.getenv("ZELAI_API_URL", API_URL).rstrip("/")
    url = urlsplit(base)
    local = url.scheme == "http" and url.hostname in {"localhost", "127.0.0.1", "::1"}
    if (url.scheme != "https" and not local) or not url.hostname or any(
        (url.username, url.password, url.path, url.query, url.fragment)
    ):
        raise ValueError("ZELAI_API_URL deve ser uma origem HTTPS ou HTTP em loopback")

    token = os.getenv("ZELAI_TOKEN")
    if not token:
        email, password = os.getenv("ZELAI_EMAIL"), os.getenv("ZELAI_SENHA")
        if not email or not password:
            raise ValueError("Configure ZELAI_EMAIL e ZELAI_SENHA ou ZELAI_TOKEN")
        token = request_json(base, "/auth/login", body={"email": email, "senha": password})["token"]
        if not isinstance(token, str) or not token:
            raise ValueError("Token ausente na resposta de autenticação")

    def fetch(page):
        query = urlencode({"page": page, "limit": PAGE_SIZE})
        return request_json(base, f"/ocorrencias?{query}", token=token)

    return fetch


def demo_fetcher():
    rng = random.Random(42)
    start = datetime(2026, 10, 1, tzinfo=timezone.utc)
    neighborhoods = ("Bela Vista", "Ipiranga", "Lapa", "Mooca", "Pinheiros", "Santana")
    items = []
    for index in range(DEMO_SIZE):
        created = start + timedelta(hours=index * 4)
        items.append({
            "id": str(UUID(int=index + 1, version=4)),
            "categoria": rng.choice(CATEGORIES),
            "status": rng.choices(STATUSES, (4, 2, 1, 2, 3))[0],
            "urgencia": rng.choices(URGENCIES, (4, 4, 2))[0],
            "endereco": {"bairro": rng.choice(neighborhoods), "cidade": "São Paulo"},
            "apoios": rng.randrange(20),
            "criadaEm": created.isoformat(),
        })
    items.reverse()

    def fetch(page):
        offset = (page - 1) * PAGE_SIZE
        return {"items": items[offset:offset + PAGE_SIZE], "page": page,
                "limit": PAGE_SIZE, "total": len(items),
                "totalPages": math.ceil(len(items) / PAGE_SIZE)}

    return fetch


def pages(fetch):
    expected_total = None
    for number in range(1, MAX_PAGES + 1):
        payload = fetch(number)
        items, total = payload["items"], payload["total"]
        if expected_total is None:
            expected_total = total
        if type(total) is not int or total < 0 or total != expected_total:
            raise ValueError("Total inválido ou alterado durante a paginação. Reexecute o lote.")
        total_pages = math.ceil(total / PAGE_SIZE)
        expected_size = max(0, min(PAGE_SIZE, total - (number - 1) * PAGE_SIZE))
        if total_pages > MAX_PAGES or not isinstance(items, list) or (
            payload["page"], payload["limit"], payload["totalPages"], len(items)
        ) != (number, PAGE_SIZE, total_pages, expected_size):
            raise ValueError(f"Contrato de paginação inconsistente na página {number}")
        yield items
        if number >= total_pages:
            return
