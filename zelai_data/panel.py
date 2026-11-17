"""Painel HTML autocontido com agregados de bairro, sem identificadores de ocorrências."""

import json
from pathlib import Path


def render(mode, timestamp, rows, ranking):
    payload = json.dumps({
        "mode": mode, "timestamp": timestamp, "rows": rows, "ranking": ranking,
    }, ensure_ascii=False).replace("<", "\\u003c")
    template = Path(__file__).with_name("panel.html").read_text(encoding="utf-8")
    return template.replace("__ZELAI_PAYLOAD__", payload)
