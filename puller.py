#!/usr/bin/env python3
"""
Ponte Millennium -> canal-ml (roda no Railway como CRON JOB, stateless).

Por que existe: a HostGator (onde roda o canal-ml) bloqueia saida na porta 6017
do Millennium. O Railway alcanca a 6017 (a API do Millennium nao filtra por IP,
so por usuario/senha). Entao este script roda no Railway, puxa o estoque do
Millennium e empurra pro canal-ml pelos endpoints que ja existem.

Fluxo (1 execucao, depois encerra):
  1) GET {MILLENNIUM_URL}/produtos/saldodeestoque?vitrine=..&trans_id=..&$format=json
     (Basic auth). Pagina avancando o trans_id ate esgotar. Full pull (stateless):
     comeca em trans_id=0 toda vez -> nao precisa guardar cursor.
  2) POST em lotes para {CANAL_BASE}/api/erp-estoque.php com { token, itens:[{codigo,estoque}] }.

Config: 100% por variaveis de ambiente (nada de segredo no codigo). Ver README.
Single-flight: como e um cron curto que encerra, nao ha 2 execucoes simultaneas
batendo na licenca do Millennium.
"""

import base64
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

# ------------------------- Config (env) -------------------------

def env(name: str, default: str = "") -> str:
    return (os.environ.get(name, default) or "").strip()

MILLENNIUM_URL  = env("MILLENNIUM_URL")      # ex: http://rotaextrema.millenniumhosting.com.br:6017/api/millenium_eco
MILLENNIUM_USER = env("MILLENNIUM_USER")
MILLENNIUM_PASS = env("MILLENNIUM_PASS")
VITRINE         = env("VITRINE", "0")
CANAL_BASE      = env("CANAL_BASE").rstrip("/")   # ex: https://sistema.wolfattack.com.br/projeto/canal-ml
ERP_PUSH_TOKEN  = env("ERP_PUSH_TOKEN")

# Campo de saldo do Millennium a usar como estoque "real".
# 'saldo' foi o usado no piloto PowerShell; o conector PHP usa
# 'saldo_vitrine_sem_reserva'. Deixa configuravel pra decidir sem mexer no codigo.
SALDO_FIELD     = env("SALDO_FIELD", "saldo")

BATCH_SIZE      = int(env("BATCH_SIZE", "300"))
MAX_PAGES       = int(env("MAX_PAGES", "50"))
HTTP_TIMEOUT    = int(env("HTTP_TIMEOUT", "60"))
DRY_RUN         = env("DRY_RUN", "0") not in ("", "0", "false", "False")

# Destino do estoque:
#   "gist" (padrao) = fluxo INVERTIDO. Grava estoque.json num gist secreto; o
#                     canal-ml (cron da HostGator) puxa de la. Nao passa pelo WAF.
#   "push"          = modo antigo. POST direto em api/erp-estoque.php (barrado
#                     pelo ModSecurity da HostGator quando roda de datacenter).
SINK            = (env("SINK", "gist") or "gist").lower()
GIST_ID         = env("GIST_ID")
GIST_TOKEN      = env("GIST_TOKEN")
GIST_FILE       = env("GIST_FILE", "estoque.json")


def die(msg: str, code: int = 1):
    print(f"[bridge] ERRO: {msg}", flush=True)
    sys.exit(code)


def millennium_base() -> str:
    u = MILLENNIUM_URL.rstrip("/")
    # tolera config apontando pro /$help
    if u.endswith("/$help"):
        u = u[: -len("/$help")].rstrip("/")
    # aceita host puro ou ja com /api/millenium_eco
    if "/api/" in u.lower():
        return u
    return u + "/api/millenium_eco"


def basic_auth_header() -> str:
    raw = f"{MILLENNIUM_USER}:{MILLENNIUM_PASS}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


def http_get_json(url: str, headers: dict) -> tuple[int, dict, str]:
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            body = r.read().decode("utf-8", "replace")
            status = r.status
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")
        status = e.code
    except Exception as e:  # rede/timeout
        return 0, {}, f"net_error: {e}"
    try:
        data = json.loads(body)
    except Exception:
        data = {}
    return status, data, body


def _origin_de(url: str) -> str:
    p = urllib.parse.urlsplit(url)
    return f"{p.scheme}://{p.netloc}" if p.scheme and p.netloc else ""


# User-Agent de Chrome real. O ModSecurity da HostGator devolve 406 para
# requisicoes que "parecem robo": User-Agent de Python/urllib OU faltando
# cabecalhos que todo navegador manda (Accept-Language, Referer, Origin).
# Testado: do navegador do usuario a mesma requisicao passa (401 token);
# do datacenter, sem esses cabecalhos, apanha 406. Entao imitamos o browser.
_UA_CHROME = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)


def http_post_json(url: str, payload: dict) -> tuple[int, dict, str]:
    body = json.dumps(payload).encode("utf-8")
    origin = _origin_de(url)
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        "X-ERP-Token": ERP_PUSH_TOKEN,
        "User-Agent": _UA_CHROME,
    }
    if origin:
        headers["Origin"] = origin
        headers["Referer"] = origin + "/"
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            txt = r.read().decode("utf-8", "replace")
            status = r.status
    except urllib.error.HTTPError as e:
        txt = e.read().decode("utf-8", "replace")
        status = e.code
    except Exception as e:
        return 0, {}, f"net_error: {e}"
    try:
        data = json.loads(txt)
    except Exception:
        data = {}
    return status, data, txt


def puxar_estoque() -> list[dict]:
    """Full pull do saldo de estoque da vitrine (CDC por trans_id, cursor comeca em 0)."""
    base = millennium_base()
    headers = {
        "Authorization": basic_auth_header(),
        "Accept": "application/json",
        "User-Agent": _UA_CHROME,
    }
    cursor = 0
    itens: list[dict] = []
    seen: set[str] = set()

    for page in range(1, MAX_PAGES + 1):
        url = (
            f"{base}/produtos/saldodeestoque"
            f"?vitrine={VITRINE}&trans_id={cursor}&$format=json"
        )
        status, data, raw = http_get_json(url, headers)

        if status == 0:
            die(f"falha de rede no Millennium (pagina {page}): {raw}")
        # licenca ocupada / indisponivel -> backoff curto e retenta a mesma pagina
        if status in (429, 500, 503) and "licen" in raw.lower():
            print(f"[bridge] licenca ocupada (HTTP {status}), aguardando 3s...", flush=True)
            time.sleep(3)
            continue
        if status < 200 or status >= 300:
            die(f"HTTP {status} do Millennium (pagina {page}): {raw[:300]}")

        rows = data.get("value") or []  # OData: itens vem em "value" (singular)
        if not rows:
            break

        max_trans = cursor
        for r in rows:
            try:
                t = int(r.get("trans_id") or 0)
            except (TypeError, ValueError):
                t = 0
            if t > max_trans:
                max_trans = t
            sku = str(r.get("sku") or "").strip()
            if not sku or sku in seen:
                continue
            seen.add(sku)
            try:
                saldo = float(r.get(SALDO_FIELD) or 0)
            except (TypeError, ValueError):
                saldo = 0.0
            itens.append({"codigo": sku, "estoque": saldo})

        if max_trans <= cursor:
            break
        cursor = max_trans

    return itens


def empurrar_para_canal(itens: list[dict]) -> None:
    if not CANAL_BASE:
        die("CANAL_BASE nao configurado.")
    if not ERP_PUSH_TOKEN:
        die("ERP_PUSH_TOKEN nao configurado.")

    push_url = f"{CANAL_BASE}/api/erp-estoque.php"
    enviados = 0
    total = len(itens)

    for i in range(0, total, BATCH_SIZE):
        lote = itens[i : i + BATCH_SIZE]
        if DRY_RUN:
            amostra = ", ".join(f"{x['codigo']}={x['estoque']}" for x in lote[:3])
            print(f"[bridge] DRY-RUN lote {i//BATCH_SIZE+1}: {len(lote)} itens (amostra: {amostra})", flush=True)
            enviados += len(lote)
            continue

        status, data, raw = http_post_json(push_url, {"token": ERP_PUSH_TOKEN, "itens": lote})
        if status == 0:
            die(f"falha de rede no push canal-ml: {raw}")
        if status < 200 or status >= 300 or not data.get("ok"):
            die(f"push canal-ml recusou (HTTP {status}): {raw[:800]}")
        enviados += len(lote)
        print(
            f"[bridge] lote {i//BATCH_SIZE+1}: {len(lote)} itens; "
            f"canal ok={data.get('ok')} enfileirados={data.get('enfileirados')}",
            flush=True,
        )

    print(f"[bridge] FIM. total_itens={total} enviados={enviados} dry_run={DRY_RUN}", flush=True)


def gravar_no_gist(itens: list[dict]) -> None:
    """Fluxo invertido: grava estoque.json num gist secreto via API do GitHub.
    O canal-ml (cron da HostGator) puxa esse gist por HTTPS e aplica o estoque."""
    if not GIST_ID:
        die("configure GIST_ID (id do gist secreto).")
    if not GIST_TOKEN:
        die("configure GIST_TOKEN (PAT com escopo gist).")

    feed = {
        "gerado_em": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "vitrine": VITRINE,
        "campo_saldo": SALDO_FIELD,
        "total": len(itens),
        "itens": itens,
    }
    conteudo = json.dumps(feed, ensure_ascii=False)

    if DRY_RUN:
        amostra = ", ".join(f"{x['codigo']}={x['estoque']}" for x in itens[:3])
        print(f"[bridge] DRY-RUN: gravaria {len(itens)} itens no gist "
              f"{GIST_ID} (amostra: {amostra})", flush=True)
        return

    payload = json.dumps({"files": {GIST_FILE: {"content": conteudo}}}).encode("utf-8")
    req = urllib.request.Request(
        f"https://api.github.com/gists/{GIST_ID}",
        data=payload,
        headers={
            "Authorization": f"Bearer {GIST_TOKEN}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
            "User-Agent": "WolfBridge-Puller",
        },
        method="PATCH",
    )
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            status = r.status
            r.read()
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")
        die(f"GitHub recusou o update do gist (HTTP {e.code}): {body[:400]}")
    except Exception as e:
        die(f"falha de rede ao gravar o gist: {e}")

    if status < 200 or status >= 300:
        die(f"update do gist retornou HTTP {status}.")
    print(f"[bridge] gist {GIST_ID} atualizado: {len(itens)} itens.", flush=True)


def main():
    if not MILLENNIUM_URL or not MILLENNIUM_USER or not MILLENNIUM_PASS:
        die("configure MILLENNIUM_URL, MILLENNIUM_USER e MILLENNIUM_PASS.")
    if VITRINE in ("", "0"):
        die("configure VITRINE (id inteiro da vitrine).")

    print(
        f"[bridge] inicio. vitrine={VITRINE} campo_saldo={SALDO_FIELD} "
        f"sink={SINK} dry_run={DRY_RUN}",
        flush=True,
    )
    itens = puxar_estoque()
    print(f"[bridge] Millennium retornou {len(itens)} SKUs.", flush=True)
    if not itens:
        print("[bridge] nada para enviar (0 itens). Verifique vitrine/credenciais.", flush=True)
        return

    if SINK == "push":
        empurrar_para_canal(itens)
    else:
        gravar_no_gist(itens)


if __name__ == "__main__":
    main()
