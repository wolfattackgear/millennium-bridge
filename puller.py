#!/usr/bin/env python3
"""
Ponte Millennium -> canal-ml. Puxa o estoque do Millennium e (no modo padrao)
grava num gist secreto; o canal-ml puxa desse gist pelo cron dele.
"""

import base64
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


def env(name: str, default: str = "") -> str:
    return (os.environ.get(name, default) or "").strip()

MILLENNIUM_URL  = env("MILLENNIUM_URL")
MILLENNIUM_USER = env("MILLENNIUM_USER")
MILLENNIUM_PASS = env("MILLENNIUM_PASS")
VITRINE         = env("VITRINE", "0")
CANAL_BASE      = env("CANAL_BASE").rstrip("/")
ERP_PUSH_TOKEN  = env("ERP_PUSH_TOKEN")

SALDO_FIELD     = env("SALDO_FIELD", "saldo")

BATCH_SIZE      = int(env("BATCH_SIZE", "300"))
MAX_PAGES       = int(env("MAX_PAGES", "50"))
HTTP_TIMEOUT    = int(env("HTTP_TIMEOUT", "60"))
NET_RETRIES     = int(env("NET_RETRIES", "3"))
NET_BACKOFF     = int(env("NET_BACKOFF", "5"))
DRY_RUN         = env("DRY_RUN", "0") not in ("", "0", "false", "False")

SINK            = (env("SINK", "gist") or "gist").lower()
GIST_ID         = env("GIST_ID")
GIST_TOKEN      = env("GIST_TOKEN")
GIST_FILE       = env("GIST_FILE", "estoque.json")


def die(msg: str, code: int = 1):
    print(f"[bridge] ERRO: {msg}", flush=True)
    sys.exit(code)


def millennium_base() -> str:
    u = MILLENNIUM_URL.rstrip("/")
    if u.endswith("/$help"):
        u = u[: -len("/$help")].rstrip("/")
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
    except Exception as e:
        return 0, {}, f"net_error: {e}"
    try:
        data = json.loads(body)
    except Exception:
        data = {}
    return status, data, body


_UA_CHROME = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)


def puxar_estoque() -> list[dict]:
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
        tent_rede = 0
        tent_lic = 0
        while True:
            status, data, raw = http_get_json(url, headers)
            if status == 0:
                if tent_rede < NET_RETRIES:
                    tent_rede += 1
                    print(f"[bridge] timeout/rede no Millennium (pagina {page}), "
                          f"tentativa {tent_rede}/{NET_RETRIES}, aguardando {NET_BACKOFF}s...", flush=True)
                    time.sleep(NET_BACKOFF)
                    continue
                die(f"falha de rede no Millennium (pagina {page}) apos {NET_RETRIES} tentativas: {raw}")
            if status in (429, 500, 503) and "licen" in raw.lower():
                if tent_lic < 5:
                    tent_lic += 1
                    print(f"[bridge] licenca ocupada (HTTP {status}), "
                          f"tentativa {tent_lic}/5, aguardando 3s...", flush=True)
                    time.sleep(3)
                    continue
                die(f"licenca do Millennium ocupada (pagina {page}) apos varias tentativas.")
            if status < 200 or status >= 300:
                die(f"HTTP {status} do Millennium (pagina {page}): {raw[:300]}")
            break

        rows = data.get("value") or []
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


def gravar_no_gist(itens: list[dict]) -> None:
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


def empurrar_para_canal(itens: list[dict]) -> None:
    if not CANAL_BASE:
        die("CANAL_BASE nao configurado.")
    if not ERP_PUSH_TOKEN:
        die("ERP_PUSH_TOKEN nao configurado.")

    push_url = f"{CANAL_BASE}/api/erp-estoque.php"
    p = urllib.parse.urlsplit(push_url)
    origin = f"{p.scheme}://{p.netloc}" if p.scheme and p.netloc else ""
    total = len(itens)
    enviados = 0

    for i in range(0, total, BATCH_SIZE):
        lote = itens[i : i + BATCH_SIZE]
        if DRY_RUN:
            enviados += len(lote)
            continue
        body = json.dumps({"token": ERP_PUSH_TOKEN, "itens": lote}).encode("utf-8")
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
        req = urllib.request.Request(push_url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
                txt = r.read().decode("utf-8", "replace")
                status = r.status
        except urllib.error.HTTPError as e:
            txt = e.read().decode("utf-8", "replace")
            status = e.code
        except Exception as e:
            die(f"falha de rede no push canal-ml: {e}")
        try:
            data = json.loads(txt)
        except Exception:
            data = {}
        if status < 200 or status >= 300 or not data.get("ok"):
            die(f"push canal-ml recusou (HTTP {status}): {txt[:800]}")
        enviados += len(lote)

    print(f"[bridge] FIM. total_itens={total} enviados={enviados} dry_run={DRY_RUN}", flush=True)


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
