#!/usr/bin/env python3
"""
Worker de PEDIDOS: canal-ml -> Millennium -> NF-e -> canal-ml (roda na nuvem).

Espelho do puller.py, mas na direção de ESCRITA. A HostGator não alcança o
Millennium:6017; este worker (GitHub Actions/Railway) alcança. Ele:

  1) lê a FILA de pedidos num gist secreto (pedidos_fila.json, escrito pelo canal-ml);
  2) para cada job, roda a sequência no Millennium:
        inclui -> listapedidos (pega pedidov) -> processastatus (aprova)
        -> liberarprocessamento -> consultastatus (até status=3 Faturado)
        -> listafaturamentos(gera_xml=S) (pega xml + chave);
  3) grava o RESULTADO noutro gist (pedidos_resultado.json), que o canal-ml lê
     e usa para anexar o XML no Mercado Livre.

SEGURANÇA: por padrão FATURAR_ENABLED=0 -> o worker NÃO cria nem fatura nada
(fica só de prontidão). Ligue FATURAR_ENABLED=1 depois de confirmar com o
analista Millennium a sequência de status e os valores (filial, tipo_pgto, vitrine).

Config: 100% por variáveis de ambiente (nada de segredo no código). Ver README.
"""

import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request


def env(name: str, default: str = "") -> str:
    return (os.environ.get(name, default) or "").strip()


MILLENNIUM_URL  = env("MILLENNIUM_URL")
MILLENNIUM_USER = env("MILLENNIUM_USER")
MILLENNIUM_PASS = env("MILLENNIUM_PASS")
VITRINE         = env("VITRINE", "0")

GIST_ID         = env("GIST_PEDIDOS_ID", env("GIST_ID"))
GIST_TOKEN      = env("GIST_PEDIDOS_TOKEN", env("GIST_TOKEN"))
FILA_FILE       = env("FILA_FILE", "pedidos_fila.json")
RES_FILE        = env("RES_FILE", "pedidos_resultado.json")

# Trava de segurança: só fatura de verdade quando ligado explicitamente.
FATURAR_ENABLED = env("FATURAR_ENABLED", "0") not in ("", "0", "false", "False")

# Sequência de aprovação/faturamento (a confirmar com o analista Millennium).
PROCESSA_STATUS = env("PROCESSA_STATUS", "1")          # 1 = Pagamento Confirmado
USA_PROCESSA    = env("USA_PROCESSA", "1") not in ("", "0", "false", "False")
USA_LIBERAR     = env("USA_LIBERAR", "1") not in ("", "0", "false", "False")

POLL_TRIES      = int(env("POLL_TRIES", "20"))
POLL_SLEEP      = int(env("POLL_SLEEP", "6"))
HTTP_TIMEOUT    = int(env("HTTP_TIMEOUT", "60"))
NET_RETRIES     = int(env("NET_RETRIES", "3"))
NET_BACKOFF     = int(env("NET_BACKOFF", "5"))

_UA_CHROME = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)


def log(msg: str) -> None:
    print(f"[pedido-worker] {msg}", flush=True)


def die(msg: str, code: int = 1):
    log(f"ERRO: {msg}")
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


def _headers() -> dict:
    return {
        "Authorization": basic_auth_header(),
        "Accept": "application/json",
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": _UA_CHROME,
    }


def _request(method: str, url: str, payload: dict | None = None) -> tuple[int, dict, str]:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(url, data=data, headers=_headers(), method=method)
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
        parsed = json.loads(body)
    except Exception:
        parsed = {}
    return status, parsed, body


def millennium(method: str, metodo: str, payload: dict | None = None, query: str = "") -> tuple[int, dict, str]:
    """Chama um método do Millennium com retry de rede e de licença ocupada."""
    base = millennium_base()
    url = f"{base}/{metodo}?$format=json"
    if query:
        url = f"{base}/{metodo}?{query}&$format=json"
    tent_rede = 0
    tent_lic = 0
    while True:
        status, data, raw = _request(method, url, payload)
        if status == 0:
            if tent_rede < NET_RETRIES:
                tent_rede += 1
                log(f"timeout/rede em {metodo}, tentativa {tent_rede}/{NET_RETRIES}, aguardando {NET_BACKOFF}s...")
                time.sleep(NET_BACKOFF)
                continue
            return 0, {}, raw
        if status in (429, 500, 503) and "licen" in raw.lower():
            if tent_lic < 5:
                tent_lic += 1
                log(f"licença ocupada (HTTP {status}) em {metodo}, tentativa {tent_lic}/5, aguardando 3s...")
                time.sleep(3)
                continue
        return status, data, raw


def _values(data: dict) -> list:
    v = data.get("values")
    if isinstance(v, list):
        return v
    v = data.get("value")
    return v if isinstance(v, list) else []


# ------------------------- Gist -------------------------

def gist_ler(file: str) -> dict | None:
    req = urllib.request.Request(
        f"https://api.github.com/gists/{GIST_ID}",
        headers={
            "Authorization": f"Bearer {GIST_TOKEN}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "WolfBridge-PedidoWorker",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            data = json.loads(r.read().decode("utf-8", "replace"))
    except Exception as e:
        log(f"falha ao ler gist: {e}")
        return None
    content = (((data.get("files") or {}).get(file) or {}).get("content"))
    if not content:
        return None
    try:
        return json.loads(content)
    except Exception:
        return None


def gist_gravar(file: str, obj: dict) -> None:
    payload = json.dumps({"files": {file: {"content": json.dumps(obj, ensure_ascii=False)}}}).encode("utf-8")
    req = urllib.request.Request(
        f"https://api.github.com/gists/{GIST_ID}",
        data=payload,
        headers={
            "Authorization": f"Bearer {GIST_TOKEN}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
            "User-Agent": "WolfBridge-PedidoWorker",
        },
        method="PATCH",
    )
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            r.read()
    except urllib.error.HTTPError as e:
        die(f"GitHub recusou o update do gist de resultado (HTTP {e.code}): {e.read().decode('utf-8','replace')[:300]}")
    except Exception as e:
        die(f"falha de rede ao gravar resultado: {e}")


# ------------------------- Sequência de faturamento -------------------------

def faturar_job(job: dict) -> dict:
    """Roda inclui -> aprova -> libera -> acompanha -> pega XML. Retorna o resultado."""
    cod = str(job.get("cod_pedidov") or "").strip()
    order_id = str(job.get("order_id") or "").strip()
    payload = job.get("payload") or {}
    res = {"cod_pedidov": cod, "order_id": order_id, "ok": False, "etapa": "inclui"}

    # 1) inclui
    st, data, raw = millennium("POST", "pedido_venda/inclui", payload)
    if st < 200 or st >= 300:
        res["erro"] = f"inclui HTTP {st}: {raw[:300]}"
        return res
    log(f"{cod}: incluído (HTTP {st}).")

    # 2) descobrir pedidov (id interno)
    st, data, raw = millennium("GET", "pedido_venda/listapedidos", query=f"cod_pedidov={cod}&vitrine={VITRINE}")
    pedidov = ""
    for r in _values(data):
        if str(r.get("cod_pedidov") or "") == cod:
            pedidov = str(r.get("pedidov") or "")
            break
    if not pedidov and _values(data):
        pedidov = str(_values(data)[0].get("pedidov") or "")
    res["pedidov"] = pedidov
    if not pedidov:
        res["etapa"] = "listapedidos"
        res["erro"] = f"pedido criado mas pedidov não encontrado (cod {cod})."
        return res
    log(f"{cod}: pedidov interno = {pedidov}.")

    # 3) aprovar (processastatus) — sequência confirmável por env
    if USA_PROCESSA:
        res["etapa"] = "processastatus"
        body = {
            "vitrine": int(VITRINE or 0),
            "status_pedidos": [{"cod_pedidov": cod, "status": int(PROCESSA_STATUS or 1)}],
        }
        st, data, raw = millennium("POST", "pedido_venda/processastatus", body)
        if st < 200 or st >= 300:
            res["erro"] = f"processastatus HTTP {st}: {raw[:300]}"
            return res
        log(f"{cod}: processastatus status={PROCESSA_STATUS} ok.")

    # 4) liberar processamento
    if USA_LIBERAR:
        res["etapa"] = "liberarprocessamento"
        st, data, raw = millennium("POST", "pedido_venda/liberarprocessamento", {"pedidov": int(pedidov)})
        if st < 200 or st >= 300:
            res["erro"] = f"liberarprocessamento HTTP {st}: {raw[:300]}"
            return res
        log(f"{cod}: liberado para processamento.")

    # 5) acompanhar até status=3 (Faturado)
    res["etapa"] = "consultastatus"
    status_final = None
    nota = ""
    for i in range(POLL_TRIES):
        st, data, raw = millennium("GET", "pedido_venda/consultastatus", query=f"list_pedidov=({pedidov})&vitrine={VITRINE}")
        for r in _values(data):
            if str(r.get("pedidov") or "") == pedidov:
                status_final = r.get("status")
                nota = str(r.get("nfs") or "") or nota
                break
        log(f"{cod}: consultastatus tentativa {i+1}/{POLL_TRIES} -> status={status_final} nfs={nota}")
        if status_final == 3:
            break
        if status_final == 5:
            res["erro"] = "pedido foi cancelado no Millennium."
            return res
        time.sleep(POLL_SLEEP)

    res["status"] = status_final
    res["nota"] = nota
    if status_final != 3:
        res["erro"] = f"NF-e não faturou a tempo (status={status_final} após {POLL_TRIES} tentativas)."
        return res

    # 6) pegar XML + chave
    res["etapa"] = "listafaturamentos"
    st, data, raw = millennium("GET", "pedido_venda/listafaturamentos", query=f"cod_pedidov={cod}&gera_xml=S&vitrine={VITRINE}")
    xml = ""
    chave = ""
    serie = ""
    for r in _values(data):
        if r.get("xml"):
            xml = str(r.get("xml"))
            chave = str(r.get("chave_nf") or "")
            serie = str(r.get("serie_nf") or "")
            nota = str(r.get("nf") or "") or nota
            break
    if not xml:
        # fallback: consultaxmlnfe
        st, data, raw = millennium("GET", "pedido_venda/consultaxmlnfe", query=f"cod_pedidov={cod}")
        for r in _values(data):
            for x in (r.get("xmls") or []):
                if x.get("xml"):
                    xml = str(x.get("xml"))
                    break
    if not xml:
        res["erro"] = "faturado, mas não consegui obter o XML (listafaturamentos/consultaxmlnfe vazios)."
        return res

    res.update({"ok": True, "etapa": "faturado", "xml": xml, "chave": chave, "serie": serie, "nota": nota})
    log(f"{cod}: XML obtido ({len(xml)} bytes), chave={chave}.")
    return res


def main():
    if not GIST_ID or not GIST_TOKEN:
        die("configure GIST_PEDIDOS_ID e GIST_PEDIDOS_TOKEN.")
    if not MILLENNIUM_URL or not MILLENNIUM_USER or not MILLENNIUM_PASS:
        die("configure MILLENNIUM_URL, MILLENNIUM_USER e MILLENNIUM_PASS.")

    fila = gist_ler(FILA_FILE) or {}
    jobs = fila.get("jobs") or []
    log(f"início. jobs na fila={len(jobs)} faturar_enabled={FATURAR_ENABLED} vitrine={VITRINE}")

    if not jobs:
        log("nada na fila.")
        return

    if not FATURAR_ENABLED:
        log("FATURAR_ENABLED=0 -> modo seguro: NÃO vou criar/faturar nada. "
            "Ligue FATURAR_ENABLED=1 após confirmar a sequência com o analista.")
        return

    resultados = []
    for job in jobs:
        cod = str(job.get("cod_pedidov") or "")
        try:
            r = faturar_job(job)
        except Exception as e:
            r = {"cod_pedidov": cod, "order_id": str(job.get("order_id") or ""),
                 "ok": False, "etapa": "excecao", "erro": str(e)}
        resultados.append(r)

    gist_gravar(RES_FILE, {
        "gerado_em": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "total": len(resultados),
        "resultados": resultados,
    })
    ok = sum(1 for r in resultados if r.get("ok"))
    log(f"FIM. processados={len(resultados)} faturados={ok}")


if __name__ == "__main__":
    main()
