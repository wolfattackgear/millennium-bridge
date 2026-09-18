#!/usr/bin/env python3
"""
Worker de PEDIDOS: canal-ml -> Millennium -> (operador fatura) -> XML -> canal-ml.

O canal-ml NÃO fatura. Este worker só:
  METADE 1 (criar): job 'pendente' -> cria o PEDIDO DE VENDA no Millennium (`inclui`).
                    Quem fatura é o OPERADOR, manualmente, dentro do Millennium.
  METADE 2 (colher): job 'criado' -> consulta o status; se o operador já FATUROU
                    (status=3), puxa o XML da NF-e (listafaturamentos gera_xml=S)
                    e devolve. O canal-ml anexa esse XML no Mercado Livre.

Ele NUNCA aprova/libera/emite nota — isso é do operador no Millennium.

Contrato via gist (mesmo gist secreto):
  pedidos_fila.json      (canal-ml escreve) -> {jobs:[{cod_pedidov,order_id,pack_id,status,pedidov,payload}]}
  pedidos_resultado.json (este worker escreve) -> {resultados:[{cod_pedidov,order_id,fase,pedidov,nota,serie,chave,xml,erro}]}
    fase: 'criado' | 'faturado' | 'erro'

SEGURANÇA: WORKER_ENABLED começa 0 (não cria nada). Ligue WORKER_ENABLED=1 depois
de validar o dry-run e confirmar os valores (vitrine, tipo_pgto) com o analista.
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

WORKER_ENABLED  = env("WORKER_ENABLED", "0") not in ("", "0", "false", "False")

HTTP_TIMEOUT    = int(env("HTTP_TIMEOUT", "60"))
NET_RETRIES     = int(env("NET_RETRIES", "3"))
NET_BACKOFF     = int(env("NET_BACKOFF", "5"))

_UA_CHROME = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)

STATUS_FATURADO = 3
STATUS_CANCELADO = 5


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


def _headers() -> dict:
    raw = f"{MILLENNIUM_USER}:{MILLENNIUM_PASS}".encode("utf-8")
    return {
        "Authorization": "Basic " + base64.b64encode(raw).decode("ascii"),
        "Accept": "application/json",
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": _UA_CHROME,
    }


def millennium(method: str, metodo: str, payload: dict | None = None, query: str = "") -> tuple[int, dict, str]:
    base = millennium_base()
    url = f"{base}/{metodo}?$format=json" if not query else f"{base}/{metodo}?{query}&$format=json"
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    tent_rede = 0
    tent_lic = 0
    while True:
        req = urllib.request.Request(url, data=data, headers=_headers(), method=method)
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
                body = r.read().decode("utf-8", "replace")
                status = r.status
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace")
            status = e.code
        except Exception as e:
            if tent_rede < NET_RETRIES:
                tent_rede += 1
                log(f"timeout/rede em {metodo}, tentativa {tent_rede}/{NET_RETRIES}...")
                time.sleep(NET_BACKOFF)
                continue
            return 0, {}, f"net_error: {e}"
        if status in (429, 500, 503) and "licen" in body.lower() and tent_lic < 5:
            tent_lic += 1
            log(f"licença ocupada (HTTP {status}) em {metodo}, tentativa {tent_lic}/5...")
            time.sleep(3)
            continue
        try:
            parsed = json.loads(body)
        except Exception:
            parsed = {}
        return status, parsed, body


def _values(data: dict) -> list:
    v = data.get("values")
    if isinstance(v, list):
        return v
    v = data.get("value")
    return v if isinstance(v, list) else []


def buscar_pedidov(cod: str) -> tuple[str, object]:
    """Retorna (pedidov, status) do pedido pelo cod_pedidov, ou ('', None)."""
    st, data, raw = millennium("GET", "pedido_venda/listapedidos", query=f"cod_pedidov={cod}&vitrine={VITRINE}")
    for r in _values(data):
        if str(r.get("cod_pedidov") or "") == cod:
            return str(r.get("pedidov") or ""), r.get("status")
    vs = _values(data)
    if vs:
        return str(vs[0].get("pedidov") or ""), vs[0].get("status")
    return "", None


# ------------------------- Gist -------------------------

def gist_ler(file: str) -> dict | None:
    req = urllib.request.Request(
        f"https://api.github.com/gists/{GIST_ID}",
        headers={"Authorization": f"Bearer {GIST_TOKEN}", "Accept": "application/vnd.github+json",
                 "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "WolfBridge-PedidoWorker"},
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
        f"https://api.github.com/gists/{GIST_ID}", data=payload,
        headers={"Authorization": f"Bearer {GIST_TOKEN}", "Accept": "application/vnd.github+json",
                 "X-GitHub-Api-Version": "2022-11-28", "Content-Type": "application/json",
                 "User-Agent": "WolfBridge-PedidoWorker"},
        method="PATCH",
    )
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            r.read()
    except urllib.error.HTTPError as e:
        die(f"GitHub recusou o resultado (HTTP {e.code}): {e.read().decode('utf-8','replace')[:300]}")
    except Exception as e:
        die(f"falha de rede ao gravar resultado: {e}")


# ------------------------- Fases -------------------------

def criar(job: dict) -> dict:
    """METADE 1: cria o pedido de venda no Millennium (se ainda não existe)."""
    cod = str(job.get("cod_pedidov") or "")
    order_id = str(job.get("order_id") or "")
    res = {"cod_pedidov": cod, "order_id": order_id, "etapa": "inclui"}

    # já existe? (idempotência — não recria)
    pedidov, _status = buscar_pedidov(cod)
    if pedidov:
        log(f"{cod}: já existe no Millennium (pedidov={pedidov}); não recria.")
        return {**res, "fase": "criado", "pedidov": pedidov}

    st, data, raw = millennium("POST", "pedido_venda/inclui", job.get("payload") or {})
    if st < 200 or st >= 300:
        return {**res, "fase": "erro", "erro": f"inclui HTTP {st}: {raw[:300]}"}
    log(f"{cod}: pedido de venda criado (HTTP {st}).")

    pedidov, _status = buscar_pedidov(cod)
    return {**res, "fase": "criado", "pedidov": pedidov}


def colher(job: dict) -> dict | None:
    """METADE 2: se o operador já faturou, puxa o XML. Senão, mantém 'criado'."""
    cod = str(job.get("cod_pedidov") or "")
    order_id = str(job.get("order_id") or "")
    pedidov = str(job.get("pedidov") or "")
    res = {"cod_pedidov": cod, "order_id": order_id, "etapa": "consultastatus"}

    if not pedidov:
        pedidov, _ = buscar_pedidov(cod)
    if not pedidov:
        return {**res, "fase": "erro", "erro": f"não achei o pedidov do cod {cod} no Millennium."}

    st, data, raw = millennium("GET", "pedido_venda/consultastatus", query=f"list_pedidov=({pedidov})&vitrine={VITRINE}")
    status = None
    nota = ""
    for r in _values(data):
        if str(r.get("pedidov") or "") == pedidov:
            status = r.get("status")
            nota = str(r.get("nfs") or "")
            break

    if status == STATUS_CANCELADO:
        return {**res, "fase": "erro", "pedidov": pedidov, "erro": "pedido cancelado no Millennium."}
    if status != STATUS_FATURADO:
        log(f"{cod}: aguardando operador faturar (status={status}).")
        return {**res, "fase": "criado", "pedidov": pedidov}  # segue esperando

    # faturado -> pega XML + chave
    st, data, raw = millennium("GET", "pedido_venda/listafaturamentos", query=f"cod_pedidov={cod}&gera_xml=S&vitrine={VITRINE}")
    xml = chave = serie = ""
    for r in _values(data):
        if r.get("xml"):
            xml = str(r.get("xml"))
            chave = str(r.get("chave_nf") or "")
            serie = str(r.get("serie_nf") or "")
            nota = str(r.get("nf") or "") or nota
            break
    if not xml:
        st, data, raw = millennium("GET", "pedido_venda/consultaxmlnfe", query=f"cod_pedidov={cod}")
        for r in _values(data):
            for x in (r.get("xmls") or []):
                if x.get("xml"):
                    xml = str(x.get("xml"))
                    break
    if not xml:
        log(f"{cod}: faturado mas XML ainda indisponível; tenta no próximo ciclo.")
        return {**res, "fase": "criado", "pedidov": pedidov}

    log(f"{cod}: operador faturou; XML obtido ({len(xml)} bytes), chave={chave}.")
    return {**res, "fase": "faturado", "pedidov": pedidov, "xml": xml, "chave": chave, "serie": serie, "nota": nota}


def main():
    if not GIST_ID or not GIST_TOKEN:
        die("configure GIST_PEDIDOS_ID e GIST_PEDIDOS_TOKEN.")
    if not MILLENNIUM_URL or not MILLENNIUM_USER or not MILLENNIUM_PASS:
        die("configure MILLENNIUM_URL, MILLENNIUM_USER e MILLENNIUM_PASS.")

    fila = gist_ler(FILA_FILE) or {}
    jobs = fila.get("jobs") or []
    log(f"início. jobs={len(jobs)} worker_enabled={WORKER_ENABLED} vitrine={VITRINE}")
    if not jobs:
        log("nada na fila.")
        return
    if not WORKER_ENABLED:
        log("WORKER_ENABLED=0 -> modo seguro: não vou criar nem colher nada. "
            "Ligue WORKER_ENABLED=1 quando validar os valores com o analista.")
        return

    resultados = []
    for job in jobs:
        cod = str(job.get("cod_pedidov") or "")
        status = str(job.get("status") or "pendente")
        try:
            if status == "pendente":
                r = criar(job)
                # se criou agora, já tenta colher no mesmo ciclo (caso o operador seja rápido)
                if r.get("fase") == "criado":
                    r2 = colher({**job, "pedidov": r.get("pedidov"), "status": "criado"})
                    if r2 and r2.get("fase") in ("faturado", "erro"):
                        r = r2
            else:  # 'criado' / 'processando'
                r = colher(job) or {"cod_pedidov": cod, "order_id": str(job.get("order_id") or ""), "fase": "criado"}
        except Exception as e:
            r = {"cod_pedidov": cod, "order_id": str(job.get("order_id") or ""), "fase": "erro", "etapa": "excecao", "erro": str(e)}
        resultados.append(r)

    gist_gravar(RES_FILE, {
        "gerado_em": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "total": len(resultados),
        "resultados": resultados,
    })
    criados = sum(1 for r in resultados if r.get("fase") == "criado")
    fat = sum(1 for r in resultados if r.get("fase") == "faturado")
    err = sum(1 for r in resultados if r.get("fase") == "erro")
    log(f"FIM. criados/aguardando={criados} faturados={fat} erros={err}")


if __name__ == "__main__":
    main()
