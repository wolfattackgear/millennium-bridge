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
from datetime import datetime, timedelta


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

# Espelha o 4Middleware: "Cadastrar pedido e posteriormente enviar chamada
# processastatus aprovando". O pedido do ML já vem pago, então aprovamos (=1,
# Pagamento Confirmado). O operador ainda separa e FATURA manualmente no ERP.
USA_PROCESSA    = env("USA_PROCESSA", "1") not in ("", "0", "false", "False")
APROVAR_STATUS  = int(env("APROVAR_STATUS", "1"))

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
    if not isinstance(data, dict):
        return []
    for key in ("values", "value", "faturamentos", "pedidos", "data", "result"):
        v = data.get(key)
        if isinstance(v, list):
            return v
    return []


def _pick(row: dict, *names):
    if not isinstance(row, dict):
        return ""
    lower = {str(k).lower(): v for k, v in row.items()}
    for n in names:
        v = lower.get(str(n).lower())
        if v is None or v == "":
            continue
        return v
    return ""


def _norm_cod(c) -> str:
    d = "".join(ch for ch in str(c or "") if ch.isdigit())
    return d.lstrip("0") or d


def _truthy(v) -> bool:
    return v in (True, 1, "1", "S", "s", "true", "True", "sim")


def _xml_de_row(row: dict) -> str:
    xml = _pick(row, "xml", "xml_nfe", "xmlnfe", "xml_nf")
    if isinstance(xml, dict):
        xml = _pick(xml, "xml", "conteudo", "content")
    if xml:
        return str(xml)
    xmls = row.get("xmls") or row.get("XMLS") or []
    if isinstance(xmls, list):
        for x in xmls:
            if isinstance(x, dict) and (x.get("xml") or x.get("XML")):
                return str(x.get("xml") or x.get("XML"))
            if isinstance(x, str) and x.strip().startswith("<"):
                return x
    return ""


def buscar_pedidov(cod: str) -> tuple[str, object]:
    """Retorna (pedidov, status) do pedido pelo cod_pedidov, ou ('', None)."""
    queries = [f"cod_pedidov={cod}"]
    if VITRINE and VITRINE not in ("0", ""):
        queries.append(f"cod_pedidov={cod}&vitrine={VITRINE}")
    for q in queries:
        st, data, raw = millennium("GET", "pedido_venda/listapedidos", query=q)
        for r in _values(data):
            rc = str(_pick(r, "cod_pedidov") or "")
            if rc == cod or _norm_cod(rc) == _norm_cod(cod):
                return str(_pick(r, "pedidov") or ""), r.get("status") or _pick(r, "status")
        vs = _values(data)
        if vs:
            return str(_pick(vs[0], "pedidov") or ""), vs[0].get("status")
    return "", None


def puxar_xml_nfe(cod: str, pedidov: str = "", nota: str = "", chave: str = "") -> str:
    queries = [f"cod_pedidov={cod}"]
    if pedidov:
        queries.append(f"pedidov={pedidov}")
    if nota:
        queries.append(f"nf={nota}")
    if chave:
        queries.append(f"chave_nf={chave}")
    for q in queries:
        st, data, raw = millennium("GET", "pedido_venda/consultaxmlnfe", query=q)
        for r in _values(data):
            xml = _xml_de_row(r) if isinstance(r, dict) else ""
            if xml:
                return xml
            if isinstance(r, dict):
                for x in (r.get("xmls") or r.get("XMLS") or []):
                    if isinstance(x, dict) and (x.get("xml") or x.get("XML")):
                        return str(x.get("xml") or x.get("XML"))
        xml = _xml_de_row(data) if isinstance(data, dict) else ""
        if xml:
            return xml
    return ""


def colher_indice_notas(dias: int = 14) -> dict:
    """Indexa faturamentos recentes por cod_pedidov (sem zeros à esquerda)."""
    since = (datetime.utcnow() - timedelta(days=max(1, dias))).strftime("%Y-%m-%d")
    queries = [
        f"data_atualizacao={since}&gera_xml=S",
        f"DATA_ATUALIZACAO={since}&gera_xml=S",
        f"data_atualizacao={since}",
        f"DATA_ATUALIZACAO={since}",
    ]
    if VITRINE and VITRINE not in ("0", ""):
        queries.append(f"data_atualizacao={since}&gera_xml=S&vitrine={VITRINE}")

    rows = []
    used = ""
    for q in queries:
        st, data, raw = millennium("GET", "pedido_venda/listafaturamentos", query=q)
        vs = _values(data)
        log(f"listafaturamentos HTTP {st} n={len(vs)} q={q}")
        if vs:
            rows = vs
            used = q
            log("amostra chaves nota=" + ",".join(list(vs[0].keys())[:18]))
            break
        if isinstance(data, dict) and st >= 200:
            log("listafaturamentos chaves resposta=" + ",".join(list(data.keys())[:12]))

    idx = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        if _truthy(_pick(r, "cancelado", "cancelada")):
            continue
        cod = _norm_cod(_pick(r, "cod_pedidov", "codpedidov", "pedido"))
        if not cod:
            continue
        prev = idx.get(cod)
        xml = _xml_de_row(r)
        if prev is None or (xml and not _xml_de_row(prev)):
            idx[cod] = r
    log(f"notas indexadas={len(idx)}" + (f" via {used}" if used else " (vazio)"))
    return idx


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

def aprovar(cod: str) -> tuple[bool, str]:
    """processastatus aprovando (status=1, Pagamento Confirmado). Igual 4Middleware."""
    body = {"vitrine": int(VITRINE or 0), "status_pedidos": [{"cod_pedidov": cod, "status": APROVAR_STATUS}]}
    st, data, raw = millennium("POST", "pedido_venda/processastatus", body)
    if st < 200 or st >= 300:
        return False, f"processastatus HTTP {st}: {raw[:300]}"
    # o retorno traz acoes[]; se alguma ação for 100 (erro), reporta
    for a in (data.get("acoes") or []):
        if a.get("acao") == 100 and a.get("erro"):
            return False, f"processastatus erro: {str(a.get('erro'))[:250]}"
    return True, ""


def _resumo_payload(payload: dict) -> str:
    prods = payload.get("produtos") if isinstance(payload, dict) else None
    skus = []
    if isinstance(prods, list):
        for p in prods:
            if isinstance(p, dict) and p.get("sku"):
                skus.append(str(p.get("sku")))
    vit = payload.get("vitrine") if isinstance(payload, dict) else None
    return f"vitrine={vit} skus={','.join(skus) or '-'}"


def criar(job: dict) -> dict:
    """METADE 1: cadastra o pedido de venda e aprova (processastatus), se ainda não existe."""
    cod = str(job.get("cod_pedidov") or "")
    order_id = str(job.get("order_id") or "")
    res = {"cod_pedidov": cod, "order_id": order_id, "etapa": "inclui"}
    payload = job.get("payload") or {}
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            log(f"{cod}: FALHA inclui — payload não é JSON.")
            return {**res, "fase": "erro", "erro": "payload do gist não é JSON objeto"}
    if not isinstance(payload, dict) or not payload:
        log(f"{cod}: FALHA inclui — payload vazio.")
        return {**res, "fase": "erro", "erro": "payload vazio"}
    # vitrine do worker prevalece (evita mismatch com o secret VITRINE)
    if VITRINE and str(VITRINE) not in ("", "0"):
        try:
            payload["vitrine"] = int(VITRINE)
        except ValueError:
            payload["vitrine"] = VITRINE

    # já existe? (idempotência — não recria nem reaprova)
    pedidov, _status = buscar_pedidov(cod)
    if pedidov:
        log(f"{cod}: já existe no Millennium (pedidov={pedidov}); não recria.")
        return {**res, "fase": "criado", "pedidov": pedidov}

    log(f"{cod}: enviando inclui ({_resumo_payload(payload)}).")
    st, data, raw = millennium("POST", "pedido_venda/inclui", payload)
    if st < 200 or st >= 300:
        trecho = (raw or "")[:300].replace("\n", " ")
        log(f"{cod}: FALHA inclui HTTP {st}: {trecho}")
        return {**res, "fase": "erro", "erro": f"inclui HTTP {st}: {raw[:300]}"}
    log(f"{cod}: pedido de venda cadastrado (HTTP {st}).")

    pedidov, _status = buscar_pedidov(cod)

    # aprovar (processastatus) — pedido do ML já vem pago
    if USA_PROCESSA:
        ok, err = aprovar(cod)
        if not ok:
            # cadastrou mas não aprovou (licença/rede?). Marca erro; a fila retenta
            # no próximo ciclo — o inclui é idempotente (buscar_pedidov acha e não recria).
            return {**res, "etapa": "processastatus", "fase": "erro", "pedidov": pedidov, "erro": err}
        log(f"{cod}: aprovado (processastatus status={APROVAR_STATUS}).")

    return {**res, "fase": "criado", "pedidov": pedidov}


def colher(job: dict, indice: dict | None = None) -> dict | None:
    """METADE 2: se o operador já FATUROU, puxa o XML da NF-e."""
    cod = str(job.get("cod_pedidov") or "")
    order_id = str(job.get("order_id") or "")
    pedidov = str(job.get("pedidov") or "")
    res = {"cod_pedidov": cod, "order_id": order_id, "etapa": "listafaturamentos", "pedidov": pedidov}

    row = None
    if isinstance(indice, dict):
        row = indice.get(_norm_cod(cod))

    # fallback: consulta pontual (alguns tenants só filtram por cod_pedidov)
    if row is None:
        st, data, raw = millennium(
            "GET", "pedido_venda/listafaturamentos",
            query=f"cod_pedidov={cod}&gera_xml=S",
        )
        for r in _values(data):
            if isinstance(r, dict) and not _truthy(_pick(r, "cancelado", "cancelada")):
                row = r
                break
        if row is None and st >= 200:
            log(f"{cod}: listafaturamentos pontual HTTP {st} n={len(_values(data))}")

    xml = chave = serie = nota = ""
    if isinstance(row, dict):
        xml = _xml_de_row(row)
        chave = str(_pick(row, "chave_nf", "chave_nfe", "nfe_chave", "chave") or "")
        serie = str(_pick(row, "serie_nf", "serie_nfe", "serie") or "")
        nota = str(_pick(row, "nf", "nota", "nro_nf", "numero_nf") or "")
        if not pedidov:
            pedidov = str(_pick(row, "pedidov") or "")
            res["pedidov"] = pedidov

    if not xml:
        xml = puxar_xml_nfe(cod, pedidov, nota, chave)

    # pedido já faturado no ERP (status=3) mas a lista não trouxe XML
    if not xml:
        pv, stt = buscar_pedidov(cod)
        if pv:
            pedidov = pedidov or pv
            res["pedidov"] = pedidov
        try:
            stt_i = int(stt) if stt is not None and str(stt).strip() != "" else None
        except (TypeError, ValueError):
            stt_i = None
        if stt_i == STATUS_FATURADO:
            log(f"{cod}: listapedidos status=faturado; tentando consultaxmlnfe.")
            xml = puxar_xml_nfe(cod, pedidov, nota, chave)

    if xml:
        log(f"{cod}: operador faturou; XML obtido ({len(xml)} bytes), nf={nota} chave={chave}.")
        return {**res, "fase": "faturado", "pedidov": pedidov, "xml": xml, "chave": chave, "serie": serie, "nota": nota}

    log(f"{cod}: aguardando operador faturar (sem nota ainda).")
    return {**res, "fase": "criado", "pedidov": pedidov}


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
    indice = colher_indice_notas(14)
    for i, job in enumerate(jobs, 1):
        cod = str(job.get("cod_pedidov") or "")
        status = str(job.get("status") or "pendente")
        log(f"job {i}/{len(jobs)} {cod} status={status}")
        try:
            if status in ("pendente", "erro"):
                r = criar(job)
                if r.get("fase") == "criado":
                    r2 = colher({**job, "pedidov": r.get("pedidov"), "status": "criado"}, indice)
                    if r2 and r2.get("fase") in ("faturado", "erro"):
                        r = r2
            else:  # 'criado' / 'processando'
                r = colher(job, indice) or {"cod_pedidov": cod, "order_id": str(job.get("order_id") or ""), "fase": "criado"}
        except Exception as e:
            log(f"{cod}: EXCECAO {type(e).__name__}: {e}")
            r = {"cod_pedidov": cod, "order_id": str(job.get("order_id") or ""), "fase": "erro", "etapa": "excecao", "erro": str(e)}
        if r.get("fase") == "erro":
            log(f"{cod}: resultado=erro etapa={r.get('etapa')} {str(r.get('erro') or '')[:200]}")
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
