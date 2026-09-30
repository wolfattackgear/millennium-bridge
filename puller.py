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
import calendar
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
NET_RETRIES     = int(env("NET_RETRIES", "3"))   # tentativas extras em timeout/rede
NET_BACKOFF     = int(env("NET_BACKOFF", "5"))   # segundos de pausa entre tentativas
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

# Auto-limite (poupa a licenca unica do Millennium):
#   ESTOQUE_MIN = idade minima do feed pra puxar estoque de novo (min).
#   PRECO_HORAS = de quanto em quanto tempo puxar preco (h); produto novo puxa na hora.
#   PULLER_FORCE = 1 forca pull completo agora (ignora o auto-limite).
ESTOQUE_MIN     = int(env("ESTOQUE_MIN", "15"))
PRECO_HORAS     = int(env("PRECO_HORAS", "24"))
PULLER_FORCE    = env("PULLER_FORCE", "0") not in ("", "0", "false", "False")
CONTROL_FILE    = env("CONTROL_FILE", "estoque_control.json")


def die(msg: str, code: int = 1):
    print(f"[bridge] ERRO: {msg}", flush=True)
    sys.exit(code)


def _epoch(s) -> float:
    """ISO 'YYYY-MM-DDTHH:MM:SSZ' -> epoch (0.0 se vazio/invalido)."""
    s = str(s or "").strip()
    if not s:
        return 0.0
    try:
        return float(calendar.timegm(time.strptime(s, "%Y-%m-%dT%H:%M:%SZ")))
    except Exception:
        return 0.0


def gist_ler_arquivos() -> dict:
    """Le os arquivos do gist (estoque.json anterior + controle). Best-effort:
    se faltar rede/credencial, devolve {} e o pull segue sem cache/gate."""
    if not GIST_ID or not GIST_TOKEN:
        return {}
    req = urllib.request.Request(
        f"https://api.github.com/gists/{GIST_ID}",
        headers={
            "Authorization": f"Bearer {GIST_TOKEN}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "WolfBridge-Puller",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            data = json.loads(r.read().decode("utf-8", "replace"))
    except Exception as e:
        print(f"[bridge] aviso: nao li o gist ({e}); sigo sem cache/auto-limite.", flush=True)
        return {}
    out = {}
    for nome, meta in (data.get("files") or {}).items():
        cont = (meta or {}).get("content")
        if not cont:
            continue
        try:
            out[nome] = json.loads(cont)
        except Exception:
            out[nome] = {}
    return out


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


def _paginar(metodo: str):
    """Gera todas as linhas de um metodo do Millennium, paginando por trans_id
    (full pull: comeca em 0). Retry curto para timeout/rede e licenca ocupada."""
    base = millennium_base()
    headers = {
        "Authorization": basic_auth_header(),
        "Accept": "application/json",
        "User-Agent": _UA_CHROME,
    }
    cursor = 0
    for page in range(1, MAX_PAGES + 1):
        url = f"{base}/{metodo}?vitrine={VITRINE}&trans_id={cursor}&$format=json"
        tent_rede = 0
        tent_lic = 0
        while True:
            status, data, raw = http_get_json(url, headers)
            if status == 0:  # timeout / erro de rede
                if tent_rede < NET_RETRIES:
                    tent_rede += 1
                    print(f"[bridge] timeout/rede ({metodo} pag {page}), "
                          f"tentativa {tent_rede}/{NET_RETRIES}, aguardando {NET_BACKOFF}s...", flush=True)
                    time.sleep(NET_BACKOFF)
                    continue
                die(f"falha de rede no Millennium ({metodo} pag {page}) apos {NET_RETRIES} tentativas: {raw}")
            if status in (429, 500, 503) and "licen" in raw.lower():
                if tent_lic < 5:
                    tent_lic += 1
                    print(f"[bridge] licenca ocupada (HTTP {status}) em {metodo}, "
                          f"tentativa {tent_lic}/5, aguardando 3s...", flush=True)
                    time.sleep(3)
                    continue
                die(f"licenca do Millennium ocupada ({metodo} pag {page}) apos varias tentativas.")
            if status == 401:
                # 401 = sessao/licenca ocupada ("retag"); credenciais validas.
                if tent_lic < 5:
                    tent_lic += 1
                    print(f"[bridge] sessao ocupada/retag (HTTP 401) em {metodo}, "
                          f"tentativa {tent_lic}/5, aguardando 5s...", flush=True)
                    time.sleep(5)
                    continue
                die(f"sessao/licenca do Millennium ocupada 401 ({metodo} pag {page}) apos varias tentativas.")
            if status < 200 or status >= 300:
                die(f"HTTP {status} do Millennium ({metodo} pag {page}): {raw[:300]}")
            break  # resposta OK

        rows = data.get("value") or []  # OData: itens vem em "value"
        if not rows:
            return
        max_trans = cursor
        for r in rows:
            try:
                t = int(r.get("trans_id") or 0)
            except (TypeError, ValueError):
                t = 0
            if t > max_trans:
                max_trans = t
            yield r
        if max_trans <= cursor:
            return
        cursor = max_trans


_INCLUIR_KEYS = ("incluir", "incluido", "flag_incluir", "integrar", "enviar")
# Radicais: o checkbox da guia SKU chama-se "Incluir"; o campo real quase sempre
# contém "inclu" (ou "integr"). Detecta variantes tipo b_incluir, st_incluir, etc.
_INCLUIR_STEMS = ("inclu", "integr")


def _incluir_key(s: dict):
    for key in _INCLUIR_KEYS:
        if key in s:
            return key
    for k in s.keys():
        kn = str(k).strip().lower()
        if any(st in kn for st in _INCLUIR_STEMS):
            return k
    return None


def _flag_true(v) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    if isinstance(v, (int, float)):
        return float(v) != 0.0
    s = str(v).strip().upper()
    return s in ("1", "T", "TRUE", "S", "SIM", "Y", "YES")


def sku_marcado_incluir(s: dict) -> bool:
    """Checkbox 'Incluir' da guia SKU da vitrine — o mesmo que a 4Middleware usava."""
    k = _incluir_key(s)
    return _flag_true(s.get(k)) if k is not None else False


def puxar_tudo(preco_cache: dict | None = None, forcar_preco: bool = False) -> tuple[list[dict], bool]:
    """Junta estoque + preco + nome por SKU, só os marcados 'Incluir' na vitrine.

    preco_cache: {sku: preco} do feed anterior — evita puxar preço toda rodada.
    forcar_preco: True puxa precodetabela agora (1x/dia, 'Puxar agora' ou SKU novo).
    Retorna (itens, preco_foi_puxado)."""
    preco_cache = preco_cache or {}
    catalogo: dict[str, dict] = {}
    n_vitrine = 0
    n_incluir = 0
    tem_campo = False
    chaves_amostra = None
    campo_nome = None
    amostra_obj = None

    # 0) Catálogo da vitrine: só entra SKU com checkbox Incluir (igual 4Middleware).
    for p in _paginar("produtos/listavitrine"):
        desc = str(p.get("descricao1") or p.get("descricao_original") or "").strip()
        skus = p.get("sku") if isinstance(p.get("sku"), list) else []
        for s in skus:
            if not isinstance(s, dict):
                continue
            n_vitrine += 1
            if chaves_amostra is None:
                chaves_amostra = sorted(str(k) for k in s.keys())
                amostra_obj = {str(k): s.get(k) for k in list(s.keys())[:40]}
            if _incluir_key(s) is not None:
                tem_campo = True
                if campo_nome is None:
                    campo_nome = _incluir_key(s)
            marcado = sku_marcado_incluir(s)
            if marcado:
                n_incluir += 1
            sku = str(s.get("sku") or "").strip()
            if not sku:
                continue
            cor = str(s.get("desc_cor") or "").strip()
            tam = str(s.get("desc_tamanho") or "").strip()
            extra = " ".join(x for x in (cor, tam) if x)
            nome = (desc + (" " + extra if extra else "")).strip()
            catalogo[sku] = {
                "nome": nome,
                "ean": str(s.get("barra") or "").strip(),
                "incluir": marcado,
            }

    print(
        f"[bridge] listavitrine: {n_vitrine} SKUs na vitrine, {n_incluir} com incluir. "
        f"campo_incluir={campo_nome or 'NAO'} chaves_sku={chaves_amostra} "
        f"amostra_sku={json.dumps(amostra_obj, ensure_ascii=False)[:800]}",
        flush=True,
    )
    if n_vitrine > 0 and not tem_campo:
        print(
            "[bridge] AVISO: nenhum SKU trouxe o campo 'incluir'. "
            "Filtro não aplicado nesta rodada — conferir chaves_sku acima.",
            flush=True,
        )
        permitidos = set(catalogo.keys())
    else:
        permitidos = {sku for sku, meta in catalogo.items() if meta.get("incluir")}

    itens: dict[str, dict] = {}

    # 1) estoque (+ ref/cod_produto + ean/barra) — só SKUs permitidos
    for r in _paginar("produtos/saldodeestoque"):
        sku = str(r.get("sku") or "").strip()
        if not sku or sku not in permitidos:
            continue
        try:
            saldo = float(r.get(SALDO_FIELD) or 0)
        except (TypeError, ValueError):
            saldo = 0.0
        it = itens.setdefault(sku, {"codigo": sku})
        it["estoque"] = saldo
        ref = str(r.get("cod_produto") or "").strip()
        ean = str(r.get("barra") or "").strip()
        if ref:
            it["ref"] = ref
        if ean:
            it["ean"] = ean

    # 2) preco (preco1 = "POR", base do markup do nosso lado).
    #    Preço quase não muda: só puxa se for forçado (1x/dia ou "Puxar agora")
    #    OU se algum SKU permitido não tem preço em cache (produto novo).
    precisa_preco = forcar_preco or any(sku not in preco_cache for sku in permitidos)
    preco_puxado = False
    if precisa_preco:
        for r in _paginar("produtos/precodetabela"):
            sku = str(r.get("sku") or "").strip()
            if not sku or sku not in itens:
                continue
            try:
                preco = float(r.get("preco1") or 0)
            except (TypeError, ValueError):
                preco = 0.0
            itens[sku]["preco"] = preco
        preco_puxado = True
        print(f"[bridge] preco: precodetabela puxado (forcar={forcar_preco}).", flush=True)
    else:
        print(f"[bridge] preco: reuso o cache do feed anterior ({len(preco_cache)} SKUs).", flush=True)

    # 3) nome/ean da vitrine + SKUs com incluir mas sem linha de saldo
    for sku in permitidos:
        meta = catalogo.get(sku) or {}
        it = itens.setdefault(sku, {"codigo": sku, "estoque": 0.0})
        if meta.get("nome"):
            it["nome"] = meta["nome"]
        if meta.get("ean") and not it.get("ean"):
            it["ean"] = meta["ean"]

    # Qualquer SKU ainda sem preço nesta rodada herda do cache (ou 0).
    for sku, it in itens.items():
        if "preco" not in it:
            it["preco"] = float(preco_cache.get(sku, 0.0))

    return list(itens.values()), preco_puxado


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


def gravar_no_gist(itens: list[dict], preco_atualizado_em: str = "") -> None:
    """Fluxo invertido: grava estoque.json num gist secreto via API do GitHub.
    O canal-ml (cron da HostGator) puxa esse gist por HTTPS e aplica o estoque."""
    if not GIST_ID:
        die("configure GIST_ID (id do gist secreto).")
    if not GIST_TOKEN:
        die("configure GIST_TOKEN (PAT com escopo gist).")

    feed = {
        "gerado_em": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "preco_atualizado_em": preco_atualizado_em,
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
        f"sink={SINK} dry_run={DRY_RUN} estoque_min={ESTOQUE_MIN} preco_horas={PRECO_HORAS}",
        flush=True,
    )

    # Estado anterior (gist): feed de estoque + recado de "Puxar agora".
    arqs = gist_ler_arquivos() if SINK != "push" else {}
    prev = arqs.get(GIST_FILE) or {}
    ctrl = arqs.get(CONTROL_FILE) or {}
    agora = time.time()
    prev_gerado = _epoch(prev.get("gerado_em"))
    force_req = _epoch(ctrl.get("force_req_em"))

    # Forçar: env PULLER_FORCE, ou "Puxar agora" (recado mais novo que o último feed).
    forcar = PULLER_FORCE or (force_req > 0 and force_req > prev_gerado)

    # Auto-limite do ESTOQUE: feed fresco e sem força -> não puxa (poupa a licença).
    if not forcar and prev_gerado > 0 and (agora - prev_gerado) < ESTOQUE_MIN * 60:
        idade = int((agora - prev_gerado) / 60)
        print(
            f"[bridge] feed fresco ({idade}min < {ESTOQUE_MIN}min) e sem 'Puxar agora'; "
            f"pulo o pull (poupa a licenca do Millennium).",
            flush=True,
        )
        return

    # PREÇO: 1x/dia (ou forçado). SKU novo sem cache também puxa (decidido em puxar_tudo).
    prev_preco = _epoch(prev.get("preco_atualizado_em"))
    preco_venceu = forcar or prev_preco <= 0 or (agora - prev_preco) >= PRECO_HORAS * 3600
    preco_cache = {
        str(it.get("codigo")): float(it.get("preco") or 0)
        for it in (prev.get("itens") or [])
        if isinstance(it, dict) and it.get("codigo") and it.get("preco") is not None
    }
    print(
        f"[bridge] modo: forcar={forcar} preco_venceu={preco_venceu} "
        f"cache_precos={len(preco_cache)} feed_idade_min="
        f"{int((agora - prev_gerado) / 60) if prev_gerado else 'n/a'}",
        flush=True,
    )

    itens, preco_puxado = puxar_tudo(preco_cache, forcar_preco=preco_venceu)
    print(f"[bridge] Millennium retornou {len(itens)} SKUs (estoque+preco+nome).", flush=True)
    if not itens:
        print("[bridge] nada para enviar (0 itens). Verifique vitrine/credenciais.", flush=True)
        return

    # preco_atualizado_em: agora se puxou preço; senão preserva o carimbo anterior.
    if preco_puxado:
        preco_ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    else:
        preco_ts = str(prev.get("preco_atualizado_em") or "")

    if SINK == "push":
        empurrar_para_canal(itens)
    else:
        gravar_no_gist(itens, preco_atualizado_em=preco_ts)


if __name__ == "__main__":
    main()
