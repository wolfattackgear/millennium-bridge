# millennium-bridge (Railway) — puxador de estoque Millennium → canal-ml

Ponte que roda no **Railway** (a HostGator bloqueia a saída na porta 6017 do
Millennium; o Railway alcança). É um **cron job stateless**: a cada X minutos
puxa o estoque do Millennium e empurra pro canal-ml pelos endpoints que já
existem (`api/erp-estoque.php`), e encerra. Sem estado, sem volume, sem chamadas
concorrentes — cabe no plano grátis.

## Como funciona
1. `GET {MILLENNIUM_URL}/produtos/saldodeestoque?vitrine=..&trans_id=0&$format=json` (Basic auth), paginando por `trans_id`.
2. Monta `[{codigo: sku, estoque: <SALDO_FIELD>}]`.
3. `POST {CANAL_BASE}/api/erp-estoque.php` em lotes com `{ token, itens }` (header `X-ERP-Token`).

O canal-ml faz upsert por SKU (idempotente), então rodar de novo não duplica.

## Variáveis de ambiente (aba **Variables** no Railway)
| Var | Ex. | Obrig. |
|---|---|---|
| `MILLENNIUM_URL` | `http://rotaextrema.millenniumhosting.com.br:6017/api/millenium_eco` | sim |
| `MILLENNIUM_USER` | `integracao` | sim |
| `MILLENNIUM_PASS` | *(senha)* | sim |
| `VITRINE` | `301` | sim |
| `CANAL_BASE` | `https://sistema.wolfattack.com.br/projeto/canal-ml` | sim |
| `ERP_PUSH_TOKEN` | *(mesmo valor do canal-ml)* | sim |
| `SALDO_FIELD` | `saldo` ou `saldo_vitrine_sem_reserva` | não (def. `saldo`) |
| `DRY_RUN` | `1` no 1º teste, depois `0` | não |
| `BATCH_SIZE` | `300` | não |

## Deploy no Railway (passo a passo)
1. Suba esta pasta num repositório GitHub (pode ser um repo novo só com estes
   arquivos, ou deixe dentro do canal-ml em `tools/millennium-bridge` e, no
   Railway, defina **Settings → Root Directory = tools/millennium-bridge**).
2. Railway → **New Project → Deploy from GitHub repo** → escolha o repo.
3. Em **Variables**, cole as variáveis acima (use o `.env.example` como base).
   A senha e o token ficam SÓ aqui, nunca no código.
4. Em **Settings → Deploy**:
   - **Start Command**: `python puller.py` (o `railway.json` já define isso).
   - **Cron Schedule**: `*/15 * * * *` (a cada 15 min). Com cron, o serviço roda
     e encerra — não fica ligado 24/7 (economiza o crédito do plano grátis).
5. **Primeiro teste com `DRY_RUN=1`**: dispare um run manual (Deployments →
   Redeploy/Run) e veja os **Logs** — deve listar quantos SKUs leu e uma amostra,
   sem empurrar nada.
6. Deu certo? Troque `DRY_RUN=0` e rode de novo — agora empurra pro canal-ml.
   Confira no canal-ml (logs/estoque) que os SKUs chegaram.

## Decisões que o teste fecha
- **Vitrine**: confirme qual vitrine é a do canal ML (no piloto, 203 tinha ~930
  itens; 301 tinha poucos). Ajuste `VITRINE`.
- **Campo de saldo**: `saldo` (o que o piloto usou) vs `saldo_vitrine_sem_reserva`
  (real sem reserva). Rode com `DRY_RUN=1` e compare — ajuste `SALDO_FIELD`.

## Evoluções (depois do piloto)
- Preço e catálogo: mesmos endpoints (`api/erp-preco.php`, `api/erp-catalogo.php`)
  — dá pra estender este script com `precodetabela` e `listavitrine`.
- CDC incremental (guardar `trans_id` num volume) só vale se o catálogo crescer
  muito; para ~1k SKUs, o full pull a cada 15 min é suficiente e mais simples.
