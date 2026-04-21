# Focus Victrix — Automação Semanal do Relatório Focus (BCB)

Projeto da **Victrix Capital** para geração e envio automático da tabela semanal de medianas do Relatório Focus do Banco Central do Brasil.

---

## Novidades desta versão (v2)

- ✅ **Bug do "há 4 semanas" corrigido.** A versão anterior pegava a 5ª data distinta mais recente da API, o que retornava publicações de poucos dias atrás. Agora calcula a data-alvo real (hoje − 28 dias) e busca a publicação mais próxima.
- ✅ **Base local em Parquet.** Todos os dados baixados da API são persistidos em `data/*.parquet` e commitados automaticamente no repo pelo GitHub Actions. Queries "há N semanas" agora são puras funções sobre a base local.
- ✅ **Código modular.** O monolito `focus_victrix.py` foi dividido em `src/` (storage, bcb_api, ingest, query, render, mailer).
- ✅ **Testes unitários.** `pytest tests/` — inclui teste de regressão que reproduz o bug antigo.
- ✅ **Keep-alive workflow** para evitar desabilitação automática do cron após 60 dias.
- ✅ **Retry com backoff** nas chamadas à API do BCB.
- ✅ **Descoberta dinâmica da última reunião Selic do ano** (não mais hard-coded `R8`).
- ✅ **Notificação de falha** abrindo issue automática.

---

## O que faz

Toda **segunda-feira às 10h37 BRT**, o script:

1. Ingere dados novos do Focus (via API Olinda/BCB) e faz upsert no Parquet local
2. Consulta a base local para montar a tabela (valores de hoje, −1 semana, −4 semanas) e o histórico IPCA de 52 semanas
3. Renderiza tabela e gráfico no visual Victrix Capital (PNG transparente)
4. Envia por email via Gmail SMTP
5. Commita a base Parquet atualizada de volta no repo

---

## Estrutura do projeto

```
focus_victrix/
├── focus_victrix.py              ← orquestrador (~120 linhas)
├── src/
│   ├── storage.py                ← camada Parquet (upsert + queries)
│   ├── bcb_api.py                ← cliente Olinda com retry
│   ├── ingest.py                 ← API → Parquet
│   ├── query.py                  ← "hoje / −1 sem / −4 sem" correto
│   ├── render.py                 ← tabela e gráfico
│   └── mailer.py                 ← envio Gmail SMTP
├── tests/
│   └── test_basics.py            ← 8 testes, inclui regressão do bug
├── data/
│   ├── focus_anuais.parquet      ← histórico IPCA/PIB/Câmbio
│   └── focus_selic.parquet       ← histórico Selic por reunião
├── assets/
│   ├── blur/blur_65.png
│   ├── logo/png01.png
│   └── font/static/ZalandoSansSemiExpanded-*.ttf
├── requirements.txt
├── .gitignore
└── .github/workflows/
    ├── focus_semanal.yml         ← cron principal, 37 13 * * 1
    └── keepalive.yml             ← ping quinzenal
```

---

## Diagnóstico dos dois problemas anteriores

### 1) GitHub Actions não disparava no horário

Era uma combinação de quatro causas possíveis:

| Causa | Solução aplicada |
|---|---|
| Cron em horário "redondo" (13:30 UTC) cai em pico de fila do GitHub — atrasa ou pula | Movido para `37 13 * * 1` (horário quebrado) |
| Repos sem atividade por 60 dias têm workflows desabilitados | Novo `keepalive.yml` faz ping quinzenal |
| Falhas silenciosas sem notificação | Step `if: failure()` abre issue automática |
| App Password expirada/revogada | Ver abaixo, seção `Configuração` |

> **Importante:** você **não precisa estar logado** para GitHub Actions rodar. Ele roda na infra do GitHub 24/7. Se tinha impressão disso, era porque o job falhava silenciosamente.

### 2) Dados errados na tabela

O código antigo fazia:

```python
sf4 = ultima_publicacao(4)  # "5ª data distinta mais recente"
```

Com a API publicando múltiplas vezes por semana (revisões, dias úteis), a 5ª data distinta podia ser de apenas ~1 semana atrás, não 4. Isso fazia a coluna "Há 4 semanas" mostrar valores quase iguais aos de hoje.

**Correção (`src/query.py`):**

```python
# Calcula a data-alvo real e busca a publicação mais próxima ≤ alvo
quatro = storage.data_mais_proxima(df, hoje - timedelta(days=28), tolerancia_dias=7)
```

Garante também que as 3 datas retornadas (hoje, −1 sem, −4 sem) sejam **estritamente distintas e ordenadas**. O teste `test_data_mais_proxima_quatro_semanas_real` reproduz o cenário problemático e garante que não volta.

---

## Configuração

### 1. Clonar e instalar

```bash
git clone https://github.com/chamuscadoz/focus_victrix
cd focus_victrix
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Regenerar e configurar a App Password do Gmail

O erro `SMTPAuthenticationError 535` indica App Password inválida. Regenere:

1. Acesse `myaccount.google.com/security` → confirme **2FA ativo** em `z.cassiolato@gmail.com`
2. `myaccount.google.com/apppasswords` → criar nova com nome "Focus Victrix"
3. Copie a senha de 16 caracteres (ignore os espaços — o código remove automaticamente)

**Local:**
```bash
echo 'export GMAIL_APP_PASSWORD="abcd efgh ijkl mnop"' >> ~/.bashrc
source ~/.bashrc
```

**GitHub Actions:**
`Settings → Secrets and variables → Actions → New repository secret`
- Name: `GMAIL_APP_PASSWORD`
- Value: cole a senha (com ou sem espaços — tanto faz)

### 3. Assets

Certifique-se de que `assets/blur/blur_65.png`, `assets/logo/png01.png` e as fontes Zalando em `assets/font/static/` estão no repo. O código tem fallback para DejaVu Sans se a fonte não for encontrada, mas o visual fica diferente.

---

## Como rodar

```bash
# Tudo (ingest + render + email)
python focus_victrix.py

# Só gera PNGs locais (sem email, útil pra testar visual)
python focus_victrix.py --skip-email

# Usa só a base local, sem chamar a API (offline)
python focus_victrix.py --skip-ingest --skip-email

# Ignora checagem de dia útil/feriado
python focus_victrix.py --force
```

Rodar os testes:
```bash
pytest tests/ -v
```

---

## Migração da versão anterior (v1 → v2)

Se você tinha o `focus_victrix.py` monolítico rodando:

1. **Backup:** `cp focus_victrix.py focus_victrix_v1.py.bak`
2. **Substitua os arquivos** pelos desta versão
3. **Instale dependências novas:** `pip install -r requirements.txt` (adiciona pandas, pyarrow, pytest)
4. **Primeira ingestão:** `python focus_victrix.py --skip-email --force` — vai criar `data/focus_anuais.parquet` e `data/focus_selic.parquet` já populados com ~52 semanas de histórico
5. **Commit a base inicial:** `git add data/ && git commit -m "feat: base inicial"`
6. **Atualize o workflow:** copie `.github/workflows/focus_semanal.yml` e `.github/workflows/keepalive.yml`
7. **Regenere a App Password** (seção 2 acima)
8. **Teste manualmente** no GitHub: `Actions → Focus Victrix Semanal → Run workflow`

---

## Troubleshooting

| Erro | Causa | Solução |
|---|---|---|
| `SMTPAuthenticationError 535` | App Password inválida/revogada | Regerar, atualizar secret |
| `RuntimeError: Falha ao consultar BCB` | API Olinda fora do ar ou rate-limit | Retry automático cobre 95%; se persistir, rodar de novo em 1h |
| "Data" vazia ou inconsistente | Base Parquet corrompida | Apagar `data/*.parquet` e rodar com `--force` |
| `FileNotFoundError` das fontes | Zalando Sans não está em `assets/font/` | Copiar os `.ttf` ou aceitar o fallback DejaVu Sans |
| Workflow não dispara | Repo sem atividade há 60 dias (desabilitado pelo GitHub) | O `keepalive.yml` previne isso; se já aconteceu, reabilite em Actions → ⋯ → Enable |

---

## Contato

**Victrix Capital**
`contato@victrixcapital.com.br`
`victrixcapital.com.br`
