# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the script

```bash
# Activate the virtual environment first
source venv/bin/activate

# Run the script (requires GMAIL_APP_PASSWORD env var to actually send email)
GMAIL_APP_PASSWORD=<secret> python focus_victrix.py

# To test image generation without sending email, comment out envia_email() in main()
python focus_victrix.py
```

## Architecture

The entire project is a single file: `focus_victrix.py`. It runs weekly via GitHub Actions (every Monday at 10:30 BRT) and can also be triggered manually via `workflow_dispatch`.

**Data flow:**
1. Fetch current-week Focus data (3 dates: today, 1 week ago, 4 weeks ago) from BCB Olinda OData API
2. Fetch IPCA historical series for 2026 and 2027 (last 52 weeks) from the same API
3. Render a styled table image (`gera_imagem`) using matplotlib with custom drawing (no standard chart types — everything is `ax.text`, `patches.Rectangle`, `ax.plot`)
4. Render a line chart (`gera_grafico_ipca`) with the two IPCA series
5. Embed both images inline in an HTML email and send via Gmail SMTP SSL

**BCB API endpoints used:**
- `ExpectativasMercadoAnuais` — IPCA, PIB Total, Câmbio (filter by `Indicador`, `Data`, `DataReferencia`, `baseCalculo eq 0`)
- `ExpectativasMercadoSelic` — Selic (filter by `Data` and `Reuniao`, e.g. `R8/2026`)

**Key detail — `DataReferencia` vs `Data`:** In `ExpectativasMercadoAnuais`, `Data` is the survey/publication date (typically Monday, but varies with holidays), while `DataReferencia` is the forecast target year (string `'2026'`). The table query filters by `Data`; the historical chart query filters by `DataReferencia`.

**Publication date discovery:** `ultima_publicacao(n)` queries the API directly to find the n-th most recent publication date instead of assuming a fixed weekday. This handles holiday weeks where BCB may publish on a different day. Falls back to Monday-based calculation if the API call fails.

**Email:** Sent from `z.cassiolato@gmail.com` using a Gmail App Password stored as the `GMAIL_APP_PASSWORD` GitHub Actions secret. The `To` field is set to the sender; all recipients are in `BCC_DESTINOS`.

## Visual style

The Victrix Capital color palette is defined as module-level constants (`BG`, `LIME`, `MID_GRN`, `SAGE`, `WHITE`, `ALT_ROW`). The table image uses a fully custom renderer (not standard matplotlib charts). The chart uses `facecolor=BG` throughout for consistency.
