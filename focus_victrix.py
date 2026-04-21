"""
focus_victrix.py
Orquestrador principal: ingere dados, monta tabela/gráfico, envia email.

Uso:
    python focus_victrix.py                 # roda tudo (ingest + render + email)
    python focus_victrix.py --skip-email    # gera PNGs mas não envia
    python focus_victrix.py --skip-ingest   # usa apenas base local (offline)
    python focus_victrix.py --dry-run       # loga o que faria sem rodar email
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta

import holidays

from src import ingest, mailer, query, render, storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("focus_victrix")


def e_dia_util_com_publicacao() -> bool:
    hoje = datetime.today().date()
    br_holidays = holidays.Brazil(years=hoje.year)
    if hoje in br_holidays or hoje.weekday() >= 5:
        logger.info("Hoje (%s) é feriado ou fim de semana. Abortando.", hoje)
        return False
    if not ingest.existe_publicacao_recente(dias=7):
        logger.info("Nenhuma publicação recente encontrada. Abortando.")
        return False
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-ingest", action="store_true",
                        help="Não busca novos dados da API; usa base local.")
    parser.add_argument("--skip-email", action="store_true",
                        help="Gera PNGs mas não envia email.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Alias para --skip-email.")
    parser.add_argument("--force", action="store_true",
                        help="Ignora checagem de dia útil/publicação.")
    args = parser.parse_args()

    if not args.force and not args.skip_ingest:
        if not e_dia_util_com_publicacao():
            return 0

    # --- INGEST
    if not args.skip_ingest:
        logger.info("Ingerindo dados do BCB...")
        stats = ingest.ingerir_semana()
        logger.info("Ingestão concluída: %s", stats)
    else:
        logger.info("Ingestão pulada (--skip-ingest).")

    # --- CARREGA BASE
    df_anuais = storage.load_anuais()
    df_selic = storage.load_selic()
    logger.info("Base local: %d linhas anuais | %d linhas selic",
                len(df_anuais), len(df_selic))
    if df_anuais.empty:
        logger.error("Base anual vazia. Rode com --force ou verifique a API.")
        return 1

    # --- QUERY
    datas = query.tres_datas(df_anuais)
    logger.info("Datas: hoje=%s | -1sem=%s | -4sem=%s | -8sem=%s",
                datas.hoje, datas.uma_semana, datas.quatro_semanas, datas.oito_semanas)

    ano_tabela = (datas.hoje or datetime.today().date()).year
    linhas = query.montar_linhas(df_anuais, df_selic, datas, ano_ref=ano_tabela)

    # Converte para o formato do render
    rows_data = []
    for linha in linhas:
        seta = render.seta(linha.hoje, linha.v1)
        comp = f"{seta} {linha.streak}s" if seta in ("▲", "▼") and linha.streak > 0 else seta
        rows_data.append({
            "label": linha.label,
            "v8": render.fmt(linha.v8),
            "v4": render.fmt(linha.v4),
            "v1": render.fmt(linha.v1),
            "hoje": render.fmt(linha.hoje),
            "comp": comp,
        })

    # --- HISTÓRICO PARA OS GRÁFICOS
    hist_ipca_atual = storage.historico_anual(df_anuais, "IPCA", ano_tabela, semanas=52)
    hist_ipca_prox = storage.historico_anual(df_anuais, "IPCA", ano_tabela + 1, semanas=52)
    hist_selic_atual = storage.historico_anual(df_anuais, "Selic", ano_tabela, semanas=52)
    hist_selic_prox = storage.historico_anual(df_anuais, "Selic", ano_tabela + 1, semanas=52)
    logger.info("Histórico IPCA: %d pontos (%d) | %d pontos (%d)",
                len(hist_ipca_atual), ano_tabela, len(hist_ipca_prox), ano_tabela + 1)
    logger.info("Histórico Selic: %d pontos (%d) | %d pontos (%d)",
                len(hist_selic_atual), ano_tabela, len(hist_selic_prox), ano_tabela + 1)

    # --- RENDER
    logger.info("Gerando tabela...")
    img_bytes = render.gera_imagem(rows_data, ano_tabela=ano_tabela)
    with open("focus_victrix_output.png", "wb") as f:
        f.write(img_bytes)

    logger.info("Gerando gráfico IPCA...")
    graf_ipca_bytes = render.gera_grafico_ipca(hist_ipca_atual, hist_ipca_prox)
    with open("focus_victrix_grafico.png", "wb") as f:
        f.write(graf_ipca_bytes)

    logger.info("Gerando gráfico Selic...")
    graf_selic_bytes = render.gera_grafico_selic(hist_selic_atual, hist_selic_prox)
    with open("focus_victrix_selic.png", "wb") as f:
        f.write(graf_selic_bytes)

    # --- EMAIL
    if args.skip_email or args.dry_run:
        logger.info("Email pulado (skip-email/dry-run).")
        return 0

    logger.info("Enviando email...")
    mailer.envia_email(img_bytes, graf_ipca_bytes, graf_selic_bytes)
    logger.info("Concluído.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
