"""
ingest.py
Baixa dados novos do BCB e faz upsert na base Parquet.

Estratégia:
    - Descobre as datas de publicação mais recentes pela API (via IPCA, que sai sempre).
    - Para cada data nova (não presente ainda no parquet OU com menos indicadores do que o esperado),
      busca IPCA, PIB Total, Câmbio e Selic (última reunião do ano corrente e +1).
    - Guarda tudo indexado por (data, indicador, ano_ref) / (data, reuniao).
    - Para o gráfico, também faz um "catch up" do histórico anual (52 semanas) nos dois anos de interesse.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

import pandas as pd

from . import bcb_api, storage

logger = logging.getLogger(__name__)

INDICADORES_ANUAIS = ["IPCA", "PIB Total", "Câmbio", "Selic"]


def ingerir_semana(
    anos_ref: tuple[int, int] | None = None,
    historico_semanas: int = 60,
) -> dict:
    """
    Faz ingestão incremental.
    - `anos_ref`: os dois anos alvo da tabela/gráfico. Default = (ano_corrente, ano_corrente+1).
    - `historico_semanas`: semanas de histórico no primeiro run (quando CSV está vazio).

    Retorna um dict com estatísticas da ingestão.
    """
    hoje = datetime.today().date()
    if anos_ref is None:
        anos_ref = (hoje.year, hoje.year + 1)

    # Descobre a data mais recente já salva para definir início incremental
    df_atual = storage.load_anuais()
    if not df_atual.empty:
        ultima_data = max(df_atual["data"])
        data_inicio_hist = ultima_data + timedelta(days=1)
        logger.info("Ingestão incremental a partir de %s", data_inicio_hist)
    else:
        data_inicio_hist = hoje - timedelta(weeks=historico_semanas)
        logger.info("Primeiro run — buscando %d semanas desde %s",
                    historico_semanas, data_inicio_hist)

    stats = {
        "datas_novas": 0,
        "linhas_anuais_upserted": 0,
        "linhas_selic_upserted": 0,
        "anos_ref": anos_ref,
    }

    # ---- (1) Histórico anual incremental
    novas_anuais: list[dict] = []
    for indicador in INDICADORES_ANUAIS:
        for ano in anos_ref:
            logger.info("Buscando %s / %d desde %s", indicador, ano, data_inicio_hist)
            serie = bcb_api.historico_anuais(indicador, ano, data_inicio_hist)
            for ponto in serie:
                novas_anuais.append({
                    "data": ponto["data"],
                    "indicador": indicador,
                    "ano_ref": ano,
                    "mediana": ponto["mediana"],
                })
    if novas_anuais:
        df_anuais = pd.DataFrame(novas_anuais)
        stats["linhas_anuais_upserted"] = storage.upsert_anuais(df_anuais)
        logger.info("Upsert anuais: %d linhas", stats["linhas_anuais_upserted"])

    # ---- (2) Selic incremental: apenas datas de publicação mais recentes que o CSV
    df_selic_atual = storage.load_selic()
    if not df_selic_atual.empty:
        ultima_selic = max(df_selic_atual["data"])
        datas_pub = [d for d in bcb_api.datas_publicacao_recentes(indicador="IPCA", n=12)
                     if d > ultima_selic]
        logger.info("Selic: %d novas datas de publicação após %s", len(datas_pub), ultima_selic)
    else:
        datas_pub = bcb_api.datas_publicacao_recentes(indicador="IPCA", n=12)
    stats["datas_novas"] = len(datas_pub)

    novas_selic: list[dict] = []
    for data_pub in datas_pub:
        for ano in anos_ref:
            res = bcb_api.selic_ultima_reuniao_do_ano(data_pub, ano)
            if res is None:
                continue
            reuniao, mediana = res
            novas_selic.append({
                "data": data_pub,
                "reuniao": reuniao,
                "mediana": mediana,
            })
    if novas_selic:
        df_selic = pd.DataFrame(novas_selic)
        stats["linhas_selic_upserted"] = storage.upsert_selic(df_selic)
        logger.info("Upsert selic: %d linhas", stats["linhas_selic_upserted"])

    return stats


def existe_publicacao_recente(dias: int = 7) -> bool:
    """Checa se há publicação do Focus nos últimos `dias` dias úteis, via API."""
    try:
        datas = bcb_api.datas_publicacao_recentes(indicador="IPCA", n=5)
    except RuntimeError:
        return False
    if not datas:
        return False
    mais_recente = max(datas)
    return (datetime.today().date() - mais_recente).days <= dias
