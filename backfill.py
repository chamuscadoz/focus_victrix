"""
backfill_historico.py
Script de uso único: baixa o histórico completo da API Focus do BCB e popula
os parquets locais. Roda uma vez após a primeira instalação.
Após esse backfill, o fluxo semanal normal (focus_victrix.py) apenas adiciona
as linhas novas — ele NÃO precisa rebuscar o passado.
Escopo do backfill:
    - Anuais: IPCA, PIB Total, Câmbio (IndicadorDetalhe='Fim do ano'), Selic
      → anos 2026 e 2027 (conforme solicitado)
    - Selic COPOM: todas as reuniões disponíveis, filtradas depois por ano
Estratégia de paginação:
    A API não pagina. Usamos janelas de 2 anos de 'Data' + $top=10000 para
    buscar tudo sem estourar o limite.
Uso:
    python backfill_historico.py
    python backfill_historico.py --desde 2020-01-01     # limita profundidade
    python backfill_historico.py --anos 2026 2027 2028  # outros anos de referência
    python backfill_historico.py --dry-run              # só mostra o que faria
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date, timedelta
from typing import Iterable
from urllib.parse import quote

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backfill")

BASE_URL = "https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/"

# Configurações dos indicadores a baixar do endpoint Anuais
# (nome da API, IndicadorDetalhe se necessário, rótulo amigável)
INDICADORES_ANUAIS: list[tuple[str, str | None, str]] = [
    ("IPCA",      None,         "IPCA"),
    ("PIB Total", None,         "PIB Total"),
    ("Câmbio",    None,         "Câmbio"),
    ("Selic",     None,         "Selic"),
]


# =============================================================================
# Montagem de URL OData sem codificar os '$' dos nomes de parâmetro
# =============================================================================

def _build_url(endpoint: str, odata_params: dict) -> str:
    """
    Monta a URL OData mantendo os '$' literais nos nomes dos parâmetros.

    requests.get(params=...) codifica '$' como '%24', o que a API do BCB
    rejeita com 400. Aqui construímos a query string manualmente, codificando
    apenas os *valores* com urllib.parse.quote().
    """
    _safe = "'(),/"
    qs = "&".join(f"{k}={quote(str(v), safe=_safe)}" for k, v in odata_params.items())
    return BASE_URL + endpoint + "?" + qs


# =============================================================================
# Cliente HTTP enxuto com retry
# =============================================================================

def fetch(endpoint: str, odata_params: dict, tentativas: int = 4) -> list[dict]:
    url = _build_url(endpoint, odata_params)
    last_exc: Exception | None = None
    for t in range(1, tentativas + 1):
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            return r.json().get("value", [])
        except (requests.RequestException, ValueError) as e:
            last_exc = e
            espera = 1.5 ** t
            logger.warning("Falha %d/%d: %s — retry em %.1fs", t, tentativas, e, espera)
            time.sleep(espera)
    raise RuntimeError(f"BCB indisponível após {tentativas} tentativas: {last_exc}")


# =============================================================================
# Coleta em janelas (para não bater no limite de $top)
# =============================================================================

def janelas_de_data(desde: date, ate: date, tamanho_anos: int = 2) -> Iterable[tuple[date, date]]:
    """Gera janelas de ~2 anos cobrindo [desde, ate]."""
    ini = desde
    while ini < ate:
        fim = min(date(ini.year + tamanho_anos, ini.month, 1), ate)
        yield ini, fim
        ini = fim + timedelta(days=1)


def coletar_anuais(
    indicador_api: str,
    detalhe: str | None,
    anos_ref: list[int],
    desde: date,
    ate: date,
) -> list[dict]:
    """Coleta histórico completo do endpoint Anuais para um indicador."""
    resultados: list[dict] = []
    filtros_base = [
        f"Indicador eq '{indicador_api}'",
        "baseCalculo eq 0",
    ]
    if detalhe is not None:
        filtros_base.append(f"IndicadorDetalhe eq '{detalhe}'")

    for ano in anos_ref:
        filtros_ano = filtros_base + [f"DataReferencia eq '{ano}'"]
        total_ano = 0
        for janela_ini, janela_fim in janelas_de_data(desde, ate):
            filtros = filtros_ano + [
                f"Data ge '{janela_ini.isoformat()}'",
                f"Data le '{janela_fim.isoformat()}'",
            ]
            odata_params = {
                "$filter": " and ".join(filtros),
                "$select": "Data,DataReferencia,Mediana,Media,DesvioPadrao,Minimo,Maximo,numeroRespondentes",
                "$format": "json",
                "$top": "10000",
                "$orderby": "Data asc",
            }
            rows = fetch("ExpectativasMercadoAnuais", odata_params)
            total_ano += len(rows)
            for r in rows:
                try:
                    resultados.append({
                        "data":     date.fromisoformat(r["Data"][:10]),
                        "indicador": indicador_api,
                        "detalhe":  detalhe,
                        "ano_ref":  int(r["DataReferencia"]),
                        "mediana":  float(r["Mediana"])          if r.get("Mediana")             is not None else None,
                        "media":    float(r["Media"])            if r.get("Media")               is not None else None,
                        "desvio":   float(r["DesvioPadrao"])     if r.get("DesvioPadrao")         is not None else None,
                        "minimo":   float(r["Minimo"])           if r.get("Minimo")               is not None else None,
                        "maximo":   float(r["Maximo"])           if r.get("Maximo")               is not None else None,
                        "n_resp":   int(r["numeroRespondentes"]) if r.get("numeroRespondentes")   is not None else None,
                    })
                except (KeyError, TypeError, ValueError) as e:
                    logger.debug("Linha descartada: %s (%s)", r, e)
        logger.info("  %s %d → %d linhas", indicador_api, ano, total_ano)
    return resultados


def coletar_selic_copom(desde: date, ate: date) -> list[dict]:
    """
    Coleta trajetória Selic reunião-a-reunião.
    Não filtra por ano no servidor (endpoint não tem DataReferencia).
    """
    resultados: list[dict] = []
    for janela_ini, janela_fim in janelas_de_data(desde, ate):
        odata_params = {
            "$filter": f"Data ge '{janela_ini.isoformat()}' and Data le '{janela_fim.isoformat()}'",
            "$select": "Data,Reuniao,Mediana,Media,DesvioPadrao,Minimo,Maximo,numeroRespondentes",
            "$format": "json",
            "$top": "10000",
            "$orderby": "Data asc",
        }
        rows = fetch("ExpectativasMercadoSelic", odata_params)
        for r in rows:
            try:
                resultados.append({
                    "data":    date.fromisoformat(r["Data"][:10]),
                    "reuniao": r["Reuniao"],
                    "mediana": float(r["Mediana"])          if r.get("Mediana")           is not None else None,
                    "media":   float(r["Media"])            if r.get("Media")             is not None else None,
                    "desvio":  float(r["DesvioPadrao"])     if r.get("DesvioPadrao")       is not None else None,
                    "minimo":  float(r["Minimo"])           if r.get("Minimo")             is not None else None,
                    "maximo":  float(r["Maximo"])           if r.get("Maximo")             is not None else None,
                    "n_resp":  int(r["numeroRespondentes"]) if r.get("numeroRespondentes") is not None else None,
                })
            except (KeyError, TypeError, ValueError) as e:
                logger.debug("Linha Selic descartada: %s (%s)", r, e)
    logger.info("  Selic COPOM (todas as reuniões) → %d linhas", len(resultados))
    return resultados


# =============================================================================
# Deduplicação por semana (guarda 1 linha por semana: a do 1º dia útil)
# =============================================================================

def dedup_por_semana(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    """
    O Focus é publicado no 1º dia útil da semana (normalmente segunda).
    Mas a API registra revisões intraday e dias posteriores.
    Aqui mantemos APENAS a primeira publicação de cada semana ISO — que é
    o dado oficial que aparece no site do BCB.
    """
    if df.empty:
        return df
    df = df.copy()
    df["_ano_sem"] = (
        pd.to_datetime(df["data"]).dt.isocalendar().year.astype(str)
        + "-"
        + pd.to_datetime(df["data"]).dt.isocalendar().week.astype(str).str.zfill(2)
    )
    chave_dedup = group_cols + ["_ano_sem"]
    df = df.sort_values("data").drop_duplicates(subset=chave_dedup, keep="first")
    df = df.drop(columns=["_ano_sem"])
    return df.reset_index(drop=True)


# =============================================================================
# Salvamento
# =============================================================================

def salvar(df: pd.DataFrame, path: str, label: str) -> None:
    if df.empty:
        logger.warning("%s: DataFrame vazio, nada a salvar em %s", label, path)
        return
    import os
    base = os.path.splitext(path)[0]
    parquet_path = base + ".parquet"
    csv_path = base + ".csv"
    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False)
    logger.info(
        "✓ %s salvo: %s + .csv (%d linhas, %d datas únicas)",
        label, parquet_path, len(df), df["data"].nunique(),
    )


# =============================================================================
# Main
# =============================================================================

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--desde", default="2001-01-01",
                        help="Data inicial (default: 2001-01-01, início das séries)")
    parser.add_argument("--ate", default=None,
                        help="Data final (default: hoje)")
    parser.add_argument("--anos", nargs="+", type=int, default=[2026, 2027],
                        help="Anos de referência a baixar (default: 2026 2027)")
    parser.add_argument("--saida-anuais", default="data/focus_anuais.parquet")
    parser.add_argument("--saida-selic",  default="data/focus_selic.parquet")
    parser.add_argument("--sem-dedup", action="store_true",
                        help="Mantém todas as publicações (não deduplica por semana)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Só mostra o que faria")
    args = parser.parse_args()

    desde = date.fromisoformat(args.desde)
    ate   = date.fromisoformat(args.ate) if args.ate else date.today()

    logger.info("=== BACKFILL HISTÓRICO FOCUS BCB ===")
    logger.info("Período: %s → %s", desde, ate)
    logger.info("Anos de referência: %s", args.anos)
    logger.info("Dedup por semana: %s", not args.sem_dedup)

    if args.dry_run:
        logger.info("--dry-run: abortando antes das chamadas.")
        return 0

    # -------- ANUAIS --------
    logger.info("\n[1/2] Coletando endpoint Anuais...")
    linhas_anuais: list[dict] = []
    for indicador_api, detalhe, label in INDICADORES_ANUAIS:
        logger.info(" → %s", label)
        linhas_anuais.extend(
            coletar_anuais(indicador_api, detalhe, args.anos, desde, ate)
        )

    df_anuais = pd.DataFrame(linhas_anuais)
    logger.info("Total bruto Anuais: %d linhas", len(df_anuais))

    if not args.sem_dedup and not df_anuais.empty:
        antes = len(df_anuais)
        df_anuais = dedup_por_semana(df_anuais, ["indicador", "detalhe", "ano_ref"])
        logger.info("Após dedup por semana: %d linhas (de %d)", len(df_anuais), antes)

    salvar(df_anuais, args.saida_anuais, "Anuais")

    # -------- SELIC COPOM --------
    logger.info("\n[2/2] Coletando endpoint Selic COPOM...")
    linhas_selic = coletar_selic_copom(desde, ate)
    df_selic = pd.DataFrame(linhas_selic)
    logger.info("Total bruto Selic: %d linhas", len(df_selic))

    if not args.sem_dedup and not df_selic.empty:
        antes = len(df_selic)
        df_selic = dedup_por_semana(df_selic, ["reuniao"])
        logger.info("Após dedup por semana: %d linhas (de %d)", len(df_selic), antes)

    salvar(df_selic, args.saida_selic, "Selic")

    # -------- RESUMO --------
    logger.info("\n=== CONCLUÍDO ===")
    if not df_anuais.empty:
        logger.info("Anuais:")
        for (ind, det), grupo in df_anuais.groupby(["indicador", "detalhe"], dropna=False):
            por_ano = grupo.groupby("ano_ref").size().to_dict()
            label = f"{ind}" + (f" ({det})" if pd.notna(det) and det else "")
            logger.info("  %s: %s", label, por_ano)
    if not df_selic.empty:
        reunioes = sorted(df_selic["reuniao"].unique())
        logger.info("Selic: %d reuniões distintas (%s ... %s)",
                    len(reunioes), reunioes[0], reunioes[-1])

    return 0


if __name__ == "__main__":
    sys.exit(main())