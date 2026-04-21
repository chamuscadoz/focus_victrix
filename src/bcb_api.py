"""
bcb_api.py
Cliente fino da API Olinda/Expectativas do BCB.

Responsabilidades:
    - Montar URLs OData corretamente (escapando aspas).
    - Retry com backoff para falhas transitórias.
    - Retornar estruturas simples (listas de dicts nativos), sem lógica de negócio.
"""

from __future__ import annotations

import logging
import time
from datetime import date
from typing import Optional
from urllib.parse import urlencode

import requests

logger = logging.getLogger(__name__)

BASE_URL = (
    "https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/"
)

DEFAULT_TIMEOUT = 20
MAX_RETRIES = 3
BACKOFF_BASE = 1.5  # segundos


def _build_url(endpoint: str, odata_params: dict) -> str:
    """
    Constrói a URL OData sem codificar os '$' dos nomes de parâmetro.

    requests.get(params=...) codifica '$' como '%24', o que a API do BCB
    rejeita com 400. A solução é montar a query string manualmente, mantendo
    os '$' literais nos nomes, mas ainda codificando os *valores* normalmente.

    Ex.:  ?$filter=Indicador eq 'IPCA'&$top=10&$format=json
    """
    # urlencode codifica apenas os valores; os nomes já têm '$' e ficam intactos
    # porque não há caracteres problemáticos nos próprios nomes OData.
    qs = "&".join(f"{k}={_quote_value(v)}" for k, v in odata_params.items())
    return BASE_URL + endpoint + "?" + qs


def _quote_value(v: str) -> str:
    """Codifica o valor de um parâmetro OData preservando chars especiais da sintaxe."""
    # Codifica tudo exceto os chars que a API OData espera literais no valor.
    # Usamos quote() do urllib com safe=' ' vazio para codificar espaços como %20
    # (a API do BCB rejeita '+' como substituto de espaço).
    from urllib.parse import quote
    return quote(str(v), safe="'(),/")


def _get(url: str, timeout: int = DEFAULT_TIMEOUT) -> list[dict]:
    """GET com retry exponencial. Recebe URL já montada. Retorna array 'value' do JSON OData."""
    last_exc: Optional[Exception] = None
    for tentativa in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            return r.json().get("value", [])
        except (requests.RequestException, ValueError) as e:
            last_exc = e
            espera = BACKOFF_BASE ** tentativa
            logger.warning(
                "Falha BCB (tentativa %d/%d): %s — retry em %.1fs",
                tentativa, MAX_RETRIES, e, espera,
            )
            time.sleep(espera)
    logger.error("Todas as %d tentativas falharam: %s", MAX_RETRIES, last_exc)
    raise RuntimeError(f"Falha ao consultar BCB: {last_exc}") from last_exc


def _odata_quote(value: str) -> str:
    """Escapa aspas simples para OData ('' em vez de \\')."""
    return value.replace("'", "''")


# -----------------------------------------------------------------------------
# Expectativas anuais (IPCA, PIB Total, Câmbio)
# -----------------------------------------------------------------------------

def datas_publicacao_recentes(indicador: str = "IPCA", n: int = 60) -> list[date]:
    """Retorna as últimas `n` datas de publicação distintas para um indicador, desc."""
    ind = _odata_quote(indicador)
    odata = {
        "$filter": f"Indicador eq '{ind}' and baseCalculo eq 0",
        "$select": "Data",
        "$format": "json",
        "$orderby": "Data desc",
        "$top": str(max(n * 5, 100)),
    }
    url = _build_url("ExpectativasMercadoAnuais", odata)
    rows = _get(url)
    datas = sorted({_parse_iso_date(r["Data"]) for r in rows if r.get("Data")},
                   reverse=True)
    return datas[:n]


def expectativas_anuais_por_data(
    data_pub: date, indicador: str, ano_ref: Optional[int] = None
) -> list[dict]:
    """
    Retorna linhas de expectativas anuais para (indicador, data_pub).
    Se `ano_ref` for informado, filtra também por DataReferencia.
    Cada dict contém: ano_ref (int), mediana (float).
    """
    ind = _odata_quote(indicador)
    filtros = [
        f"Indicador eq '{ind}'",
        f"Data eq '{data_pub.isoformat()}'",
        "baseCalculo eq 0",
    ]
    if ano_ref is not None:
        filtros.append(f"DataReferencia eq '{ano_ref}'")
    odata = {
        "$filter": " and ".join(filtros),
        "$select": "DataReferencia,Mediana",
        "$format": "json",
        "$top": "20",
    }
    url = _build_url("ExpectativasMercadoAnuais", odata)
    rows = _get(url)
    out = []
    for r in rows:
        try:
            out.append({
                "ano_ref": int(r["DataReferencia"]),
                "mediana": float(r["Mediana"]),
            })
        except (KeyError, TypeError, ValueError):
            continue
    return out


def historico_anuais(
    indicador: str, ano_ref: int, data_inicio: date
) -> list[dict]:
    """Série temporal de (data_pub, mediana) para (indicador, ano_ref) desde data_inicio."""
    ind = _odata_quote(indicador)
    filtros = [
        f"Indicador eq '{ind}'",
        f"DataReferencia eq '{ano_ref}'",
        f"Data ge '{data_inicio.isoformat()}'",
        "baseCalculo eq 0",
    ]
    odata = {
        "$filter": " and ".join(filtros),
        "$select": "Data,Mediana",
        "$format": "json",
        "$top": "2000",
    }
    url = _build_url("ExpectativasMercadoAnuais", odata)
    rows = _get(url)
    out = []
    for r in rows:
        try:
            out.append({
                "data": _parse_iso_date(r["Data"]),
                "mediana": float(r["Mediana"]),
            })
        except (KeyError, TypeError, ValueError):
            continue
    return out


# -----------------------------------------------------------------------------
# Selic
# -----------------------------------------------------------------------------

def selic_por_reuniao(data_pub: date, reuniao: str) -> Optional[float]:
    """Mediana Selic para uma reunião (e.g. 'R8/2026') numa data de publicação."""
    odata = {
        "$filter": f"Data eq '{data_pub.isoformat()}' and Reuniao eq '{_odata_quote(reuniao)}'",
        "$select": "Mediana",
        "$format": "json",
        "$top": "1",
    }
    url = _build_url("ExpectativasMercadoSelic", odata)
    rows = _get(url)
    if rows:
        try:
            return float(rows[0]["Mediana"])
        except (KeyError, TypeError, ValueError):
            return None
    return None


def selic_ultima_reuniao_do_ano(data_pub: date, ano: int) -> Optional[tuple[str, float]]:
    """
    Descobre a última reunião COPOM do ano e retorna (reuniao, mediana).
    Robusto a mudanças no calendário (nem todo ano tem R8).
    """
    odata = {
        "$filter": f"Data eq '{data_pub.isoformat()}'",
        "$select": "Reuniao,Mediana",
        "$format": "json",
        "$top": "200",
    }
    url = _build_url("ExpectativasMercadoSelic", odata)
    rows = _get(url)
    candidatas: list[tuple[int, str, float]] = []
    for r in rows:
        reuniao = r.get("Reuniao", "")
        if not reuniao.endswith(f"/{ano}"):
            continue
        try:
            num = int(reuniao.split("/")[0].lstrip("R"))
            candidatas.append((num, reuniao, float(r["Mediana"])))
        except (ValueError, KeyError, TypeError):
            continue
    if not candidatas:
        return None
    candidatas.sort(reverse=True)
    _, reuniao, mediana = candidatas[0]
    return reuniao, mediana


# -----------------------------------------------------------------------------
# Utilidades
# -----------------------------------------------------------------------------

def _parse_iso_date(s: str) -> date:
    # A API retorna 'YYYY-MM-DD'
    return date.fromisoformat(s[:10])
