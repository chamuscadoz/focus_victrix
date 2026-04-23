"""
query.py
Camada de consulta que traduz "hoje / há 1 semana / há 4 semanas" em datas de publicação reais,
independentemente de feriados.

Bug corrigido: antes, "há 4 semanas" era obtido como "a 5ª data distinta mais recente na API",
o que retornava publicações recentíssimas (dias de distância, não semanas), porque a API
publica várias vezes por semana e a indexação por posição não equivale a offset em semanas.

Agora: calculamos a data-alvo (hoje - N dias), pegamos a publicação mais próxima ≤ alvo.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd

from . import storage

logger = logging.getLogger(__name__)


@dataclass
class TresPublicacoes:
    hoje: Optional[date]
    uma_semana: Optional[date]
    quatro_semanas: Optional[date]
    oito_semanas: Optional[date] = None

    def __iter__(self):
        return iter((self.hoje, self.uma_semana, self.quatro_semanas, self.oito_semanas))


def tres_datas(df_anuais: pd.DataFrame, referencia: Optional[date] = None) -> TresPublicacoes:
    """
    Encontra as 4 datas de publicação corretas para a tabela semanal.
    """
    if referencia is None:
        referencia = datetime.today().date()

    hoje = storage.data_mais_proxima(df_anuais, referencia, tolerancia_dias=10)

    uma: Optional[date] = None
    quatro: Optional[date] = None
    oito: Optional[date] = None
    if hoje is not None:
        uma    = _data_ate(df_anuais, referencia - timedelta(days=7),  tolerancia=7)
        quatro = _data_ate(df_anuais, referencia - timedelta(days=28), tolerancia=7)
        oito   = _data_ate(df_anuais, referencia - timedelta(days=56), tolerancia=7)

    return TresPublicacoes(hoje=hoje, uma_semana=uma, quatro_semanas=quatro, oito_semanas=oito)


def _data_anterior_a(df: pd.DataFrame, base: date, offset_dias: int) -> Optional[date]:
    """Retorna a data de publicação ≤ base - offset_dias (fallback conservador)."""
    alvo = base - timedelta(days=offset_dias)
    return storage.data_mais_proxima(df, alvo, tolerancia_dias=15)


def _data_ate(df: pd.DataFrame, limite: date, tolerancia: int) -> Optional[date]:
    """Retorna a data de publicação mais recente que seja estritamente ≤ limite."""
    todas = sorted(
        [d for d in df["data"].unique().tolist() if d <= limite], reverse=True
    )
    if not todas:
        return None
    d = todas[0]
    if (limite - d).days <= tolerancia:
        return d
    return None


@dataclass
class LinhaTabela:
    label: str
    v8: Optional[float]
    v4: Optional[float]
    v1: Optional[float]
    hoje: Optional[float]
    streak: int = 0


def _calcular_streak(
    df: pd.DataFrame,
    indicador: str,
    ano_ref: int,
    v_hoje: float,
    v_v1: float,
    data_v1: date,
) -> int:
    """
    Conta semanas consecutivas de variação na mesma direção que hoje→v1.
    Regride 7 dias por vez a partir de data_v1. Inclui a semana atual (+1).
    """
    h, a = round(v_hoje, 2), round(v_v1, 2)
    if h == a:
        return 0
    direcao = "▲" if h > a else "▼"

    sub = df[(df["indicador"] == indicador) & (df["ano_ref"] == ano_ref)]
    if sub.empty:
        return 1

    streak_prior = 0
    d_atual = data_v1
    v_atual = v_v1

    for _ in range(52):
        d_ant = storage.data_mais_proxima(sub, d_atual - timedelta(days=7), tolerancia_dias=7)
        if d_ant is None or d_ant >= d_atual:
            break
        v_ant = storage.valor_anual(df, indicador, ano_ref, d_ant)
        if v_ant is None:
            break
        ra, rb = round(v_atual, 2), round(v_ant, 2)
        dir_ = "▲" if ra > rb else ("▼" if ra < rb else "=")
        if dir_ != direcao:
            break
        streak_prior += 1
        d_atual = d_ant
        v_atual = v_ant

    return streak_prior + 1


def montar_linhas(
    df_anuais: pd.DataFrame,
    datas: TresPublicacoes,
    ano_ref: int,
) -> list[LinhaTabela]:
    """Monta as 4 linhas (IPCA, PIB, Câmbio, Selic) usando a base local."""
    def val_anual(indicador: str, d: Optional[date]) -> Optional[float]:
        if d is None:
            return None
        return storage.valor_anual(df_anuais, indicador, ano_ref, d)

    def streak_para(indicador: str, hoje_val: Optional[float], v1_val: Optional[float], data_v1: Optional[date]) -> int:
        if hoje_val is None or v1_val is None or data_v1 is None:
            return 0
        return _calcular_streak(df_anuais, indicador, ano_ref, hoje_val, v1_val, data_v1)

    ipca_hoje = val_anual("IPCA", datas.hoje)
    ipca_v1   = val_anual("IPCA", datas.uma_semana)

    pib_hoje  = val_anual("PIB Total", datas.hoje)
    pib_v1    = val_anual("PIB Total", datas.uma_semana)

    cam_hoje  = val_anual("Câmbio", datas.hoje)
    cam_v1    = val_anual("Câmbio", datas.uma_semana)

    sel_hoje  = val_anual("Selic", datas.hoje)
    sel_v1    = val_anual("Selic", datas.uma_semana)

    return [
        LinhaTabela(
            "IPCA (variacao %)",
            val_anual("IPCA", datas.oito_semanas),
            val_anual("IPCA", datas.quatro_semanas),
            ipca_v1, ipca_hoje,
            streak_para("IPCA", ipca_hoje, ipca_v1, datas.uma_semana),
        ),
        LinhaTabela(
            "PIB (variacao %)",
            val_anual("PIB Total", datas.oito_semanas),
            val_anual("PIB Total", datas.quatro_semanas),
            pib_v1, pib_hoje,
            streak_para("PIB Total", pib_hoje, pib_v1, datas.uma_semana),
        ),
        LinhaTabela(
            "Cambio (USDBRL)",
            val_anual("Câmbio", datas.oito_semanas),
            val_anual("Câmbio", datas.quatro_semanas),
            cam_v1, cam_hoje,
            streak_para("Câmbio", cam_hoje, cam_v1, datas.uma_semana),
        ),
        LinhaTabela(
            "Selic (% ao ano)",
            val_anual("Selic", datas.oito_semanas),
            val_anual("Selic", datas.quatro_semanas),
            sel_v1, sel_hoje,
            streak_para("Selic", sel_hoje, sel_v1, datas.uma_semana),
        ),
    ]
