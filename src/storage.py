"""
storage.py
Camada de persistência em CSV para os dados do Focus.

Arquivos:
    data/focus_anuais.csv  (IPCA, PIB Total, Câmbio — por ano-referência)
    data/focus_selic.csv   (Selic — por reunião)

Schema focus_anuais:
    data         : date   — data da publicação do Focus
    indicador    : str    — 'IPCA' | 'PIB Total' | 'Câmbio'
    ano_ref      : int    — ano-referência da expectativa
    mediana      : float

Schema focus_selic:
    data         : date
    reuniao      : str    — e.g. 'R8/2026'
    mediana      : float

Chave primária (upsert): (data, indicador, ano_ref) / (data, reuniao).
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

_HERE = os.path.dirname(os.path.abspath(__file__))
_DATA_DIR = os.path.normpath(os.path.join(_HERE, "..", "data"))

ANUAIS_PATH = os.path.join(_DATA_DIR, "focus_anuais.csv")
SELIC_PATH = os.path.join(_DATA_DIR, "focus_selic.csv")

ANUAIS_COLS = ["data", "indicador", "ano_ref", "mediana"]
SELIC_COLS = ["data", "reuniao", "mediana"]


def _ensure_data_dir() -> None:
    os.makedirs(_DATA_DIR, exist_ok=True)


def _load(path: str, cols: list[str]) -> pd.DataFrame:
    if os.path.exists(path):
        df = pd.read_csv(path)
        for c in cols:
            if c not in df.columns:
                df[c] = pd.Series(dtype="object")
        return df[cols]
    return pd.DataFrame(columns=cols)


def load_anuais() -> pd.DataFrame:
    df = _load(ANUAIS_PATH, ANUAIS_COLS)
    if not df.empty:
        df["data"] = pd.to_datetime(df["data"]).dt.date
        df["ano_ref"] = df["ano_ref"].astype(int)
        df["mediana"] = df["mediana"].astype(float)
    return df


def load_selic() -> pd.DataFrame:
    df = _load(SELIC_PATH, SELIC_COLS)
    if not df.empty:
        df["data"] = pd.to_datetime(df["data"]).dt.date
        df["mediana"] = df["mediana"].astype(float)
    return df


def upsert_anuais(new_rows: pd.DataFrame) -> int:
    """Faz upsert por chave (data, indicador, ano_ref). Retorna nº de linhas inseridas/atualizadas."""
    if new_rows.empty:
        return 0
    _ensure_data_dir()
    current = load_anuais()
    new_rows = new_rows[ANUAIS_COLS].copy()
    new_rows["data"] = pd.to_datetime(new_rows["data"]).dt.date
    new_rows["ano_ref"] = new_rows["ano_ref"].astype(int)
    new_rows["mediana"] = new_rows["mediana"].astype(float)

    combined = pd.concat([current, new_rows], ignore_index=True)
    # Últimas observações por chave vencem
    combined = combined.drop_duplicates(
        subset=["data", "indicador", "ano_ref"], keep="last"
    ).sort_values(["data", "indicador", "ano_ref"]).reset_index(drop=True)
    combined.to_csv(ANUAIS_PATH, index=False)
    return len(new_rows)


def upsert_selic(new_rows: pd.DataFrame) -> int:
    if new_rows.empty:
        return 0
    _ensure_data_dir()
    current = load_selic()
    new_rows = new_rows[SELIC_COLS].copy()
    new_rows["data"] = pd.to_datetime(new_rows["data"]).dt.date
    new_rows["mediana"] = new_rows["mediana"].astype(float)
    combined = pd.concat([current, new_rows], ignore_index=True)
    combined = combined.drop_duplicates(
        subset=["data", "reuniao"], keep="last"
    ).sort_values(["data", "reuniao"]).reset_index(drop=True)
    combined.to_csv(SELIC_PATH, index=False)
    return len(new_rows)


# -----------------------------------------------------------------------------
# Queries
# -----------------------------------------------------------------------------

def datas_publicacao(df: pd.DataFrame) -> list[date]:
    """Datas distintas de publicação, ordenadas desc."""
    if df.empty:
        return []
    return sorted(df["data"].unique().tolist(), reverse=True)


def data_mais_proxima(
    df: pd.DataFrame, alvo: date, tolerancia_dias: int = 10
) -> Optional[date]:
    """Retorna a data de publicação mais próxima de `alvo`, dentro da tolerância.

    Prefere a última data ≤ alvo (a mais recente que não é "do futuro"). Se não houver
    nenhuma dentro da tolerância, retorna None.
    """
    if df.empty:
        return None
    todas = sorted(set(df["data"].tolist()))
    anteriores = [d for d in todas if d <= alvo]
    if anteriores:
        candidata = max(anteriores)
        if (alvo - candidata).days <= tolerancia_dias:
            return candidata
    # Fallback: data mais próxima em módulo
    candidata = min(todas, key=lambda d: abs((d - alvo).days))
    if abs((candidata - alvo).days) <= tolerancia_dias:
        return candidata
    return None


def valor_anual(
    df: pd.DataFrame, indicador: str, ano_ref: int, data_pub: date
) -> Optional[float]:
    sel = df[
        (df["indicador"] == indicador)
        & (df["ano_ref"] == ano_ref)
        & (df["data"] == data_pub)
    ]
    if sel.empty:
        return None
    return float(sel["mediana"].iloc[0])


def valor_selic(df: pd.DataFrame, reuniao: str, data_pub: date) -> Optional[float]:
    sel = df[(df["reuniao"] == reuniao) & (df["data"] == data_pub)]
    if sel.empty:
        return None
    return float(sel["mediana"].iloc[0])


def historico_anual(
    df: pd.DataFrame, indicador: str, ano_ref: int, semanas: int = 52
) -> list[tuple[str, float]]:
    """Lista de (data_iso, mediana) para (indicador, ano_ref) nas últimas `semanas` semanas."""
    if df.empty:
        return []
    corte = datetime.today().date() - timedelta(weeks=semanas)
    sel = df[
        (df["indicador"] == indicador)
        & (df["ano_ref"] == ano_ref)
        & (df["data"] >= corte)
    ].sort_values("data")
    return [(d.isoformat(), float(m)) for d, m in zip(sel["data"], sel["mediana"])]
