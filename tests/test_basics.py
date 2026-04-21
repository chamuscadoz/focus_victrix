"""Testes unitários das funções críticas."""

from datetime import date, timedelta

import pandas as pd
import pytest

from src import query, render, storage


# ----- fmt / seta ----------------------------------------------------------

def test_fmt_formata_pt_br():
    assert render.fmt(1234.56) == "1.234,56"
    assert render.fmt(0.5) == "0,50"
    assert render.fmt(None) == "-"


def test_seta_simbolos():
    assert render.seta(5.0, 4.9) == "▲"
    assert render.seta(4.9, 5.0) == "▼"
    assert render.seta(5.0, 5.0) == "="
    assert render.seta(5.001, 5.002) == "="  # arredondamento a 2 casas
    assert render.seta(None, 5.0) == "-"


# ----- storage.data_mais_proxima -------------------------------------------

def _df_fake_datas(datas: list[date]) -> pd.DataFrame:
    return pd.DataFrame({
        "data": datas,
        "indicador": ["IPCA"] * len(datas),
        "ano_ref": [2026] * len(datas),
        "mediana": [4.5] * len(datas),
    })


def test_data_mais_proxima_prefere_anterior_ou_igual():
    base = date(2026, 4, 20)  # segunda
    pubs = [base, base - timedelta(days=3), base - timedelta(days=7)]
    df = _df_fake_datas(pubs)
    # hoje (=base) → base
    assert storage.data_mais_proxima(df, base) == base
    # há 1 semana (base - 7) → base - 7 (casamento exato)
    assert storage.data_mais_proxima(df, base - timedelta(days=7)) == base - timedelta(days=7)


def test_data_mais_proxima_quatro_semanas_real():
    """
    Bug que havia antes: ultima_publicacao(4) pegava a 5ª data distinta mais recente,
    o que — com publicações em múltiplos dias da semana — retornava ~uma semana atrás,
    não 4 semanas. Aqui simulamos esse cenário e garantimos que a nova lógica
    acerta a data de 4 semanas atrás mesmo com várias publicações intermediárias.
    """
    base = date(2026, 4, 20)
    # 20 publicações nos últimos ~30 dias (mais do que 1 por semana)
    pubs = [base - timedelta(days=d) for d in [0, 2, 3, 6, 7, 9, 13, 14, 16, 20, 21, 23, 27, 28, 30]]
    df = _df_fake_datas(pubs)
    alvo = base - timedelta(days=28)
    resultado = storage.data_mais_proxima(df, alvo)
    # Deve retornar a publicação mais próxima ≤ alvo — ou seja, base-28 ou base-30
    assert resultado in {base - timedelta(days=28), base - timedelta(days=30)}
    # NÃO pode retornar algo recente tipo base-7 (era o bug)
    assert (base - resultado).days >= 27


def test_data_mais_proxima_sem_dados():
    df = pd.DataFrame(columns=["data", "indicador", "ano_ref", "mediana"])
    assert storage.data_mais_proxima(df, date(2026, 4, 20)) is None


def test_data_mais_proxima_fora_da_tolerancia():
    pubs = [date(2025, 1, 1)]
    df = _df_fake_datas(pubs)
    # alvo muito distante → retorna None
    assert storage.data_mais_proxima(df, date(2026, 4, 20), tolerancia_dias=10) is None


# ----- query.tres_datas ----------------------------------------------------

def test_tres_datas_garante_distintas():
    """Se só houver uma data, as três campos retornados não devem colidir silenciosamente."""
    base = date(2026, 4, 20)
    df = _df_fake_datas([base])
    datas = query.tres_datas(df, referencia=base)
    # hoje encontrou, mas -1sem e -4sem devem ser None (fora da tolerância)
    assert datas.hoje == base
    assert datas.uma_semana is None
    assert datas.quatro_semanas is None


def test_tres_datas_cenario_real():
    base = date(2026, 4, 20)  # segunda
    pubs = [
        base,                            # hoje
        base - timedelta(days=7),        # -1 sem
        base - timedelta(days=14),
        base - timedelta(days=21),
        base - timedelta(days=28),       # -4 sem
    ]
    df = _df_fake_datas(pubs)
    datas = query.tres_datas(df, referencia=base)
    assert datas.hoje == base
    assert datas.uma_semana == base - timedelta(days=7)
    assert datas.quatro_semanas == base - timedelta(days=28)
