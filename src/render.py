"""
render.py
Geração da tabela (PNG) e do gráfico de IPCA (PNG).

Preserva fielmente o visual original da identidade Victrix Capital.
"""

from __future__ import annotations

import io
import os
from datetime import datetime
from typing import Optional

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.font_manager as fm
import matplotlib.image as mpimg
import matplotlib.patches as patches
import matplotlib.pyplot as plt

# Paleta Victrix Capital
BG = "#0E1C0E"
LIME = "#88E833"
MID_GRN = "#2E6F3A"
SAGE = "#D5DAD0"
WHITE = "#FFFFFF"
ALT_ROW = "#152615"

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.normpath(os.path.join(_HERE, ".."))
_FONT_PFX = os.path.join(_ROOT, "assets", "font", "static", "ZalandoSansSemiExpanded-")
_FONT_MAP = {
    "reg": "Regular",
    "semi": "SemiBold",
    "bold": "Bold",
    "light": "Light",
    "xlight": "ExtraLight",
}
_BLUR_PATH = os.path.join(_ROOT, "assets", "blur", "blur_65.png")
_LOGO_PATH = os.path.join(_ROOT, "assets", "logo", "png01.png")


def _fp(style: str, size: float) -> fm.FontProperties:
    path = _FONT_PFX + _FONT_MAP[style] + ".ttf"
    if os.path.exists(path):
        p = fm.FontProperties(fname=path)
    else:
        # Fallback silencioso — CI pode não ter as fontes Zalando
        p = fm.FontProperties(family="DejaVu Sans")
    p.set_size(size)
    return p


def fmt(valor: Optional[float], decimais: int = 2) -> str:
    if valor is None:
        return "-"
    return (
        f"{valor:,.{decimais}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    )


def seta(hoje: Optional[float], ant: Optional[float]) -> str:
    if hoje is None or ant is None:
        return "-"
    h = round(hoje, 2)
    a = round(ant, 2)
    return "▲" if h > a else ("▼" if h < a else "=")


def _hex_to_rgba(h: str, a: float) -> tuple[float, float, float, float]:
    h = h.lstrip("#")
    r, g, b = (int(h[i : i + 2], 16) / 255 for i in (0, 2, 4))
    return (r, g, b, a)


def gera_imagem(rows_data: list[dict], ano_tabela: int) -> bytes:
    """rows_data: lista de dicts com keys: label, v8, v4, v1, hoje, comp."""
    blur_img = mpimg.imread(_BLUR_PATH)
    logo_img = mpimg.imread(_LOGO_PATH)

    headers = ["", "Há 8\nsemanas", "Há 4\nsemanas", "Há 1\nsemana", "Atual", "Var.\nsemanal"]
    COL_W = [1.65, 0.78, 0.78, 0.78, 0.82, 1.04]
    ROW_H = 0.52
    TITLE_H = 0.90
    LOGO_H = 0.55
    total_w = sum(COL_W)
    FIG_W = total_w
    FIG_H = TITLE_H + ROW_H * len(rows_data) + LOGO_H
    x0 = 0.0
    hoje_x = x0 + COL_W[0] + COL_W[1] + COL_W[2] + COL_W[3]

    fig, ax = plt.subplots(figsize=(FIG_W, FIG_H), facecolor=(0, 0, 0, 0))
    ax.set_facecolor((0, 0, 0, 0))
    ax.set_zorder(1)

    bg = fig.add_axes([0, 0, 1, 1], zorder=0)
    bg.imshow(blur_img, aspect="auto")
    bg.add_patch(
        patches.Rectangle(
            (0, 0), 1, 1,
            facecolor=_hex_to_rgba(BG, 0.25), edgecolor="none", transform=bg.transAxes,
        )
    )
    bg.axis("off")

    ax.axis("off")
    ax.set_xlim(0, FIG_W)
    ax.set_ylim(0, FIG_H)

    # Cabeçalho esquerdo
    ax.add_patch(
        patches.Rectangle(
            (x0, FIG_H - TITLE_H), COL_W[0], TITLE_H,
            facecolor=_hex_to_rgba(MID_GRN, 0.25), edgecolor="none",
        )
    )
    ax.text(
        x0 + 0.10, FIG_H - TITLE_H / 2 + 0.10, "Mediana Focus",
        color=WHITE, fontproperties=_fp("bold", 10), va="center", ha="left",
    )
    ax.text(
        x0 + 0.10, FIG_H - TITLE_H / 2 - 0.17, "(BCB)",
        color=LIME, fontproperties=_fp("light", 9.5), va="center", ha="left",
    )

    # Cabeçalho direito — ano
    hdr_x = x0 + COL_W[0]
    hdr_w = total_w - COL_W[0]
    ax.add_patch(
        patches.Rectangle(
            (hdr_x, FIG_H - TITLE_H), hdr_w, TITLE_H,
            facecolor=_hex_to_rgba(MID_GRN, 0.25), edgecolor="none",
        )
    )
    ax.text(
        hdr_x + hdr_w / 2, FIG_H - 0.10, str(ano_tabela),
        color=LIME, fontproperties=_fp("xlight", 16), va="top", ha="center",
    )

    # Sub-headers
    cx = x0
    for i, (h, w) in enumerate(zip(headers, COL_W)):
        if i == 0:
            cx += w
            continue
        ax.text(
            cx + w / 2, FIG_H - TITLE_H + 0.22, h,
            color=LIME if i == 4 else WHITE,
            fontproperties=_fp("semi", 7 if i == 4 else 6.5),
            va="center", ha="center", multialignment="center", linespacing=1.2,
        )
        cx += w

    ax.plot(
        [hdr_x, hdr_x + hdr_w], [FIG_H - TITLE_H + 0.44] * 2,
        color=WHITE, linewidth=0.35, alpha=0.35,
    )

    # Linhas de dados
    for r_idx, row in enumerate(rows_data):
        row_y = FIG_H - TITLE_H - ROW_H * (r_idx + 1)
        fill = _hex_to_rgba(ALT_ROW, 0.85) if r_idx % 2 == 0 else _hex_to_rgba(BG, 0.0)
        ax.add_patch(patches.Rectangle(
            (x0, row_y), total_w, ROW_H, facecolor=fill, edgecolor="none"))
        ax.add_patch(patches.Rectangle(
            (hoje_x, row_y), COL_W[4], ROW_H,
            facecolor=_hex_to_rgba(LIME, 0.10), edgecolor="none"))

        fp_label = _fp("semi", 7.8) if r_idx % 2 == 1 else _fp("reg", 7.8)
        ax.text(
            x0 + 0.10, row_y + ROW_H / 2, row["label"],
            color=WHITE, fontproperties=fp_label, va="center", ha="left",
        )

        cx = x0 + COL_W[0]
        for col_i, (val, w) in enumerate(zip(
            [row["v8"], row["v4"], row["v1"], row["hoje"]], COL_W[1:5]
        )):
            fp_val = _fp("bold", 10.5 if col_i == 3 else 8.0)
            ax.text(
                cx + w / 2, row_y + ROW_H / 2, val,
                color=LIME if col_i == 3 else SAGE,
                fontproperties=fp_val, va="center", ha="center",
            )
            cx += w

        if row.get("comp"):
            ac = LIME if "▲" in row["comp"] else SAGE
            ax.text(
                cx + COL_W[5] / 2, row_y + ROW_H / 2, row["comp"],
                color=ac, fontsize=8.0, fontweight="semibold",
                va="center", ha="center",
            )

        ax.plot(
            [x0, x0 + total_w], [row_y] * 2,
            color=MID_GRN, linewidth=0.4, alpha=0.5,
        )

    # Logo
    logo_h = LOGO_H * 0.65
    logo_w = logo_h * (logo_img.shape[1] / logo_img.shape[0])
    logo_x = x0 + 0.10
    logo_y = (LOGO_H - logo_h) / 2
    ax.imshow(
        logo_img, extent=[logo_x, logo_x + logo_w, logo_y, logo_y + logo_h],
        aspect="auto", origin="upper", zorder=5,
    )
    ax.set_xlim(0, FIG_W)
    ax.set_ylim(0, FIG_H)

    buf = io.BytesIO()
    plt.savefig(buf, format="PNG", dpi=300, transparent=True,
                bbox_inches="tight", pad_inches=0.0)
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def _gera_grafico_serie(
    serie_ano1: list,
    serie_ano2: list,
    titulo: str,
    label_ano1: str,
    label_ano2: str,
) -> bytes:
    """Gera gráfico de linha com duas séries anuais (estilo Victrix)."""
    blur_img = mpimg.imread(_BLUR_PATH)

    _w = sum([1.9, 1.05, 1.05, 0.90, 0.95])
    fig, ax = plt.subplots(figsize=(_w, 3.5), facecolor=(0, 0, 0, 0))
    ax.set_facecolor("none")
    ax.set_zorder(1)
    # Margens internas para que rótulos e anotações fiquem dentro do canvas,
    # evitando que bbox_inches="tight" expanda a área e deixe bordas transparentes.
    fig.subplots_adjust(left=0.08, right=0.84, top=0.90, bottom=0.22)

    bg = fig.add_axes([0, 0, 1, 1], zorder=0)
    bg.imshow(blur_img, aspect="auto")
    bg.add_patch(patches.Rectangle(
        (0, 0), 1, 1,
        facecolor=_hex_to_rgba(BG, 0.25), edgecolor="none", transform=bg.transAxes,
    ))
    bg.axis("off")

    all_vals: list[float] = []
    legend_colors: list[str] = []

    if serie_ano1:
        datas_1 = [datetime.strptime(d, "%Y-%m-%d") for d, _ in serie_ano1]
        vals_1 = [v for _, v in serie_ano1]
        all_vals += vals_1
        ax.plot(datas_1, vals_1, color=LIME, linewidth=2.8, label=label_ano1, zorder=3)
        ax.plot(datas_1[-1], vals_1[-1], "o", color=LIME, markersize=5, zorder=4)
        ax.annotate(
            f"{fmt(vals_1[-1])}%",
            xy=(datas_1[-1], vals_1[-1]), xytext=(8, 0),
            textcoords="offset points", color=LIME,
            fontproperties=_fp("bold", 11), va="center", zorder=4,
        )
        legend_colors.append(LIME)

    if serie_ano2:
        datas_2 = [datetime.strptime(d, "%Y-%m-%d") for d, _ in serie_ano2]
        vals_2 = [v for _, v in serie_ano2]
        all_vals += vals_2
        ax.plot(datas_2, vals_2, color=SAGE, linewidth=2.0, linestyle="--",
                label=label_ano2, zorder=3)
        ax.plot(datas_2[-1], vals_2[-1], "o", color=SAGE, markersize=5, zorder=4)
        ax.annotate(
            f"{fmt(vals_2[-1])}%",
            xy=(datas_2[-1], vals_2[-1]), xytext=(8, 0),
            textcoords="offset points", color=SAGE,
            fontproperties=_fp("bold", 11), va="center", zorder=4,
        )
        legend_colors.append(SAGE)

    if all_vals:
        margem = (max(all_vals) - min(all_vals)) * 0.25 or 0.1
        ax.set_ylim(min(all_vals) - margem, max(all_vals) + margem)

    ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=4, interval=4))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b/%y"))
    for lbl in ax.get_xticklabels():
        lbl.set_fontproperties(_fp("light", 9))
        lbl.set_rotation(45)
        lbl.set_ha("right")
    for lbl in ax.get_yticklabels():
        lbl.set_fontproperties(_fp("light", 9))

    ax.set_title(titulo, color=WHITE, fontproperties=_fp("semi", 13), pad=10)
    ax.set_ylabel("%", color=SAGE, fontproperties=_fp("light", 9))
    ax.grid(axis="y", color=MID_GRN, alpha=0.4, linewidth=0.5, zorder=2)
    ax.tick_params(colors=SAGE)

    for spine in ax.spines.values():
        spine.set_visible(False)

    ax.legend(facecolor=_hex_to_rgba(BG, 0.85), edgecolor="none",
              labelcolor=legend_colors, prop=_fp("semi", 11))

    buf = io.BytesIO()
    plt.savefig(buf, format="PNG", dpi=150, transparent=True, pad_inches=0.0)
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def gera_grafico_ipca(serie_2026: list, serie_2027: list) -> bytes:
    return _gera_grafico_serie(
        serie_2026, serie_2027,
        titulo="Evolução da Expectativa para o IPCA",
        label_ano1="IPCA 2026",
        label_ano2="IPCA 2027",
    )


def gera_grafico_selic(serie_2026: list, serie_2027: list) -> bytes:
    return _gera_grafico_serie(
        serie_2026, serie_2027,
        titulo="Evolução da Expectativa para a Selic",
        label_ano1="Selic 2026",
        label_ano2="Selic 2027",
    )
