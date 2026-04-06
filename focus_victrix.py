"""
focus_victrix.py
Busca dados do Relatório Focus (BCB), gera tabela no estilo Victrix Capital
e envia por email.
"""

import requests
import smtplib
import os
import io
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.text import MIMEText

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.dates as mdates

EMAIL_ORIGEM  = "z.cassiolato@gmail.com"
BCC_DESTINOS   = "jvpcassiolato@gmail.com, jcassiolato@victrixcapital.com.br, gjesus@victrixcapital.com.br, ggiron@victrixcapital.com.br, rscassiolato@gmail.com, bperroni@gmail.com"
SENHA_APP     = os.environ.get("GMAIL_APP_PASSWORD", "").strip()

BG      = '#0E1C0E'
LIME    = '#88E833'
MID_GRN = '#2E6F3A'
SAGE    = '#D5DAD0'
WHITE   = '#FFFFFF'
ALT_ROW = '#152615'


def ultima_sexta(semanas_atras: int = 0) -> str:
    hoje = datetime.today()
    dias_ate_sexta = (hoje.weekday() - 4) % 7
    ultima = hoje - timedelta(days=dias_ate_sexta + semanas_atras * 7)
    return ultima.strftime("%Y-%m-%d")


def busca_focus(indicador: str, data_ref: str):
    url = (
        "https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/"
        "ExpectativasMercadoAnuais"
        f"?$top=1"
        f"&$filter=Indicador eq '{indicador}' and Data eq '{data_ref}' and baseCalculo eq 0"
        "&$select=Mediana"
        "&$format=json"
        "&$orderby=Data desc"
    )
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        dados = r.json().get("value", [])
        if dados:
            return float(dados[0]["Mediana"])
    except Exception as e:
        print(f"Erro {indicador} ({data_ref}): {e}")
    return None


def busca_selic(data_ref: str):
    ano = datetime.today().year
    reuniao = f"R8/{ano}"
    url = (
        "https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/"
        "ExpectativasMercadoSelic"
        f"?$top=1"
        f"&$filter=Data eq '{data_ref}' and Reuniao eq '{reuniao}'"
        "&$select=Mediana"
        "&$format=json"
    )
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        dados = r.json().get("value", [])
        if dados:
            return float(dados[0]["Mediana"])
    except Exception as e:
        print(f"Erro Selic ({data_ref}): {e}")
    return None


def busca_cambio(data_ref: str):
    url = (
        "https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/"
        "ExpectativasMercadoAnuais"
        f"?$top=1"
        f"&$filter=Indicador eq 'C%C3%A2mbio' and Data eq '{data_ref}' and baseCalculo eq 0"
        "&$select=Mediana"
        "&$format=json"
        "&$orderby=Data desc"
    )
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        dados = r.json().get("value", [])
        if dados:
            return float(dados[0]["Mediana"])
    except Exception as e:
        print(f"Erro Cambio ({data_ref}): {e}")
    return None


def busca_historico_ipca(ano_ref: int) -> list:
    """Retorna lista de (data_str, mediana) para expectativa de IPCA de `ano_ref`, últimas 52 semanas."""
    data_inicio = (datetime.today() - timedelta(weeks=52)).strftime("%Y-%m-%d")
    url = (
        "https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/"
        "ExpectativasMercadoAnuais"
        f"?$filter=Indicador eq 'IPCA' and DataReferencia eq '{ano_ref}' and Data ge '{data_inicio}' and baseCalculo eq 0"
        "&$select=Data,Mediana"
        "&$format=json"
    )
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        dados = r.json().get("value", [])
        resultado = []
        for d in dados:
            try:
                resultado.append((d["Data"], float(d["Mediana"])))
            except (KeyError, TypeError, ValueError):
                continue
        resultado.sort(key=lambda x: x[0])
        return resultado
    except Exception as e:
        print(f"Erro histórico IPCA {ano_ref}: {e}")
        return []


def gera_grafico_ipca(serie_2026: list, serie_2027: list) -> bytes:
    fig, ax = plt.subplots(figsize=(7, 3.5), facecolor=BG)
    ax.set_facecolor(BG)

    if serie_2026:
        datas_26 = [datetime.strptime(d, "%Y-%m-%d") for d, _ in serie_2026]
        vals_26  = [v for _, v in serie_2026]
        ax.plot(datas_26, vals_26, color=LIME, linewidth=2.0, label='IPCA 2026')

    if serie_2027:
        datas_27 = [datetime.strptime(d, "%Y-%m-%d") for d, _ in serie_2027]
        vals_27  = [v for _, v in serie_2027]
        ax.plot(datas_27, vals_27, color=SAGE, linewidth=1.5,
                linestyle='--', label='IPCA 2027')

    ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=4, interval=4))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b/%y'))
    plt.xticks(rotation=45, ha='right', color=SAGE, fontsize=7)
    plt.yticks(color=SAGE, fontsize=8)

    ax.set_title("Evolução da Expectativa para o IPCA",
                 color=WHITE, fontsize=10, pad=8)
    ax.set_ylabel('%', color=SAGE, fontsize=8)
    ax.grid(axis='y', color=MID_GRN, alpha=0.3, linewidth=0.5)

    for spine in ax.spines.values():
        spine.set_edgecolor(MID_GRN)

    ax.tick_params(colors=SAGE)
    ax.legend(facecolor=BG, edgecolor=MID_GRN, labelcolor=SAGE, fontsize=8)

    buf = io.BytesIO()
    plt.savefig(buf, format='PNG', dpi=150, bbox_inches='tight',
                pad_inches=0.15, facecolor=BG)
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def fmt(valor, decimais=2) -> str:
    if valor is None:
        return "-"
    return f"{valor:,.{decimais}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def seta(hoje, ant) -> str:
    if hoje is None or ant is None:
        return "-"
    return "▲" if hoje > ant else ("▼" if hoje < ant else "=")


def hex_to_rgba(h, a):
    h = h.lstrip('#')
    r, g, b = tuple(int(h[i:i+2], 16) / 255 for i in (0, 2, 4))
    return (r, g, b, a)


def gera_imagem(rows_data: list) -> bytes:
    headers = ['', 'Ha 4\nsemanas', 'Ha 1\nsemana', 'Hoje', 'Comp.\nsemanal *']
    COL_W   = [1.9, 1.05, 1.05, 0.90, 0.95]
    ROW_H   = 0.52
    TITLE_H = 0.90
    total_w = sum(COL_W)
    FIG_W   = total_w
    FIG_H   = TITLE_H + ROW_H * len(rows_data)
    x0      = 0.0
    hoje_x  = x0 + COL_W[0] + COL_W[1] + COL_W[2]

    fig, ax = plt.subplots(figsize=(FIG_W, FIG_H), facecolor=(0,0,0,0))
    ax.set_facecolor((0,0,0,0))
    ax.axis('off')
    ax.set_xlim(0, FIG_W)
    ax.set_ylim(0, FIG_H)

    ax.add_patch(patches.Rectangle((x0,0), total_w, FIG_H,
        facecolor=hex_to_rgba(BG, 0.80), edgecolor='none'))

    ax.add_patch(patches.Rectangle((x0, FIG_H-TITLE_H), COL_W[0], TITLE_H,
        facecolor=hex_to_rgba(MID_GRN,1.0), edgecolor='none'))
    ax.text(x0+0.10, FIG_H-TITLE_H/2+0.10, 'Mediana Focus',
        color=WHITE, fontsize=10, fontweight='bold', va='center', ha='left')
    ax.text(x0+0.10, FIG_H-TITLE_H/2-0.17, '(BCB)',
        color=WHITE, fontsize=9.5, fontweight='bold', va='center', ha='left')

    hdr_x = x0 + COL_W[0]
    hdr_w = total_w - COL_W[0]
    ax.add_patch(patches.Rectangle((hdr_x, FIG_H-TITLE_H), hdr_w, TITLE_H,
        facecolor=hex_to_rgba(MID_GRN,1.0), edgecolor='none'))
    ax.text(hdr_x+hdr_w/2, FIG_H-0.10, str(datetime.today().year),
        color=LIME, fontsize=17, fontweight='bold', va='top', ha='center')

    cx = x0
    for i, (h, w) in enumerate(zip(headers, COL_W)):
        if i == 0:
            cx += w
            continue
        ax.text(cx+w/2, FIG_H-TITLE_H+0.22, h,
            color=LIME if i==3 else WHITE,
            fontsize=7 if i==3 else 6.5, fontweight='bold',
            va='center', ha='center', multialignment='center', linespacing=1.2)
        cx += w

    ax.plot([hdr_x, hdr_x+hdr_w], [FIG_H-TITLE_H+0.44]*2,
        color=WHITE, linewidth=0.35, alpha=0.35)

    for r_idx, row in enumerate(rows_data):
        row_y = FIG_H - TITLE_H - ROW_H * (r_idx + 1)
        fill  = hex_to_rgba(ALT_ROW,1.0) if r_idx%2==0 else hex_to_rgba(BG,0.0)

        ax.add_patch(patches.Rectangle((x0,row_y), total_w, ROW_H,
            facecolor=fill, edgecolor='none'))
        ax.add_patch(patches.Rectangle((hoje_x,row_y), COL_W[3], ROW_H,
            facecolor=hex_to_rgba(LIME,0.10), edgecolor='none'))

        ax.text(x0+0.10, row_y+ROW_H/2, row["label"],
            color=WHITE, fontsize=7.8,
            fontweight='bold' if r_idx%2==1 else 'normal',
            va='center', ha='left')

        cx = x0 + COL_W[0]
        for col_i, (val, w) in enumerate(zip(
                [row["v4"], row["v1"], row["hoje"]], COL_W[1:4])):
            ax.text(cx+w/2, row_y+ROW_H/2, val,
                color=LIME if col_i==2 else SAGE,
                fontsize=11 if col_i==2 else 8.5, fontweight='bold',
                va='center', ha='center')
            cx += w

        if row.get("comp"):
            ac = LIME if '▲' in row["comp"] else SAGE
            ax.text(cx+COL_W[4]/2, row_y+ROW_H/2, row["comp"],
                color=ac, fontsize=8.5, fontweight='bold',
                va='center', ha='center')

        ax.plot([x0, x0+total_w], [row_y]*2, color=MID_GRN, linewidth=0.4, alpha=0.5)

    buf = io.BytesIO()
    plt.savefig(buf, format='PNG', dpi=300, transparent=True,
        bbox_inches='tight', pad_inches=0.0)
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def envia_email(imagem_bytes: bytes, grafico_bytes: bytes):
    data_str = datetime.today().strftime("%d/%m/%Y")
    msg = MIMEMultipart("related")
    msg["Subject"] = f"Victrix Capital - Mediana Focus (BCB) | {data_str}"
    msg["From"]    = EMAIL_ORIGEM
    msg["To"]      = EMAIL_ORIGEM
    msg["Bcc"]     = BCC_DESTINOS

    html = f"""
    <html><body style="background:#0E1C0E;padding:24px;">
      <p style="color:#D5DAD0;font-family:Arial,sans-serif;font-size:13px;">
        Bom dia,<br><br>
        Segue a tabela semanal <strong style="color:#88E833;">Mediana Focus (BCB)</strong>
        de {data_str}, com o histórico de expectativa do IPCA para 2026 e 2027.
      </p>
      <img src="cid:tabela_focus" style="max-width:600px;border-radius:4px;"/>
      <br><br>
      <img src="cid:grafico_ipca" style="max-width:600px;border-radius:4px;"/>
      <p style="color:#2E6F3A;font-family:Arial,sans-serif;font-size:10px;margin-top:16px;">
        Victrix Capital
      </p>
    </body></html>
    """
    msg.attach(MIMEText(html, "html"))

    img_part = MIMEImage(imagem_bytes, name="focus_victrix.png")
    img_part.add_header("Content-ID", "<tabela_focus>")
    img_part.add_header("Content-Disposition", "inline", filename="focus_victrix.png")
    msg.attach(img_part)

    graf_part = MIMEImage(grafico_bytes, name="grafico_ipca.png")
    graf_part.add_header("Content-ID", "<grafico_ipca>")
    graf_part.add_header("Content-Disposition", "inline", filename="grafico_ipca.png")
    msg.attach(graf_part)

    senha = SENHA_APP.encode('ascii', errors='ignore').decode('ascii')
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(EMAIL_ORIGEM, senha)
        todos = BCC_DESTINOS.split(", ")
        smtp.sendmail(EMAIL_ORIGEM, todos, msg.as_string())
    print(f"Email enviado para {BCC_DESTINOS}")


def main():
    print("Buscando dados do Focus (BCB)...")

    sf0 = ultima_sexta(0)
    sf1 = ultima_sexta(1)
    sf4 = ultima_sexta(4)
    print(f"  Datas: {sf4} | {sf1} | {sf0}")

    ipca_h = busca_focus("IPCA", sf0)
    ipca_1 = busca_focus("IPCA", sf1)
    ipca_4 = busca_focus("IPCA", sf4)

    pib_h  = busca_focus("PIB Total", sf0)
    pib_1  = busca_focus("PIB Total", sf1)
    pib_4  = busca_focus("PIB Total", sf4)

    cam_h  = busca_cambio(sf0)
    cam_1  = busca_cambio(sf1)
    cam_4  = busca_cambio(sf4)

    sel_h  = busca_selic(sf0)
    sel_1  = busca_selic(sf1)
    sel_4  = busca_selic(sf4)

    rows_data = [
        {"label": "IPCA (variacao %)",  "v4": fmt(ipca_4), "v1": fmt(ipca_1), "hoje": fmt(ipca_h), "comp": seta(ipca_h, ipca_1)},
        {"label": "PIB (variacao %)",   "v4": fmt(pib_4),  "v1": fmt(pib_1),  "hoje": fmt(pib_h),  "comp": seta(pib_h, pib_1)},
        {"label": "Cambio (USDBRL)",    "v4": fmt(cam_4),  "v1": fmt(cam_1),  "hoje": fmt(cam_h),  "comp": seta(cam_h, cam_1)},
        {"label": "Selic (% ao ano)",   "v4": fmt(sel_4),  "v1": fmt(sel_1),  "hoje": fmt(sel_h),  "comp": seta(sel_h, sel_1)},
    ]

    print("Buscando histórico IPCA 2026 e 2027...")
    hist_2026 = busca_historico_ipca(2026)
    hist_2027 = busca_historico_ipca(2027)
    print(f"  IPCA 2026: {len(hist_2026)} pontos | IPCA 2027: {len(hist_2027)} pontos")

    print("Gerando imagem...")
    imagem = gera_imagem(rows_data)
    with open("focus_victrix_output.png", "wb") as f:
        f.write(imagem)
    print("Imagem salva: focus_victrix_output.png")

    print("Gerando gráfico IPCA...")
    grafico = gera_grafico_ipca(hist_2026, hist_2027)
    with open("focus_victrix_grafico.png", "wb") as f:
        f.write(grafico)
    print("Gráfico salvo: focus_victrix_grafico.png")

    print("Enviando email...")
    envia_email(imagem, grafico)
    print("Concluido.")


if __name__ == "__main__":
    main()