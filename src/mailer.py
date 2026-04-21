"""
mailer.py
Envio do email semanal com tabela e gráfico inline.
"""

from __future__ import annotations

import logging
import os
import smtplib
from datetime import datetime
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)

EMAIL_ORIGEM = os.environ.get("EMAIL_ORIGEM", "z.cassiolato@gmail.com")
BCC_DESTINOS = os.environ.get(
    "BCC_DESTINOS",
    "jvpcassiolato@gmail.com, jcassiolato@victrixcapital.com.br, "
    "gjesus@victrixcapital.com.br, ggiron@victrixcapital.com.br, "
    "rscassiolato@gmail.com, bperroni@gmail.com, rafaferro@gmail.com",
)


def _carrega_senha() -> str:
    senha = os.environ.get("GMAIL_APP_PASSWORD", "").strip()
    if not senha:
        raise RuntimeError(
            "GMAIL_APP_PASSWORD não definida. Configure o secret no GitHub Actions "
            "ou exporte a variável no ambiente local."
        )
    # Gmail App Passwords vêm com espaços quando copiadas; removemos.
    senha = senha.replace(" ", "")
    return senha.encode("ascii", errors="ignore").decode("ascii")


def envia_email(imagem_bytes: bytes, grafico_bytes: bytes, grafico_selic_bytes: bytes) -> None:
    data_str = datetime.today().strftime("%d/%m/%Y")
    msg = MIMEMultipart("related")
    msg["Subject"] = f"Victrix Capital - Mediana Focus (BCB) | {data_str}"
    msg["From"] = EMAIL_ORIGEM
    msg["To"] = EMAIL_ORIGEM
    msg["Bcc"] = BCC_DESTINOS

    html = f"""
    <html><body style="background:#0E1C0E;padding:24px;">
      <p style="color:#D5DAD0;font-family:Arial,sans-serif;font-size:13px;">
        Bom dia,<br><br>
        Segue a tabela semanal <strong style="color:#88E833;">Mediana Focus (BCB)</strong>
        de {data_str}, com o histórico de expectativas do IPCA e da Selic para o ano corrente e o seguinte.
      </p>
      <img src="cid:tabela_focus" style="max-width:600px;border-radius:4px;"/>
      <br><br>
      <img src="cid:grafico_ipca" style="max-width:600px;border-radius:4px;"/>
      <br><br>
      <img src="cid:grafico_selic" style="max-width:600px;border-radius:4px;"/>
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

    graf_selic_part = MIMEImage(grafico_selic_bytes, name="grafico_selic.png")
    graf_selic_part.add_header("Content-ID", "<grafico_selic>")
    graf_selic_part.add_header("Content-Disposition", "inline", filename="grafico_selic.png")
    msg.attach(graf_selic_part)

    senha = _carrega_senha()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(EMAIL_ORIGEM, senha)
        todos = [EMAIL_ORIGEM] + [e.strip() for e in BCC_DESTINOS.split(",") if e.strip()]
        smtp.sendmail(EMAIL_ORIGEM, todos, msg.as_string())
    logger.info("Email enviado para %d destinatários", len(todos))
