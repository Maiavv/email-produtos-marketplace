from django.core.mail import EmailMessage
from django.core.mail import get_connection
from django.core import mail

import os
import django
import datetime


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")
django.setup()


def encontrar_arquivo_recente(diretorio: str):
    arquivos = [os.path.join(diretorio, arquivo) for arquivo in os.listdir(diretorio)]
    caminho_arquivo_recente = max(arquivos, key=os.path.getctime)
    nome_arquivo = os.path.basename(caminho_arquivo_recente)
    return nome_arquivo, caminho_arquivo_recente


def enviar_email(subject: str):
    nome_arquivo, caminho_arquivo = encontrar_arquivo_recente(
        "Z:\\Vitor\\dados_concorrentes\\dados_produtos_concorrentes"
    )

    with open(caminho_arquivo, "rb") as arquivo:
        conteudo_arquivo = arquivo.read()

    mimetype = "text/xlsx"

    with get_connection() as conexao:
        EmailMessage(
            subject=subject,
            body="Bom dia, Segue em anexo o relatório de produtos dos concorrentes.",
            from_email="vitor.maia@balaroti.com.br",
            to=["luana.lara@balaroti.com.br", "jessica.machado@balaroti.com.br", "izabela.lazinski@balaroti.com.br", "rui@balaroti.com.br"],
            connection=conexao,
            cc=[
                "angeloluiz@balaroti.com.br"
            ],
            attachments=[(nome_arquivo, conteudo_arquivo, mimetype)],
        ).send()

