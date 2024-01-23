import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def criar_mensagem(remetente, destinatario, assunto, corpo):
    mensagem = MIMEMultipart()
    mensagem["From"] = remetente
    mensagem["To"] = destinatario
    mensagem["Subject"] = assunto
    mensagem.attach(MIMEText(corpo, "plain"))
    return mensagem


def enviar_email(smtp_server, port, seu_email, sua_senha, mensagem):
    try:
        server = smtplib.SMTP(smtp_server, port)
        server.starttls()
        server.login(seu_email, sua_senha)
        server.sendmail(mensagem["From"], mensagem["To"], mensagem.as_string())
        print("Email enviado com sucesso!")
    except Exception as e:
        print(f"Erro ao enviar o e-mail: {e}")
    finally:
        server.quit()


SMTP_SERVER = "mx.balaroti.com.br"
port = 465
seu_email = "vitor.maia@balaroti.com"
senha = os.environ.get("senha_email")


email_destinatario = "ly.salles@balaroti.com.br"
assunto = "Assunto do Email"
corpo = "Corpo do Email"


mensagem = criar_mensagem(seu_email, email_destinatario, assunto, corpo)
enviar_email(SMTP_SERVER, port, seu_email, senha, mensagem)
