import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def enviar_email(destinatario, assunto, mensagem):
    remetente = 'balaroti.avisos@gmail.com'
    senha = 'AvisosBala@123'

    # Configuração do email
    msg = MIMEMultipart()
    msg['From'] = remetente
    msg['To'] = destinatario
    msg['Subject'] = assunto

    # Corpo do email
    msg.attach(MIMEText(mensagem, 'plain'))

    # Criando o servidor SMTP
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(remetente, senha)

    # Enviando o email
    server.sendmail(remetente, destinatario, msg.as_string())
    server.quit()

        