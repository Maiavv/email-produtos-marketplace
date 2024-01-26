import schedule
import datetime
from obter_produtos_concorrentes import obtem_dados_lojas_vtex
from cria_excel_email import criar_excel_email
from enviar_email import enviar_email


def job():
    lojas = ["cassol", "nichele", "obramax"]

    for loja in lojas:
        obtem_dados_lojas_vtex(loja)

    hoje = datetime.date.today()

    

    enviar_email(
        subject="Dados concorrentes - " + str(hoje), message="", recipient_list=[""]
    )


schedule.every().day.at("00:00").do(job)
