import schedule
import time

from obter_produtos_concorrentes import obtem_dados_lojas_vtex
from obter_top_1500_produtos_vendidos import obtem_top_1500
from cria_excel_email import criar_excel_email
from enviar_email import enviar_email


def job() -> None:
    lojas = ["cassol", "nichele", "obramax"]
    for loja in lojas:
        obtem_dados_lojas_vtex(loja)
        obtem_top_1500(loja)

    time.sleep(50)

    criar_excel_email()
    enviar_email("Relatório de produtos dos concorrentes")


schedule.every().day.at("02:00").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
