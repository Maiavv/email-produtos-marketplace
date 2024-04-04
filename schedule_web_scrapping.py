import schedule
import time

from web_scrapping.obter_produtos_concorrentes import obtem_dados_lojas_vtex
from web_scrapping.obter_top_1500_produtos_vendidos import obtem_top_1500
from analise.cria_excel_email import criar_excel_email
from enviar_email import enviar_email


def job() -> None:
    lojas = ["cassol", "nichele", "obramax", "revestacabamentos"]

    for loja in lojas:
        obtem_dados_lojas_vtex(loja)
        obtem_top_1500(loja)

    obtem_top_1500("balaroti")

    time.sleep(50)

    criar_excel_email()
    enviar_email("Relatório de produtos dos concorrentes")


schedule.every().day.at("01:52").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)

