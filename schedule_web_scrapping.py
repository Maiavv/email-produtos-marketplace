import schedule
import time
from obter_produtos_concorrentes import obtem_dados_lojas_vtex

# def job() -> None:
lojas: list[str] = ["cassol", "nichele", "obramax"]
for loja in lojas:
    obtem_dados_lojas_vtex(loja)

# schedule.every().day.at("04:00").do(job)

# while True:
#     schedule.run_pending()
#     time.sleep(1)
