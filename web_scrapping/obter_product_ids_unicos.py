import pandas as pd
import requests
import threading
from datetime import datetime
import time
import os


def gerar_link(start, end, loja_vtex: str) -> str:
    """Gera um URL para fazer uma requisição à API da vtex."""
    base_url = f"https://www.{loja_vtex}.com.br/api/catalog_system/pub/products/search?"
    query_string = "&".join([f"fq=productId:{i}" for i in range(start, end + 1)])
    return f"{base_url}{query_string}"


def fazer_request(link) -> list:
    """Faz uma requisição à API da vtex e retorna uma lista com os dados."""
    try:
        response = requests.get(link)
        return response.json()
    except Exception as e:
        return []


def obter_dados_importantes(data) -> list:
    """Extrai apenas o productId da resposta da API da vtex."""
    return [item.get("productId", None) for item in data if item.get("productId", None)]


def salvar_em_csv(all_product_ids, loja):
    """Salva os productIds em um arquivo CSV para a loja especificada, utilizando o modo 'append'."""
    if all_product_ids:
        df = pd.DataFrame(all_product_ids)
        nome_arquivo = f"product_ids_{loja}_v2.csv"

        if not os.path.exists(nome_arquivo):
            df.to_csv(nome_arquivo, mode="a", index=False, header=True)
        else:
            df.to_csv(nome_arquivo, mode="a", index=False, header=False)


def obtem_dados_lojas_vtex(loja) -> None:
    """Função principal para coletar dados da loja especificada e salvar progressivamente em um arquivo CSV."""
    total_products = 1000000
    per_page = 50

    for i in range(0, total_products + 1, per_page):
        end = i + per_page - 1
        try:
            link = gerar_link(i, end, loja)
            print(f"Fetching data for products {i} to {end} in {loja}...")

            response_data = fazer_request(link)
            product_ids = obter_dados_importantes(response_data)
        except:
            ...

        if product_ids:
            salvar_em_csv(product_ids, loja)


def threads_obtem_dados_lojas_vtex(lojas):
    """Função para obter os dados das lojas utilizando threads."""
    threads = [
        threading.Thread(target=obtem_dados_lojas_vtex, args=(loja,)) for loja in lojas
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    print("Finished fetching data for all stores.")

lojas = ['cassol', 'revestacabamentos']

if __name__ == "__main__":
    for loja in lojas:
        obtem_dados_lojas_vtex(loja)
