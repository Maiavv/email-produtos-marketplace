import pandas as pd
import requests
import time
import os
import sqlite3
from datetime import datetime


colunas_desejadas = [
    "productId",
    "productName",
    "brand",
    "EAN"
    "productReference",
    "itemId",
    "Price",
    "PriceWithoutDiscount",
    "AvailableQuantity",
    "IsAvailable",
]


def gerar_link(start, end, loja_vtex: str) -> str:
    """
    Gera um URL para fazer uma requisição à API da vtex.
    """
    base_url = f"https://www.{loja_vtex}.com.br/api/catalog_system/pub/products/search?"

    query_string = "&".join([f"fq=productId:{i}" for i in range(start, end + 1)])
    return f"{base_url}{query_string}"


def fazer_request(link) -> dict:
    """
    Faz uma requisição à API da vtex, e retorna um dicionário com os dados.
    """
    try:
        response = requests.get(link)
        data = response.json()
        return data
    except Exception as e:
        print(f"Erro ao fazer request: {e}")
        data = {}
        return data


def obter_dados_importantes(data, colunas_desejadas) -> list:
    """
    Extrai os dados importantes da resposta da API da vtex.
    """
    extracted_data = []
    for item in data:
        try:
            important_data = {
                key: item.get(key, None) for key in colunas_desejadas if key in item
            }
            items_data = item.get("items", [{}])[0]
            important_data["itemId"] = items_data.get("itemId", None)

            sellers_data = items_data.get("sellers", [{}])[0]
            commertial_offer_data = sellers_data.get("commertialOffer", {})
            important_data.update(
                {
                    key: commertial_offer_data.get(key, None)
                    for key in [
                        "Price",
                        "ListPrice",
                        "PriceWithoutDiscount",
                        "RewardValue",
                        "AvailableQuantity",
                        "IsAvailable",
                    ]
                }
            )
            extracted_data.append(important_data)
        except Exception as e:
            print(f"Erro ao processar item: {e}")

    return extracted_data


def transformar_dados_em_dataframe(data) -> pd.DataFrame:
    """
    Transforma os dados em um dataframe.
    """
    return pd.DataFrame(data)


def salvar_em_sqlite(df, nome_tabela, nome_arquivo_db):
    conexao = sqlite3.connect(nome_arquivo_db)

    df.to_sql(nome_tabela, conexao, if_exists="append", index=False)

    conexao.close()


def obtem_dados_lojas_vtex(loja) -> None:
    """
    Função principal do programa.
    """
    total_products = 300
    per_page = 50
    save_interval = 150
    sleep_time = 1.5
    dia = datetime.now().strftime("%d-%m-%Y-%H-%M")

    all_data = []

    for i in range(1, total_products + 1, per_page):
        end = i + per_page - 1
        link = gerar_link(i, end, loja)

        print(f"Fetching data for products {i} to {end}...")

        response_data = fazer_request(link)

        extracted_data = obter_dados_importantes(response_data, colunas_desejadas)
        all_data.extend(extracted_data)

        if end % save_interval == 0:
            df = transformar_dados_em_dataframe(all_data)
            filename = f"data_{i - save_interval + 1}_{end}_dia_{dia}_loja_{loja}.xlsx"
            try:
                salvar_em_sqlite(df, loja, "dados_concorrentes.db")
                print(
                    f"Data for products {i - save_interval + 1} to {end} saved to {filename}"
                )
            except Exception as e:
                print(f"xabu ao salvar os ids com final {end} no dia {dia} da loja {loja}: {e}")
            all_data = []

        time.sleep(sleep_time)

    if all_data:
        start = total_products - (total_products % save_interval) + 1
        df = transformar_dados_em_dataframe(all_data)
        filename = f"data_{start}_{total_products}_dia_{dia}_loja_{loja}.xlsx"
        salvar_em_sqlite(df, loja, "dados_concorrentes.db")
        print(f"Data for products {start} to {total_products} saved to {filename}")
