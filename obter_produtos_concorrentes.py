import pandas as pd
import requests
import time
import os
import sqlite3
from datetime import datetime


colunas_desejadas = [
    "PriceWithoutDiscount",
    "AvailableQuantity",
    "productReference",
    "IsAvailable",
    "productName",
    "productId",
    "itemId",
    "Price",
    "brand",
    "ean",
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
        print(response.url)
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
            important_data["ean"] = items_data.get("ean", None)
            sellers_data = items_data.get("sellers", [{}])[0]
            commertial_offer_data = sellers_data.get("commertialOffer", {})
            important_data.update(
                {
                    key: commertial_offer_data.get(key, None)
                    for key in [
                        "Price",
                        "ListPrice",
                        "IsAvailable",
                        "AvailableQuantity",
                        "PriceWithoutDiscount",
                    ]
                }
            )
            extracted_data.append(important_data)
        except Exception as e:
            ...
    return extracted_data


def transformar_dados_em_dataframe(data, nome_loja, data_inicio) -> pd.DataFrame:
    """
    Transforma os dados em um DataFrame e adiciona colunas para o nome da loja e a data de início.
    """
    df = pd.DataFrame(data)
    df["Loja"] = nome_loja
    df["DataInicio"] = data_inicio
    df = df.convert_dtypes()
    return df


def salvar_em_sqlite(df, nome_loja, data_inicio, nome_arquivo_db):
    conexao = sqlite3.connect(nome_arquivo_db)
    cursor = conexao.cursor()

    # Inserir ou atualizar dados da loja
    cursor.execute("INSERT OR IGNORE INTO Lojas (nome_loja) VALUES (?)", (nome_loja,))
    id_loja = cursor.execute(
        "SELECT id_loja FROM Lojas WHERE nome_loja = ?", (nome_loja,)
    ).fetchone()[0]

    for _, produto in df.iterrows():
        # Inserir ou atualizar produto
        cursor.execute(
            "INSERT OR IGNORE INTO Produtos (id_produto, nome, marca, ean) VALUES (?, ?, ?, ?)",
            (
                produto["productReference"],
                produto["productName"],
                produto["brand"],
                produto["ean"],
            ),
        )

        # Inserir preço diário
        cursor.execute(
            """
            INSERT INTO PrecosDiarios (id_produto, id_loja, data, preco, list_price, is_available, available_quantity, preco_sem_desconto) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                produto["productReference"],
                id_loja,
                data_inicio,
                produto["Price"],
                produto["ListPrice"],
                produto["IsAvailable"],
                produto["AvailableQuantity"],
                produto["PriceWithoutDiscount"],
            ),
        )

    conexao.commit()
    conexao.close()


def obtem_dados_lojas_vtex(loja) -> None:
    """
    Função principal do programa.
    """
    total_products = 1000000
    per_page = 50
    save_interval = 10000
    sleep_time = 1.5
    dia = datetime.now().strftime("%d-%m-%Y-%H-%M")

    all_data = []

    for i in range(800000, total_products + 1, per_page):
        end = i + per_page - 1
        link = gerar_link(i, end, loja)

        print(f"Fetching data for products {i} to {end}...")

        response_data = fazer_request(link)

        extracted_data = obter_dados_importantes(response_data, colunas_desejadas)
        all_data.extend(extracted_data)

        if end % save_interval == 0:
            df = transformar_dados_em_dataframe(all_data, loja, dia)
            filename = f"data_{i - save_interval + 1}_{end}_dia_{dia}_loja_{loja}.xlsx"
            try:
                salvar_em_sqlite(df, loja, dia, "dados_concorrentes.db")
                print(
                    f"Data for products {i - save_interval + 1} to {end} saved to {filename}"
                )
            except Exception as e:
                print(f"salvar os ids com final {end} no dia {dia} da loja {loja}: {e}")
            all_data = []

        time.sleep(sleep_time)

    if all_data:
        start = total_products - (total_products % save_interval) + 1
        df = transformar_dados_em_dataframe(all_data, loja, dia)
        filename = f"data_{start}_{total_products}_dia_{dia}_loja_{loja}.xlsx"
        salvar_em_sqlite(df, loja, dia, "dados_concorrentes.db")

obtem_dados_lojas_vtex("cassol")
