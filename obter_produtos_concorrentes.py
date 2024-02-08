import pandas as pd
import threading
import requests
import time
import sqlite3
import os

from datetime import datetime
from typing import List

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
    "link",
    "ean",
]


def gerar_links(loja_vtex: str) -> List[str]:
    product_ids = pd.read_csv(f"product_id/product_ids_{loja_vtex}.csv")
    base_url = f"https://www.{loja_vtex}.com.br/api/catalog_system/pub/products/search?"

    # Certifique-se de que a coluna com IDs está corretamente nomeada como 'product_id' no CSV
    coluna = product_ids.columns[0]
    ids = product_ids[coluna].tolist()

    links = []
    for i in range(0, len(ids), 50):
        ids_slice = ids[i : i + 50]
        query_string = "&".join([f"fq=productId:{id_}" for id_ in ids_slice])
        links.append(f"{base_url}{query_string}")

    return links


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


def obter_dados_importantes(data: list, colunas_desejadas: list) -> list:
    """
    Extrai os dados importantes da resposta da API da vtex, incluindo o último campo do campo "categories".
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

            # Nova lógica para extrair o último campo de "categories"
            categories = item.get("categories", [])
            important_data["lastCategory"] = categories[-1] if categories else None

            extracted_data.append(important_data)
        except Exception as e:
            # Considerar logar o erro ou tratá-lo de maneira adequada
            pass
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
    with sqlite3.connect(nome_arquivo_db) as conexao:
        cursor = conexao.cursor()

        # Inserir ou atualizar dados da loja
        cursor.execute(
            "INSERT OR IGNORE INTO Lojas (nome_loja) VALUES (?)", (nome_loja,)
        )
        id_loja = cursor.execute(
            "SELECT id_loja FROM Lojas WHERE nome_loja = ?", (nome_loja,)
        ).fetchone()[0]

        # Preparar instrução SQL para inserção ou atualização de produtos
        produto_sql = """
        INSERT OR IGNORE INTO Produtos (id_produto, nome, marca, ean, categoria) VALUES (?, ?, ?, ?, ?)
        """

        # Preparar instrução SQL para inserção de preços diários
        preco_diario_sql = """
        INSERT INTO PrecosDiarios (id_produto, id_loja, data, preco, list_price, is_available, available_quantity, preco_sem_desconto) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """

        for _, produto in df.iterrows():
            cursor.execute(
                "SELECT id_produto FROM Produtos WHERE id_produto = ?",
                (produto["productReference"],),
            )
            existe = cursor.fetchone()

            if not existe:
                # Produto não existe, inserir novo registro
                cursor.execute(
                    produto_sql,
                    (
                        produto["productReference"],
                        produto["productName"],
                        produto["brand"],
                        produto["ean"],
                        produto["lastCategory"],
                    ),
                )
            else:
                cursor.execute(
                    "UPDATE Produtos SET categoria = ? WHERE id_produto = ?",
                    (produto["lastCategory"], produto["productReference"]),
                )

            # Inserir preço diário
            cursor.execute(
                preco_diario_sql,
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


def obtem_dados_lojas_vtex(loja) -> None:
    """
    Função principal do programa.
    """

    hoje = datetime.now()

    links = gerar_links(loja)
    for link in links:
        response_data = fazer_request(link)
        important_data = obter_dados_importantes(response_data, colunas_desejadas)
        time.sleep(1)
        df = transformar_dados_em_dataframe(important_data, loja, hoje)
        salvar_em_sqlite(df, loja, hoje, "dados_concorrentes.db")


def threads_obtem_dados_lojas_vtex(lojas):
    """
    Função para obter os dados das lojas utilizando threads.
    """
    threads = [
        threading.Thread(target=obtem_dados_lojas_vtex, args=(loja,)) for loja in lojas
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    print("Finished fetching data for all stores.")
