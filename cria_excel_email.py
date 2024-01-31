import pyodbc
import pandas as pd
import numpy as np
import os
import re

from sqlalchemy import create_engine, select, Table, MetaData, func, text
import sqlite3


def obter_produtos_balaroti():
    query = """--sql
    SELECT CADPROD.pr_codbarra, CADPROD.PR_DESCRICAO, CADPROD.PR_MARCA, listapre.LP_PRECOBASE, listapre.LP_PRECOVENDA, ESTOQUE_ATUAL.EA_SALATU, ESTOQUE_ATUAL.EA_IDLOJA, grupo.GR_DESCRICAO
    FROM listapre
    INNER JOIN CADPROD ON listapre.lp_idproduto = CADPROD.pr_idproduto
    INNER JOIN ESTOQUE_ATUAL ON CADPROD.PR_CODSEQ = ESTOQUE_ATUAL.EA_IDCODSEQ
    INNER JOIN grupo ON CADPROD.PR_IDGRUPO = grupo.GR_IDGRUPO
    WHERE CADPROD.PR_FORADELINHA = 'N' AND listapre.LP_IDLISTA = 1;
    -- COMO QUE FAZ PARA FILTRAR APENAS OS CADPROD.PR_CORBARRA LENGHT > 12
    """

    with pyodbc.connect("DSN=BDMTRIZ") as conn:
        cursor = conn.cursor()
        res = cursor.execute(query)
        df = pd.DataFrame(
            map(list, res.fetchall()), columns=[c[0] for c in res.description]
        )
    df = df.convert_dtypes()
    print(df.columns)
    df_agregado = (
        df.groupby(
            [
                "PR_CODBARRA",
                "PR_DESCRICAO",
                "PR_MARCA",
                "LP_PRECOBASE",
                "LP_PRECOVENDA",
                "GR_DESCRICAO",
            ]
        )
        .agg({"EA_SALATU": "sum"})
        .reset_index()
    )

    df_agregado.rename(
        columns={
            "PR_CODBARRA": "ean",
            "PR_DESCRICAO": "descricao",
            "GR_DESCRICAO": "grupo",
            "PR_MARCA": "marca",
            "LP_PRECOBASE": "preco_base",
            "LP_PRECOVENDA": "preco_venda",
            "EA_SALATU": "estoque_atual",
        },
        inplace=True,
    )

    df_agregado = df_agregado.astype(
        {
            "ean": str,
            "descricao": str,
            "marca": str,
            "grupo": str,
            "preco_base": float,
            "preco_venda": float,
            "estoque_atual": int,
        }
    )

    df_agregado["ean"] = df_agregado["ean"].astype(str)

    return df_agregado


def obter_produtos_concorrentes():
    query = """--sql
    SELECT ean, preco, marca, nome, list_price, id_loja, available_quantity, PrecosDiarios.data
    FROM Produtos
    JOIN PrecosDiarios ON Produtos.id_produto = PrecosDiarios.id_produto
    WHERE available_quantity > 0 
    """
    with sqlite3.connect("dados_concorrentes.db") as conn:
        df_concorrentes = pd.read_sql_query(query, conn)

    df_concorrentes = df_concorrentes.convert_dtypes()

    df_concorrentes["ean"] = df_concorrentes["ean"].astype(str)

    df_concorrentes = df_concorrentes.astype(
        {
            "ean": str,
            "preco": float,
            "marca": str,
            "list_price": float,
            "id_loja": int,
            "available_quantity": int,
        }
    )

    df_concorrentes = df_concorrentes.loc[
        df_concorrentes.groupby(["ean", "id_loja"]).data.idxmax()
    ]

    df_concorrentes = df_concorrentes.drop(columns=["data"])

    return df_concorrentes


def combinar_dados(df_balaroti, df_concorrentes):
    df_c = (
        df_concorrentes.set_index(["ean", "id_loja"])
        .unstack()
        .reorder_levels([1, 0], axis=1)
        .sort_index(axis=1)
    )
    df_bala = df_balaroti.set_index("ean").set_axis(
        pd.MultiIndex.from_tuples(
            [
                ("Balaroti", "descricao"),
                ("Balaroti", "marca"),
                ("Balaroti", "preco_base"),
                ("Balaroti", "preco_venda"),
                ("Balaroti", "grupo"),
                ("Balaroti", "estoque"),
            ]
        ),
        axis=1,
    )
    unidos = df_bala.join(df_c, how="outer")

    unidos["Balaroti", "descricao"] = (
        unidos["Balaroti", "descricao"]
        .pipe(lambda s: s.where(s.notna(), unidos[1, "nome"]))
        .pipe(lambda s: s.where(s.notna(), unidos[2, "nome"]))
        .pipe(lambda s: s.where(s.notna(), unidos[3, "nome"]))
    )

    unidos["Balaroti", "marca"] = (
        unidos["Balaroti", "marca"]
        .pipe(lambda s: s.where(s.notna(), unidos[1, "marca"]))
        .pipe(lambda s: s.where(s.notna(), unidos[2, "marca"]))
        .pipe(lambda s: s.where(s.notna(), unidos[3, "marca"]))
    )

    colunas_para_desc = [
        (loja, coluna) for loja, coluna in unidos.columns if coluna == "nome"
    ]

    colunas_para_marca = [
        (loja, coluna) for loja, coluna in unidos.columns if coluna == "marca"
    ]

    colunas_para_marca = colunas_para_marca[1:]

    unidos = unidos.drop(columns=colunas_para_desc)
    unidos = unidos.drop(columns=colunas_para_marca)

    return unidos


def obtem_mapeamento_lojas():
    query = """--sql
    SELECT id_loja, nome_loja
    FROM Lojas
    """
    with sqlite3.connect("dados_concorrentes.db") as conn:
        df_lojas = pd.read_sql_query(query, conn)

    df_lojas = df_lojas.convert_dtypes()

    df_lojas["id_loja"] = df_lojas["id_loja"].astype(int)

    return df_lojas


def calcular_variacao_preco_estoque(
    df: pd.DataFrame, dicionario_lojas: dict
) -> pd.DataFrame:
    lojas = dicionario_lojas.set_index("id_loja").nome_loja

    colunas_concorrentes = df.columns.get_level_values(0).unique()
    colunas_concorrentes = colunas_concorrentes[colunas_concorrentes != "Balaroti"]

    for loja in colunas_concorrentes:
        preco_venda_balaroti = df["Balaroti", "preco_venda"]
        preco_concorrente = df[loja, "preco"]
        variacao_preco = (
            (preco_concorrente - preco_venda_balaroti) / preco_venda_balaroti * 100
        )
        variacao_preco_formatada = variacao_preco.apply(lambda x: f"{x:.2f}%")
        df.insert(
            df.columns.get_loc((loja, "preco")) + 1,
            (loja, "variacao_preco_%"),
            variacao_preco_formatada,
        )

        estoque_balaroti = df["Balaroti", "estoque"]
        quantidade_concorrente = df[loja, "available_quantity"]
        variacao_estoque = quantidade_concorrente - estoque_balaroti
        df.insert(
            df.columns.get_loc((loja, "variacao_preco_%")) + 1,
            (loja, "variacao_estoque"),
            variacao_estoque,
        )

    df = df.rename(lambda l: lojas.get(l, l), axis=1, level=0).rename_axis(
        ["lojas", "colunas"], axis=1
    )

    return df


def formatar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    for loja, coluna in df.columns:
        if "variacao_preco" in coluna:
            continue

        if "preco" in coluna and "variacao_preco" not in coluna:
            df[loja, coluna] = (
                df[loja, coluna]
                .replace("", np.nan)
                .dropna()
                .apply(lambda x: x.replace(",", ".") if isinstance(x, str) else x)
                .astype(float)
                .apply(lambda x: f"{x:.2f}".replace(".", ","))
            )

        elif "estoque" in coluna:
            df[loja, coluna] = df[loja, coluna].fillna(0).astype(int)
            df[loja, coluna] = df[loja, coluna].replace(0, np.nan).fillna("")

    df = df.sort_index(axis=1)

    return df


def obtem_ranqueamento(caminho_top_2500: str, df_lojas) -> pd.DataFrame:
    padrao = r"top_2500\\(.*?)\\top_2500_dia_(.*?).csv"
    df_concat = pd.DataFrame()

    for root, dirs, files in os.walk(caminho_top_2500):
        for nome_arquivo in sorted(
            files, reverse=True
        ):  # Ordena os arquivos para pegar o mais recente primeiro
            if nome_arquivo.endswith(".csv"):
                caminho_completo = os.path.join(root, nome_arquivo)
                correspondencia = re.search(padrao, caminho_completo)
                if correspondencia:
                    nome_loja = correspondencia.group(1).lower()
                    id_loja = df_lojas.loc[
                        df_lojas["nome_loja"].str.lower() == nome_loja,
                        "nome_loja",
                    ].values[0]

                    df = pd.read_csv(
                        caminho_completo, sep=";", encoding="utf-8-sig", decimal=","
                    )
                    df.rename(columns={"Unnamed: 0": "ranqueamento"}, inplace=True)
                    df = df[["ranqueamento", "ean"]]
                    df["loja"] = id_loja  # Adiciona a coluna loja com o ID

                    df_concat = pd.concat([df_concat, df], ignore_index=True)
                    break  # Interrompe após processar o arquivo mais recente de cada loja

    df_concat["ean"] = df_concat["ean"].astype(str)

    return df_concat


caminho = r"Z:\Vitor\dados_concorrentes\top_2500"
df_lojas = obtem_mapeamento_lojas()
df_ranqueamento = obtem_ranqueamento(caminho, df_lojas)


def adicionar_ranqueamento_por_loja(df: pd.DataFrame, df_ranqueamento: pd.DataFrame) -> pd.DataFrame:
    # Conversão do índice do df para Int64, lidando com NaNs.
    # Nota: Se o índice já é um MultiIndex, esta abordagem precisa ser ajustada.
    df.index = pd.to_numeric(df.index, errors='coerce').astype('Int64')

    # Conversão da coluna 'ean' em df_ranqueamento para Int64, lidando com strings vazias e NaNs.
    df_ranqueamento['ean'] = pd.to_numeric(df_ranqueamento['ean'].replace('', np.nan), errors='coerce').astype('Int64')

    for loja in df_ranqueamento['loja'].unique():
        df_ranqueamento_loja = df_ranqueamento[df_ranqueamento['loja'] == loja]

        # Criação do dicionário de ranqueamento com EAN como chave
        ranqueamento_dict = df_ranqueamento_loja.set_index('ean')['ranqueamento'].to_dict()

        # Mapeamento do ranqueamento para o DataFrame df
        df[(loja, 'ranqueamento')] = df.index.map(ranqueamento_dict.get)

    return df


df_unidos = adicionar_ranqueamento_por_loja(df, df_ranqueamento)
df_unidos.to_csv("teste.csv", sep=";", encoding="utf-8-sig", decimal=",")


# %%

dicionario = {
    "descricao": "descricao",
    "marca": "marca",
    "grupo": "grupo",
    "preco_base": "preco_compra",
    "preco_venda": "preco_venda",
    "estoque": "estoque",
    "available_quantity": "estoque_site",
    "list_price": "preco_site",
    "preco": "preco",
    "variacao_preco_%": "variacao_preco",
    "variacao_estoque": "variacao_estoque",
}

categorico = pd.CategoricalDtype(
    [
        "descricao",
        "marca",
        "grupo",
        "preco_compra",
        "preco_venda",
        "estoque",
        "preco_site",
        "preco",
        "estoque_site",
        "variacao_preco",
        "variacao_estoque",
    ],
    ordered=True,
)


def criar_excel_email():
    dia_hoje = pd.Timestamp.today().strftime("%d-%m")
    df_balaroti = obter_produtos_balaroti()
    df_concorrentes = obter_produtos_concorrentes()
    df_unidos = combinar_dados(df_balaroti, df_concorrentes)
    dicionario_lojas = obtem_mapeamento_lojas()
    df_unidos = calcular_variacao_preco_estoque(df_unidos, dicionario_lojas)

    df_unidos.rename(dicionario, axis=1, inplace=True, level=1)

    df_unidos.columns = df_unidos.columns.set_levels(
        df_unidos.columns.levels[1].astype(categorico), level=1
    )
    df_final = df_unidos.sort_index(axis=1)
    df_final.replace("nan%", "", regex=True, inplace=True)
    df_final = formatar_colunas(df_final)

    df_final.fillna("").replace("nan%", "", regex=True).to_csv(
        f"dados_concorrentes_{dia_hoje}.csv",
        sep=";",
        encoding="utf-8-sig",
        na_rep="0",
        decimal=",",
    )

    return df_final


df = criar_excel_email()
