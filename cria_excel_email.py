import pyodbc
import pandas as pd
import numpy as np


from sqlalchemy import create_engine, select, Table, MetaData, func, text
import sqlite3


def obter_produtos_balaroti():
    query = """--sql
    SELECT CADPROD.pr_codbarra, CADPROD.PR_DESCRICAO, LP_PRECOBASE, LP_PRECOVENDA, ESTOQUE_ATUAL.EA_SALATU, ESTOQUE_ATUAL.EA_IDLOJA
    FROM listapre
    INNER JOIN CADPROD ON listapre.lp_idproduto = CADPROD.pr_idproduto
    INNER JOIN ESTOQUE_ATUAL ON CADPROD.PR_CODSEQ = ESTOQUE_ATUAL.EA_IDCODSEQ
    WHERE CADPROD.PR_FORADELINHA = 'N' and listapre.LP_IDLISTA = 1 
    """

    with pyodbc.connect("DSN=BDMTRIZ") as conn:
        cursor = conn.cursor()
        res = cursor.execute(query)
        df = pd.DataFrame(
            map(list, res.fetchall()), columns=[c[0] for c in res.description]
        )
    df = df.convert_dtypes()
    print(df.columns)
    # Agrupamento e soma do estoque atual, mantendo as outras colunas relevantes
    df_agregado = (
        df.groupby(["PR_CODBARRA", "PR_DESCRICAO", "LP_PRECOBASE", "LP_PRECOVENDA"])
        .agg({"EA_SALATU": "sum"})
        .reset_index()
    )

    df_agregado.rename(
        columns={
            "PR_CODBARRA": "ean",
            "PR_DESCRICAO": "descricao",
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
            "preco_base": float,
            "preco_venda": float,
            "estoque_atual": int,
        }
    )

    df_agregado["ean"] = df_agregado["ean"].astype(str)

    return df_agregado


def obter_produtos_concorrentes():
    query = """--sql
    SELECT ean, preco, nome, list_price, id_loja, available_quantity, PrecosDiarios.data
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
            "list_price": float,
            "id_loja": int,
            "available_quantity": int,
        }
    )

    # pegando somente a data mais recente
    df_concorrentes = df_concorrentes.loc[
        df_concorrentes.groupby(["ean", "id_loja"]).data.idxmax()
    ]

    # tirando a coluna data
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
                ("Balaroti", "preco_base"),
                ("Balaroti", "preco_venda"),
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

    colunas_para_remover = [
        (loja, coluna) for loja, coluna in unidos.columns if coluna == "nome"
    ]
    unidos = unidos.drop(columns=colunas_para_remover)

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


import pandas as pd
import numpy as np


def formatar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    for loja, coluna in df.columns:
        # Ignorando colunas com 'variacao_preco'
        if "variacao_preco" in coluna:
            continue

        # Checando se a coluna é relacionada a preço e não é uma coluna de variação de preço
        if (
            "preco" in coluna and "variacao_preco" not in coluna
        ):  # Tratando strings vazias e nulas, substituindo vírgulas por pontos, e convertendo para float
            df[loja, coluna] = (
                df[loja, coluna]
                .replace("", np.nan)  # Trata strings vazias
                .dropna()  # Remove valores nulos
                .apply(lambda x: x.replace(",", ".") if isinstance(x, str) else x)
                .astype(float)
                .apply(lambda x: f"{x:.2f}".replace(".", ","))
            )

        # Checando se a coluna é relacionada a estoque
        elif "estoque" in coluna:
            df[loja, coluna] = df[loja, coluna].fillna(0).astype(int)
            df[loja, coluna] = df[loja, coluna].replace(0, np.nan).fillna("")

    # Reordenando as colunas após a modificação
    df = df.sort_index(axis=1)

    return df


# %%

dicionario = {
    "descricao": "descricao",
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
        "dados_concorrentes.csv", sep=";", encoding="utf-8-sig", na_rep="0", decimal=","
    )


if __name__ == "__main__":
    criar_excel_email()