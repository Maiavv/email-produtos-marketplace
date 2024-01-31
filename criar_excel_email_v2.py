import sqlite3
import pandas as pd
import pyodbc


def obter_produtos_balaroti():
    query = """--sql
    SELECT CADPROD.pr_codbarra, CADPROD.PR_DESCRICAO, CADPROD.PR_MARCA, listapre.LP_PRECOBASE, listapre.LP_PRECOVENDA, ESTOQUE_ATUAL.EA_SALATU, ESTOQUE_ATUAL.EA_IDLOJA, grupo.GR_DESCRICAO
    FROM listapre
    INNER JOIN CADPROD ON listapre.lp_idproduto = CADPROD.pr_idproduto
    INNER JOIN ESTOQUE_ATUAL ON CADPROD.PR_CODSEQ = ESTOQUE_ATUAL.EA_IDCODSEQ
    INNER JOIN grupo ON CADPROD.PR_IDGRUPO = grupo.GR_IDGRUPO
    WHERE CADPROD.PR_FORADELINHA = 'N' AND listapre.LP_IDLISTA = 1 AND CADPROD.pr_codbarra>0 AND FLOOR(LOG10(CADPROD.pr_codbarra))+1 >= 4;
    """

    with pyodbc.connect("DSN=BDMTRIZ") as conn:
        cursor = conn.cursor()
        res = cursor.execute(query)
        df = pd.DataFrame(
            map(list, res.fetchall()), columns=[c[0] for c in res.description]
        )
    df = df.convert_dtypes()
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
