import pyodbc
import pandas as pd
import numpy as np
import os
import re
import pyarrow

from sqlalchemy import create_engine, select, Table, MetaData, func, text
import sqlite3


def obter_produtos_balaroti():
    query = """--sql
    SELECT 
        CADPROD.pr_codbarra, 
        CADPROD.PR_DESCRICAO, 
        CADPROD.PR_MARCA, 
        CADPROD.PR_CODSEQ,
        listapre.LP_PRECOBASE, 
        listapre.LP_PRECOVENDA, 
        CASE
            WHEN lstoferta.LO_DESCONTO IS NOT NULL AND NOW() BETWEEN lstoferta.LO_DATAINICIO AND lstoferta.LO_DATAFINAL THEN
                listapre.LP_PRECOVENDA * (1 - lstoferta.LO_DESCONTO / 100)
            ELSE NULL
        END AS preco_desconto,
        listapre.LP_LUCROBRUTO, --caso tenha desconto (lp_precovenda - custo da mercadoria - impostos)
        estoque_atu.EA_SALATU, 
        grupo.GR_DESCRICAO
    FROM listapre
    INNER JOIN CADPROD ON listapre.lp_idproduto = CADPROD.pr_idproduto
    INNER JOIN (select sum(EA_SALATU) as EA_SALATU, EA_IDCODSEQ from ESTOQUE_ATUAL GROUP BY EA_IDCODSEQ) as estoque_atu ON CADPROD.PR_CODSEQ = estoque_atu.EA_IDCODSEQ
    INNER JOIN grupo ON CADPROD.PR_IDGRUPO = grupo.GR_IDGRUPO
    LEFT JOIN lstoferta ON CADPROD.pr_idproduto = lstoferta.lo_idproduto 
        AND lstoferta.LO_IDLOJA = 10
        AND NOW() BETWEEN lstoferta.LO_DATAINICIO AND lstoferta.LO_DATAFINAL
    WHERE CADPROD.PR_FORADELINHA = 'N' 
        AND listapre.LP_IDLISTA = 6;
    """

    with pyodbc.connect("DSN=BDMTRIZ") as conn:
        cursor = conn.cursor()
        res = cursor.execute(query)
        df = pd.DataFrame(
            map(list, res.fetchall()), columns=[c[0] for c in res.description]
        )
    df = df.convert_dtypes()
    print(df.columns)

    df_agregado = df.rename(
        columns={
            "PR_CODBARRA": "ean",
            "PR_CODSEQ": "sku",
            "PR_DESCRICAO": "descricao",
            "GR_DESCRICAO": "grupo",
            "PR_MARCA": "marca",
            "LP_PRECOBASE": "preco_base",
            "LP_PRECOVENDA": "preco_venda",
            "PRECO_DESCONTO": "preco_desconto",
            "LP_LUCROBRUTO": "lucro_bruto",
            "EA_SALATU": "estoque_atual",
        },
    )

    df_agregado = df_agregado.astype(
        {
            "ean": str,
            "sku": int,
            "descricao": str,
            "marca": str,
            "grupo": str,
            "preco_base": float,
            "preco_venda": float,
            "preco_desconto": float,
            "lucro_bruto": float,
            "estoque_atual": int,
        }
    )

    return df_agregado


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


def obter_produtos_concorrentes():
    """ """
    query = """--sql
    SELECT ean, preco, marca, nome, list_price, id_loja, available_quantity, categoria, PrecosDiarios.data
    FROM Produtos
    JOIN PrecosDiarios ON Produtos.id_produto = PrecosDiarios.id_produto or Produtos.ean = PrecosDiarios.id_produto
    """
    with sqlite3.connect("dados_concorrentes.db") as conn:
        df_concorrentes = pd.read_sql_query(query, conn)

    # tirando a primeira linha
    df_concorrentes["ean"] = df_concorrentes["ean"].dropna()

    df_concorrentes["categoria"] = (
        df_concorrentes["categoria"]
        .replace("/", "", regex=True)
        .replace("", "666", regex=True)
    )

    df_concorrentes = df_concorrentes.astype(
        {
            "ean": str,
            "preco": float,
            "marca": str,
            "list_price": float,
            "id_loja": int,
            "available_quantity": int,
            "categoria": str,   
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
    df_bala = df_balaroti.set_index("ean")
    df_bala.rename(
        {
            "sku": ("Balaroti", "sku"),
            "descricao": ("Balaroti", "descricao"),
            "marca": ("Balaroti", "marca"),
            "grupo": ("Balaroti", "grupo"),
            "preco_base": ("Balaroti", "preco_base"),
            "preco_venda": ("Balaroti", "preco_venda"),
            "preco_desconto": ("Balaroti", "preco_desconto"),
            "lucro_bruto": ("Balaroti", "lucro_bruto"),
            "estoque_atual": ("Balaroti", "estoque"),
        },
        axis=1,
        inplace=True,
    )
    df_bala = df_bala.set_axis(pd.MultiIndex.from_tuples(df_bala.columns), axis=1)
    unidos = df_bala.join(df_c, how="outer")

    unidos["Balaroti", "descricao"] = (
        unidos["Balaroti", "descricao"]
        .pipe(lambda s: s.where(s.notna(), unidos[1, "nome"]))
        .pipe(lambda s: s.where(s.notna(), unidos[2, "nome"]))
        .pipe(lambda s: s.where(s.notna(), unidos[3, "nome"]))
        .pipe(lambda s: s.where(s.notna(), unidos[40603, "nome"]))
    )

    unidos["Balaroti", "marca"] = (
        unidos["Balaroti", "marca"]
        .pipe(lambda s: s.where(s.notna(), unidos[1, "marca"]))
        .pipe(lambda s: s.where(s.notna(), unidos[2, "marca"]))
        .pipe(lambda s: s.where(s.notna(), unidos[3, "marca"]))
        .pipe(lambda s: s.where(s.notna(), unidos[40603, "marca"]))
    )

    unidos["Balaroti", "grupo"] = (
        unidos["Balaroti", "grupo"]
        .pipe(lambda s: s.where(s.notna(), unidos[1, "categoria"]))
        .pipe(lambda s: s.where(s.notna(), unidos[2, "categoria"]))
        .pipe(lambda s: s.where(s.notna(), unidos[3, "categoria"]))
        .pipe(lambda s: s.where(s.notna(), unidos[40603, "categoria"]))
    )

    colunas_para_desc = [
        (loja, coluna) for loja, coluna in unidos.columns if coluna == "nome"
    ]

    colunas_para_marca = [
        (loja, coluna) for loja, coluna in unidos.columns if coluna == "marca"
    ]

    colunas_para_categ = [
        (loja, coluna) for loja, coluna in unidos.columns if coluna == "categoria"
    ]

    colunas_para_marca = colunas_para_marca[1:]

    unidos = unidos.drop(columns=colunas_para_desc)
    unidos = unidos.drop(columns=colunas_para_marca)
    unidos = unidos.drop(columns=colunas_para_categ)

    return unidos


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


def obtem_ranqueamento(caminho_top_2500: str, df_lojas) -> pd.DataFrame:
    padrao = r"top_1500\\(.*?)\\top_1500_dia_(.*?).csv"
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


# pegando o df para o formatar_colunas


def formatar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    for loja, coluna in df.columns:
        if "lucro_bruto" in coluna:
            df[loja, coluna] = (
                df[loja, coluna]
                .apply(lambda x: x.replace(",", ".") if isinstance(x, str) else x)
                .astype(float)
                .apply(lambda x: f"{x:.2f}%".replace(".", ","))
                .fillna("")
            )

    return df


def adicionar_ranqueamento_por_loja(
    df: pd.DataFrame, df_ranqueamento: pd.DataFrame
) -> pd.DataFrame:
    df.index = pd.to_numeric(df.index, errors="coerce").astype("Int64")

    df_ranqueamento["ean"] = pd.to_numeric(
        df_ranqueamento["ean"].replace("", np.nan), errors="coerce"
    ).astype("Int64")

    for loja in df_ranqueamento["loja"].unique():
        df_ranqueamento_loja = df_ranqueamento[df_ranqueamento["loja"] == loja]

        ranqueamento_dict = df_ranqueamento_loja.set_index("ean")[
            "ranqueamento"
        ].to_dict()

        df[(loja, "ranqueamento")] = df.index.map(ranqueamento_dict.get)

    return df


# %%

dicionario = {
    "sku": "sku",
    "descricao": "descricao",
    "marca": "marca",
    "grupo": "grupo",
    "preco_base": "preco_compra",
    "preco_venda": "preco_venda",
    "preco_desconto": "preco_desconto",
    "lucro_bruto": "lucro_bruto",
    "estoque": "estoque",
    "available_quantity": "estoque_site",
    "list_price": "preco_site",
    "preco": "preco",
    "variacao_preco_%": "variacao_preco",
    "variacao_estoque": "variacao_estoque",
}

categorico = pd.CategoricalDtype(
    [
        "sku",
        "descricao",
        "marca",
        "grupo",
        "preco_compra",
        "preco_venda",
        "preco_desconto",
        "lucro_bruto",
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
    """Cria um arquivo CSV com os dados dos concorrentes e retorna um DataFrame com os dados formatados."""

    dia_hoje = pd.Timestamp.today().strftime("%d-%m")
    caminho = r"Z:\Vitor\dados_concorrentes\top_1500"
    df_lojas = obtem_mapeamento_lojas()

    df_balaroti = obter_produtos_balaroti()
    df_concorrentes = obter_produtos_concorrentes()
    dicionario_lojas = obtem_mapeamento_lojas()
    df_ranqueamento = obtem_ranqueamento(caminho, df_lojas)

    df_unidos = combinar_dados(df_balaroti, df_concorrentes)
    df_unidos = calcular_variacao_preco_estoque(df_unidos, dicionario_lojas)

    df_unidos.rename(dicionario, axis=1, inplace=True, level=1)

    df_unidos.columns = df_unidos.columns.set_levels(
        df_unidos.columns.levels[1].astype(categorico), level=1
    )

    df_unidos = formatar_colunas(df_unidos)

    df_final = df_unidos.sort_index(axis=1)

    df_final.replace("nan%", "", regex=True, inplace=True)
    df_final = adicionar_ranqueamento_por_loja(df_final, df_ranqueamento)

    df_final.index = df_final.index.astype(str)

    df_final = df_final.iloc[3:]

    df_final = df_final.drop("7890942492949", errors="ignore")

    df_final.fillna("").replace("nan%", "", regex=True).to_excel(
        rf"Z:\\Vitor\\dados_concorrentes\\dados_produtos_concorrentes\\dados_concorrentes_{dia_hoje}.xlsx",
        na_rep="0",
        engine="openpyxl",
    )


# criar_excel_email()
