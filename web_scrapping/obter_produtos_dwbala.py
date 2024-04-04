import pandas as pd
import os
from sqlalchemy import create_engine, select, Table, MetaData, func, text

host = "pgsql.balaroti.local"
port = os.environ.get("dw_bala_port")
dbname = "dwbala_olap"
username = os.environ.get('dw_bala_username')
password = os.environ.get('dw_bala_password')


def retira_dados_bala(host, port, dbname, username, password):
    engine = create_engine(f"postgresql://{username}:{password}@{host}:{port}/{dbname}")

    metadata = MetaData()
    venda_produto = Table("tableau_vendaproduto", metadata, autoload_with=engine)
    produto = Table("tableau_produto", metadata, autoload_with=engine)
    estoque = Table("tableau_estoqueproduto", metadata, autoload_with=engine)
    familia = Table("dim_familia", metadata, autoload_with=engine)

    cte_estoque = (
        select(
            estoque.c.es_fk_produto.label("SKU"),
            func.max(estoque.c.es_saldodisponivel).label("Estoque"),
            func.max(estoque.c.es_customedio).label("CMV"),
        )
        .where(
            estoque.c.es_data == select(func.max(estoque.c.es_data)).scalar_subquery()
        )
        .group_by(estoque.c.es_fk_produto)
        .cte("cte_estoque")
    )

    cte_venda_produto = (
        select(
            venda_produto.c.vp_fk_produto.label("SKU"),
            func.sum(venda_produto.c.vp_venda_bruta).label("vp_venda_bruta"),
            func.sum(venda_produto.c.vp_quantidade).label("quantidade_vendida"),
        )
        .where(
            venda_produto.c.vp_data > text("'2023-09-01'"),
            venda_produto.c.vp_venda_bruta > 0
        )
        .group_by(venda_produto.c.vp_fk_produto)
        .cte("cte_cmv")
    )

    cte_familia = (
        select(familia.c.codproduto.label("SKU"), familia.c.idgrupo.label("grupo"))
        .group_by(familia.c.codproduto, familia.c.idgrupo)
        .cte("cte_familia")
    )

    query = (
        select(
            produto.c.pr_idproduto.label("SKU"),
            produto.c.pr_codbarra.label("EAN"),
            produto.c.pr_descricao.label("Produto"),
            cte_venda_produto.c.vp_venda_bruta.label("Preço de Venda"),
            cte_estoque.c.CMV,
            cte_venda_produto.c.quantidade_vendida.label("Quantidade Vendida"),
            cte_familia.c.grupo.label("Grupo"),
            cte_estoque.c.Estoque,
        )
        .select_from(produto)
        .join(cte_estoque, cte_estoque.c.SKU == produto.c.pr_idproduto)
        .join(cte_venda_produto, cte_venda_produto.c.SKU == produto.c.pr_idproduto)
        .join(cte_familia, cte_familia.c.SKU == produto.c.pr_idproduto)
        .where(produto.c.pr_foradelinha != "S")
    )

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    df["Preço do Produto"] = df["Preço de Venda"] / df["Quantidade Vendida"]
    return df
