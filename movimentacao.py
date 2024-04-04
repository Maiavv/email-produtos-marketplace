import pandas as pd
import sqlite3

query = """--sql
WITH Movimentacao AS (
  SELECT
    p.id_produto,
    p.nome,
    p.marca,
    p.ean,
    p.categoria,
    pd.id_loja,
    DATE(pd.data) AS data_somente,
    CASE 
      WHEN DATE(pd.data) = '2024-03-04' THEN pd.available_quantity 
      - LAG(pd.available_quantity, 1) OVER (PARTITION BY pd.id_produto ORDER BY pd.data)
    END AS mov_03_04,
    CASE 
      WHEN DATE(pd.data) = '2024-03-05' THEN pd.available_quantity 
      - LAG(pd.available_quantity, 1) OVER (PARTITION BY pd.id_produto ORDER BY pd.data)
    END AS mov_04_05,
    CASE 
      WHEN DATE(pd.data) = '2024-03-06' THEN pd.available_quantity 
      - LAG(pd.available_quantity, 1) OVER (PARTITION BY pd.id_produto ORDER BY pd.data)
    END AS mov_05_06,
    CASE 
      WHEN DATE(pd.data) = '2024-03-07' THEN pd.available_quantity 
      - LAG(pd.available_quantity, 1) OVER (PARTITION BY pd.id_produto ORDER BY pd.data)
    END AS mov_06_07,
    CASE 
      WHEN DATE(pd.data) = '2024-03-08' THEN pd.available_quantity 
      - LAG(pd.available_quantity, 1) OVER (PARTITION BY pd.id_produto ORDER BY pd.data)
    END AS mov_07_08,
    CASE 
      WHEN DATE(pd.data) = '2024-03-09' THEN pd.available_quantity 
      - LAG(pd.available_quantity, 1) OVER (PARTITION BY pd.id_produto ORDER BY pd.data)
    END AS mov_08_09
  FROM PrecosDiarios pd
  JOIN Produtos p ON pd.id_produto = p.id_produto
  WHERE DATE(pd.data) BETWEEN '2024-03-03' AND '2024-03-10'
    AND pd.id_loja = 3
)

SELECT
  nome,
  marca,
  ean,
  categoria,
  id_loja,
  MAX(mov_03_04) AS mov_03_04,
  MAX(mov_04_05) AS mov_04_05,
  MAX(mov_05_06) AS mov_05_06,
  MAX(mov_06_07) AS mov_06_07,
  MAX(mov_07_08) AS mov_07_08,
  MAX(mov_08_09) AS mov_08_09
FROM Movimentacao
GROUP BY id_produto, id_loja
ORDER BY id_produto;
"""

with sqlite3.connect("dados_concorrentes.db") as conn:
    df_concorrentes = pd.read_sql_query(query, conn)

df_concorrentes.to_excel("movimentacao_diaria_obramax.xlsx", index=False)
