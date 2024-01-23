SELECT 
    p.id_proco,
    p.nome,
    MAX(pd.preco) - MIN(pd.preco) AS variacao_preco
FROM 
    PrecosDiarios pd
INNER JOIN 
    Produtos p ON pd.id_produto = p.id_produto
WHERE 
    pd.data BETWEEN (SELECT MAX(data) FROM PrecosDiarios) AND (SELECT MAX(data) FROM PrecosDiarios, -1 DAY)
GROUP BY 
    pd.id_produto
ORDER BY 
    variacao_preco DESC;
