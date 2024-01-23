SELECT 
    p.id_proco,
    p.nome,
    ABS(MAX(pd.available_quantity) - MIN(pd.available_quantity)) AS variacao_estoque
FROM 
    PrecosDiarios pd
INNER JOIN 
    Produtos p ON pd.id_produto = p.id_produto
WHERE 
    pd.data BETWEEN (SELECT MAX(data) FROM PrecosDiarios) AND (SELECT MAX(data) FROM PrecosDiarios, -1 DAY)
GROUP BY 
    pd.id_produto
ORDER BY 
    variacao_estoque DESC;
