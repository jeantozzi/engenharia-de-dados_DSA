-- Consulta simples
SELECT 
    loc.estado, 
    pro.categoria, 
    cli.tipo AS tipo_cliente, 
    tem.hora, 
    SUM(ven.resultado)
FROM
    schema3.fato_vendas AS ven
    INNER JOIN schema3.dim_produto AS pro USING(sk_produto) 
    INNER JOIN schema3.dim_cliente AS cli USING(sk_cliente) 
    INNER JOIN schema3.dim_localidade AS loc USING(sk_localidade)
    INNER JOIN schema3.dim_tempo AS tem USING(sk_tempo) 
GROUP BY loc.estado, pro.categoria, cli.tipo, tem.hora
ORDER BY loc.estado, pro.categoria, cli.tipo, tem.hora;

-- Criando a Materialized View
CREATE MATERIALIZED VIEW schema3.mv_relatorio AS
SELECT 
    loc.estado, 
    pro.categoria, 
    cli.tipo AS tipo_cliente, 
    tem.hora, 
    SUM(ven.resultado)
FROM
    schema3.fato_vendas AS ven
    INNER JOIN schema3.dim_produto AS pro USING(sk_produto) 
    INNER JOIN schema3.dim_cliente AS cli USING(sk_cliente) 
    INNER JOIN schema3.dim_localidade AS loc USING(sk_localidade)
    INNER JOIN schema3.dim_tempo AS tem USING(sk_tempo) 
GROUP BY loc.estado, pro.categoria, cli.tipo, tem.hora
ORDER BY loc.estado, pro.categoria, cli.tipo, tem.hora;

-- Comando do pg_cron para agendar a atualização da Materialized View todos os dias ao meio-dia
SELECT cron.schedule('REFRESH mv_relatorio', '0 12 * * *', 'REFRESH MATERIALIZED VIEW mv_relatorio;');