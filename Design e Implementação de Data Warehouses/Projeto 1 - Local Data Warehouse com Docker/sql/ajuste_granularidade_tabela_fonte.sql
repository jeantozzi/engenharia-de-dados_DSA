-- Incluindo mais uma coluna na dimensão tempo
ALTER TABLE IF EXISTS schema3.dim_tempo
	ADD COLUMN hora text;

-- Limpando as tabelas da hierarquia para reexecutar a carga
TRUNCATE TABLE schema3.fato_vendas;
TRUNCATE TABLE schema3.dim_tempo CASCADE;

-- Carregando novamente a dimensão tempo
INSERT INTO schema3.dim_tempo (ano, mes, dia, hora, data_completa)
SELECT
	EXTRACT(YEAR FROM d)::INT, 
	EXTRACT(MONTH FROM d)::INT, 
	EXTRACT(DAY FROM d)::INT,
	LPAD(EXTRACT(HOUR FROM d)::INTEGER::TEXT, 2, '0'), 
	d::DATE
FROM
	GENERATE_SERIES('2020-01-01'::DATE, '2024-12-31'::DATE, '1 hour'::INTERVAL) AS d;

-- Carregando novamente a tabela fato de vendas
INSERT INTO schema3.fato_vendas (
	sk_produto, 
	sk_cliente, 
	sk_localidade, 
	sk_tempo, 
	quantidade, 
	preco_venda, 
	custo_produto, 
	receita_vendas)
SELECT 
	dw_pro.sk_produto,
	dw_cli.sk_cliente,
	dw_loc.sk_localidade,
	dw_tem.sk_tempo, 
	SUM(st_ven.quantidade) AS quantidade, 
	SUM(st_ven.preco_venda) AS preco_venda, 
	SUM(st_ven.custo_produto) AS custo_produto, 
	SUM(ROUND((CAST(st_ven.quantidade AS NUMERIC) * CAST(st_ven.preco_venda AS NUMERIC)), 2)) AS receita_vendas
FROM 
	schema2.st_ft_vendas AS st_ven
	INNER JOIN schema2.st_ft_clientes AS st_cli ON (st_cli.id_cliente = st_ven.id_cliente)
	INNER JOIN schema2.st_ft_localidades AS st_loc ON (st_loc.id_localidade = st_ven.id_localizacao)
	INNER JOIN schema2.st_ft_produtos AS st_pro ON (st_pro.id_produto = st_ven.id_produto)
	INNER JOIN schema3.dim_tempo AS dw_tem ON (
		TO_CHAR(st_ven.data_transacao, 'YYYY-MM-DD') = TO_CHAR(dw_tem.data_completa, 'YYYY-MM-DD') AND
		TO_CHAR(st_ven.data_transacao, 'HH24') = dw_tem.hora)
	INNER JOIN schema3.dim_produto AS dw_pro ON (st_pro.id_produto = dw_pro.id_produto)
	INNER JOIN schema3.dim_localidade AS dw_loc ON (st_loc.id_localidade = dw_loc.id_localidade)
	INNER JOIN schema3.dim_cliente AS dw_cli ON (st_cli.id_cliente = dw_cli.id_cliente)
GROUP BY 
	dw_pro.sk_produto, dw_cli.sk_cliente, dw_loc.sk_localidade, dw_tem.sk_tempo;