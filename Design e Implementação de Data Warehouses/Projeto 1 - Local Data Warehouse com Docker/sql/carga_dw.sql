-- Carregando os dados da dimensão tempo
INSERT INTO schema3.dim_tempo (ano, mes, dia, data_completa)
SELECT
	EXTRACT(YEAR FROM d)::INT, 
	EXTRACT(MONTH FROM d)::INT, 
	EXTRACT(DAY FROM d)::INT,
	d::DATE
FROM
	GENERATE_SERIES('2020-01-01'::DATE, '2024-12-31'::DATE, '1 day'::INTERVAL) AS d;

-- Carregando os dados da dimensão cliente
INSERT INTO schema3.dim_cliente (id_cliente, nome, tipo)
SELECT
	cli.id_cliente, 
	cli.nome_cliente, 
	tip.nome_tipo
FROM
	schema2.st_ft_clientes AS cli
	INNER JOIN schema2.st_ft_tipo_cliente AS tip USING(id_tipo);

-- Carregando os dados da dimensão produto
INSERT INTO schema3.dim_produto (id_produto, nome_produto, categoria, subcategoria)
SELECT
	pro.id_produto, 
	pro.nome_produto, 
	cat.nome_categoria, 
	sub.nome_subcategoria
FROM
	schema2.st_ft_produtos AS pro
	INNER JOIN schema2.st_ft_subcategorias AS sub USING(id_subcategoria) 
	INNER JOIN schema2.st_ft_categorias AS cat USING(id_categoria);

-- Carregando os dados da dimensão localidade
INSERT INTO schema3.dim_localidade (id_localidade, pais, regiao, estado, cidade)
SELECT
	loc.id_localidade, 
	loc.pais, 
	loc.regiao, 
	CASE
		WHEN cid.nome_cidade = 'Natal' THEN 'Rio Grande do Norte'
		WHEN cid.nome_cidade = 'Rio de Janeiro' THEN 'Rio de Janeiro'
		WHEN cid.nome_cidade = 'Belo Horizonte' THEN 'Minas Gerais'
		WHEN cid.nome_cidade = 'Salvador' THEN 'Bahia'
		WHEN cid.nome_cidade = 'Blumenau' THEN 'Santa Catarina'
		WHEN cid.nome_cidade = 'Curitiba' THEN 'Paraná'
		WHEN cid.nome_cidade = 'Fortaleza' THEN 'Ceará'
		WHEN cid.nome_cidade = 'Recife' THEN 'Pernambuco'
		WHEN cid.nome_cidade = 'Porto Alegre' THEN 'Rio Grande do Sul'
		WHEN cid.nome_cidade = 'Manaus' THEN 'Amazonas'
	END estado, 
	cid.nome_cidade
FROM
	schema2.st_ft_localidades AS loc
	INNER JOIN schema2.st_ft_cidades AS cid USING(id_cidade);

-- Carregando os dados da tabela fato de vendas
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
	INNER JOIN schema3.dim_tempo AS dw_tem ON (st_ven.data_transacao = dw_tem.data_completa)
	INNER JOIN schema3.dim_produto AS dw_pro ON (st_pro.id_produto = dw_pro.id_produto)
	INNER JOIN schema3.dim_localidade AS dw_loc ON (st_loc.id_localidade = dw_loc.id_localidade)
	INNER JOIN schema3.dim_cliente AS dw_cli ON (st_cli.id_cliente = dw_cli.id_cliente)
GROUP BY 
	dw_pro.sk_produto, dw_cli.sk_cliente, dw_loc.sk_localidade, dw_tem.sk_tempo;