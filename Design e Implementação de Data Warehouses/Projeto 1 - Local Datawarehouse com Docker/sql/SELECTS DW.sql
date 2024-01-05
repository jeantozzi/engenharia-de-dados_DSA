-- Criando a dimensão cliente
SELECT 
	cli.id_cliente, 
	cli.nome_cliente, 
	tip.nome_tipo 
FROM 
	schema2.st_ft_clientes AS cli 
	INNER JOIN schema2.st_ft_tipo_cliente AS tip USING(id_tipo);

-- Criando a dimensão produto
SELECT
	pro.id_produto,
	pro.nome_produto,
	cat.nome_categoria,
	sub.nome_subcategoria
FROM
	schema2.st_ft_produtos AS pro
	INNER JOIN schema2.st_ft_subcategorias AS sub USING(id_subcategoria)
	INNER JOIN schema2.st_ft_categorias AS cat USING(id_categoria);

-- Criando a dimensão localidade
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
	END AS estado,
	cid.nome_cidade
FROM schema2.st_ft_localidades AS loc
	INNER JOIN schema2.st_ft_cidades AS cid USING(id_cidade);

-- Criando a dimensão tempo
SELECT
    EXTRACT(YEAR FROM d)::INT AS ano,
    EXTRACT(MONTH FROM d)::INT AS mes,
    EXTRACT(DAY FROM d)::INT AS dia,
    d::DATE AS data
FROM
    GENERATE_SERIES('2020-01-01'::DATE, '2024-12-31'::DATE, '1 day'::INTERVAL) AS d


-- Criando a tabela fato venda