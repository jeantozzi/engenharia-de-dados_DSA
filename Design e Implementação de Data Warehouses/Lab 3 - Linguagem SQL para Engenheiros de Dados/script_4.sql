# Manipulação de Dados e Objetos

SELECT *
FROM lab3."TB_CATEGORIA";

INSERT INTO lab3."TB_CATEGORIA"("ID_Categoria", "Codigo_Categoria", "Nome_Categoria")
VALUES (5, 1005, 'Eletronicos');

COMMIT;

UPDATE lab3."TB_CATEGORIA"
SET "Codigo_Categoria" = 5000
WHERE "ID_Categoria" = 5;

DELETE FROM lab3."TB_CATEGORIA"
WHERE "ID_Categoria" = 5;

SELECT "Nome_Subcategoria", ROUND(CAST(AVG("Peso_Produto") as numeric), 2) AS "Media_Peso_Produto"
FROM lab3."TB_PRODUTO", lab3."TB_SUBCATEGORIA"
WHERE lab3."TB_PRODUTO"."ID_Subcategoria" = lab3."TB_SUBCATEGORIA"."ID_Subcategoria"
  AND "Peso_Produto" IS NOT NULL
GROUP BY "Nome_Subcategoria"
HAVING AVG("Peso_Produto") >= 100
ORDER BY "Nome_Subcategoria";

SELECT "Nome_Subcategoria", ROUND(CAST(AVG("Peso_Produto") as numeric), 2) AS "Media_Peso_Produto"
INTO TABLE lab3."TB_REPORT1"
FROM lab3."TB_PRODUTO", lab3."TB_SUBCATEGORIA"
WHERE lab3."TB_PRODUTO"."ID_Subcategoria" = lab3."TB_SUBCATEGORIA"."ID_Subcategoria"
  AND "Peso_Produto" IS NOT NULL
GROUP BY "Nome_Subcategoria"
HAVING AVG("Peso_Produto") >= 100
ORDER BY "Nome_Subcategoria";

SELECT * 
FROM lab3."TB_REPORT1";

CREATE TABLE lab3."TB_REPORT2" AS
SELECT "Nome_Subcategoria", ROUND(CAST(AVG("Peso_Produto") as numeric), 2) AS "Media_Peso_Produto"
FROM lab3."TB_PRODUTO", lab3."TB_SUBCATEGORIA"
WHERE lab3."TB_PRODUTO"."ID_Subcategoria" = lab3."TB_SUBCATEGORIA"."ID_Subcategoria"
  AND "Peso_Produto" IS NOT NULL
GROUP BY "Nome_Subcategoria"
HAVING AVG("Peso_Produto") >= 100
ORDER BY "Nome_Subcategoria";

SELECT * 
FROM lab3."TB_REPORT2";

CREATE VIEW lab3."VW_REPORT3" AS 
SELECT "Nome_Subcategoria", ROUND(CAST(AVG("Peso_Produto") as numeric), 2) AS "Media_Peso_Produto"
FROM lab3."TB_PRODUTO", lab3."TB_SUBCATEGORIA"
WHERE lab3."TB_PRODUTO"."ID_Subcategoria" = lab3."TB_SUBCATEGORIA"."ID_Subcategoria"
  AND "Peso_Produto" IS NOT NULL
GROUP BY "Nome_Subcategoria"
HAVING AVG("Peso_Produto") >= 100
ORDER BY "Nome_Subcategoria";

SELECT * 
FROM lab3."VW_REPORT3";

CREATE MATERIALIZED VIEW lab3."MV_REPORT4" AS 
SELECT "Nome_Subcategoria", ROUND(CAST(AVG("Peso_Produto") as numeric), 2) AS "Media_Peso_Produto"
FROM lab3."TB_PRODUTO", lab3."TB_SUBCATEGORIA"
WHERE lab3."TB_PRODUTO"."ID_Subcategoria" = lab3."TB_SUBCATEGORIA"."ID_Subcategoria"
  AND "Peso_Produto" IS NOT NULL
GROUP BY "Nome_Subcategoria"
HAVING AVG("Peso_Produto") >= 100
ORDER BY "Nome_Subcategoria";

SELECT * 
FROM lab3."MV_REPORT4";

SELECT * 
FROM lab3."TB_VENDAS";

SELECT DISTINCT EXTRACT(YEAR FROM lab3."TB_VENDAS"."Data_Venda")
FROM lab3."TB_VENDAS";

SELECT * 
FROM lab3."TB_VENDAS"
WHERE EXTRACT(YEAR FROM lab3."TB_VENDAS"."Data_Venda") < 2012;

SELECT COUNT(*) 
FROM lab3."TB_VENDAS"
WHERE EXTRACT(YEAR FROM lab3."TB_VENDAS"."Data_Venda") < 2012;

CREATE TABLE lab3."TB_VENDAS_BKP" (
   "Codigo_Venda" character varying(50),
    "ID_Produto" integer,
    "Quantidade_Venda" integer,
    "Preco_Unitario" double precision,
    "Custo_Frete" double precision,
    "Data_Venda" date
);

WITH movendo_linhas AS (
   DELETE FROM lab3."TB_VENDAS"
   WHERE EXTRACT(YEAR FROM lab3."TB_VENDAS"."Data_Venda") < 2012
   RETURNING *
)
INSERT INTO lab3."TB_VENDAS_BKP" (SELECT * FROM movendo_linhas);

SELECT COUNT(*) 
FROM lab3."TB_VENDAS";

SELECT * 
FROM lab3."TB_VENDAS"
WHERE EXTRACT(YEAR FROM lab3."TB_VENDAS"."Data_Venda") < 2012;

SELECT * 
FROM lab3."TB_VENDAS_BKP";








