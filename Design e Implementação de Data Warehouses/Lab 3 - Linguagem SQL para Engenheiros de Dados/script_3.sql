# SELECT

SELECT *
FROM lab3."TB_CATEGORIA";

SELECT *
FROM lab3."TB_CATEGORIA"
WHERE "ID_Categoria" > 2;

SELECT "Nome_Categoria", "Codigo_Categoria"
FROM lab3."TB_CATEGORIA"
WHERE "ID_Categoria" > 2;

SELECT "Nome_Categoria", "Codigo_Categoria"
FROM lab3."TB_CATEGORIA"
WHERE "Nome_Categoria" LIKE 'C%';

SELECT "Nome_Categoria", "Codigo_Categoria"
FROM lab3."TB_CATEGORIA"
WHERE "Nome_Categoria" LIKE 'C%' AND "Codigo_Categoria" < 1003;

SELECT "Nome_Categoria", "Codigo_Categoria"
FROM lab3."TB_CATEGORIA"
WHERE "Nome_Categoria" LIKE 'C%' OR "Codigo_Categoria" < 1003;

SELECT "Nome_Categoria", "Codigo_Categoria", CURRENT_TIMESTAMP AS "Hora_Consulta"
FROM lab3."TB_CATEGORIA"
WHERE "Nome_Categoria" LIKE 'C%' OR "Codigo_Categoria" < 1003;

SELECT *
FROM lab3."TB_PRODUTO";

SELECT *
FROM lab3."TB_PRODUTO"
WHERE "Peso_Produto" IS NOT NULL;

SELECT COUNT(*)
FROM lab3."TB_PRODUTO"
WHERE "Peso_Produto" IS NOT NULL;

SELECT MAX("Preco_Produto"), MIN("Preco_Produto")
FROM lab3."TB_PRODUTO"
WHERE "Peso_Produto" IS NOT NULL;

SELECT "Nome_Subcategoria", "Peso_Produto"
FROM lab3."TB_PRODUTO", lab3."TB_SUBCATEGORIA"
WHERE lab3."TB_PRODUTO"."ID_Subcategoria" = lab3."TB_SUBCATEGORIA"."ID_Subcategoria"

SELECT "Nome_Subcategoria", "Peso_Produto"
FROM lab3."TB_PRODUTO", lab3."TB_SUBCATEGORIA"
WHERE lab3."TB_PRODUTO"."ID_Subcategoria" = lab3."TB_SUBCATEGORIA"."ID_Subcategoria"
  AND "Peso_Produto" IS NOT NULL;

SELECT "Nome_Subcategoria", AVG("Peso_Produto")
FROM lab3."TB_PRODUTO", lab3."TB_SUBCATEGORIA"
WHERE lab3."TB_PRODUTO"."ID_Subcategoria" = lab3."TB_SUBCATEGORIA"."ID_Subcategoria"
  AND "Peso_Produto" IS NOT NULL
GROUP BY "Nome_Subcategoria";

SELECT "Nome_Subcategoria", AVG("Peso_Produto") AS "Media_Peso_Produto"
FROM lab3."TB_PRODUTO", lab3."TB_SUBCATEGORIA"
WHERE lab3."TB_PRODUTO"."ID_Subcategoria" = lab3."TB_SUBCATEGORIA"."ID_Subcategoria"
  AND "Peso_Produto" IS NOT NULL
GROUP BY "Nome_Subcategoria"
ORDER BY "Nome_Subcategoria";

SELECT "Nome_Subcategoria", ROUND(CAST(AVG("Peso_Produto") as numeric), 2) AS "Media_Peso_Produto"
FROM lab3."TB_PRODUTO", lab3."TB_SUBCATEGORIA"
WHERE lab3."TB_PRODUTO"."ID_Subcategoria" = lab3."TB_SUBCATEGORIA"."ID_Subcategoria"
  AND "Peso_Produto" IS NOT NULL
GROUP BY "Nome_Subcategoria"
ORDER BY "Nome_Subcategoria";

SELECT "Nome_Subcategoria", ROUND(CAST(AVG("Peso_Produto") as numeric), 2) AS "Media_Peso_Produto"
FROM lab3."TB_PRODUTO", lab3."TB_SUBCATEGORIA"
WHERE lab3."TB_PRODUTO"."ID_Subcategoria" = lab3."TB_SUBCATEGORIA"."ID_Subcategoria"
  AND "Peso_Produto" IS NOT NULL
GROUP BY "Nome_Subcategoria"
HAVING AVG("Peso_Produto") >= 100
ORDER BY "Nome_Subcategoria";

SELECT "Nome_Categoria", ROUND(CAST(AVG("Peso_Produto") as numeric), 2) AS "Media_Peso_Produto"
FROM lab3."TB_PRODUTO", lab3."TB_SUBCATEGORIA", lab3."TB_CATEGORIA"
WHERE lab3."TB_PRODUTO"."ID_Subcategoria" = lab3."TB_SUBCATEGORIA"."ID_Subcategoria"
  AND lab3."TB_SUBCATEGORIA"."ID_Categoria" = lab3."TB_CATEGORIA"."ID_Categoria"
  AND "Peso_Produto" IS NOT NULL
GROUP BY "Nome_Categoria"
HAVING AVG("Peso_Produto") >= 20
ORDER BY "Nome_Categoria";

SELECT "Nome_Categoria", 
       ROUND(CAST(AVG("Peso_Produto") as numeric), 2) AS "Media_Peso_Produto"
FROM lab3."TB_PRODUTO" TAB_A, 
     lab3."TB_SUBCATEGORIA" TAB_B, 
     lab3."TB_CATEGORIA" TAB_C
WHERE TAB_A."ID_Subcategoria" = TAB_B."ID_Subcategoria"
  AND TAB_B."ID_Categoria" = TAB_C."ID_Categoria"
  AND "Peso_Produto" IS NOT NULL
GROUP BY "Nome_Categoria"
HAVING AVG("Peso_Produto") >= 20
ORDER BY "Nome_Categoria";








