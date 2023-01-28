# Criação das tabelas

CREATE TABLE lab3."TB_CATEGORIA"
(
    "ID_Categoria" integer,
    "Codigo_Categoria" integer,
    "Nome_Categoria" character varying(15)
);

CREATE TABLE lab3."TB_SUBCATEGORIA"
(
    "ID_Subcategoria" integer,
    "Nome_Subcategoria" character varying(50),
    "ID_Categoria" integer
);

CREATE TABLE lab3."TB_PRODUTO"
(
    "ID_Produto" integer,
    "Nome_Produto" character varying(100),
    "Preco_Produto" double precision,
    "Tamanho_Produto" character varying(5),
    "Peso_Produto" double precision,
    "Modelo" character varying(20),
    "Status" character varying(15),
    "ID_Subcategoria" integer
);

CREATE TABLE lab3."TB_PRODUTO"
(
    "ID_Produto" integer,
    "Nome_Produto" character varying(100),
    "Preco_Produto" double precision,
    "Tamanho_Produto" character varying(5),
    "Peso_Produto" double precision,
    "Modelo" character varying(50),
    "Status" character varying(15),
    "ID_Subcategoria" integer
);

CREATE TABLE lab3."TB_VENDAS"
(
    "Codigo_Venda" character varying(50),
    "ID_Produto" integer,
    "Quantidade_Venda" integer,
    "Preco_Unitario" double precision,
    "Custo_Frete" double precision,
    "Data_Venda" date
);
