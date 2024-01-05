-- Tabela Dimensão Cliente
CREATE TABLE schema3.dim_cliente (
  sk_cliente SERIAL PRIMARY KEY,
  id_cliente INT NOT NULL,
  nome VARCHAR(50) NOT NULL,
  tipo VARCHAR(50) NOT NULL
);

-- Tabela Dimensão Produto
CREATE TABLE schema3.dim_produto (
  sk_produto SERIAL PRIMARY KEY,
  id_produto INT NOT NULL,
  nome_produto VARCHAR(50) NOT NULL,
  categoria VARCHAR(50) NOT NULL,
  subcategoria VARCHAR(50) NOT NULL
);

-- Tabela Dimensão Localidade
CREATE TABLE schema3.dim_localidade (
  sk_localidade SERIAL PRIMARY KEY,
  id_localidade INT NOT NULL,
  pais VARCHAR(50) NOT NULL,
  regiao VARCHAR(50) NOT NULL,
  estado VARCHAR(50) NOT NULL,
  cidade VARCHAR(50) NOT NULL
);

-- Tabela Dimensão Tempo
CREATE TABLE schema3.dim_tempo (
  sk_tempo SERIAL PRIMARY KEY,
  data_completa date,
  ano INT NOT NULL,
  mes INT NOT NULL,
  dia INT NOT NULL
);

-- Tabela Fato de Vendas
CREATE TABLE schema3.fato_vendas (
  sk_produto INT NOT NULL,
  sk_cliente INT NOT NULL,
  sk_localidade INT NOT NULL,
  sk_tempo INT NOT NULL,
  quantidade INT NOT NULL,
  preco_venda DECIMAL(10, 2) NOT NULL,
  custo_produto DECIMAL(10, 2) NOT NULL,
  receita_vendas DECIMAL(10, 2) NOT NULL,
  PRIMARY KEY (sk_produto, sk_cliente, sk_localidade, sk_tempo),
  FOREIGN KEY (sk_produto) REFERENCES schema3.dim_produto (sk_produto),
  FOREIGN KEY (sk_cliente) REFERENCES schema3.dim_cliente (sk_cliente),
  FOREIGN KEY (sk_localidade) REFERENCES schema3.dim_localidade (sk_localidade),
  FOREIGN KEY (sk_tempo) REFERENCES schema3.dim_tempo (sk_tempo)
);


