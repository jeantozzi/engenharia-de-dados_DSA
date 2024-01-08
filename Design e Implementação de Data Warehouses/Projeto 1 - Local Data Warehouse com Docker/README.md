# Local Data Warehouse com Docker

## Sumário
- [Apresentação geral do projeto](#apresentação-geral-do-projeto)
- [Definição do problema de negócio](#definição-do-problema-de-negócio)
- [Esboço de modelagem](#esboço-de-modelagem)
    - [Modelagem dimensional](#modelagem-dimensional)
    - [Modelagem física](#modelagem-física)
- [Arquitetura do ETL](#arquitetura-do-etl)
- [Criação dos containers de banco de dados](#criação-dos-containers-de-banco-de-dados)
- [Padrão de nomenclatura](#padrão-de-nomenclatura)
- [Criação e carga das tabelas no container de Fonte de Dados](#criação-e-carga-das-tabelas-no-container-de-fonte-de-dados)
    - [Tabela de categorias](#tabela-de-categorias)
    - [Tabela de subcategorias](#tabela-de-subcategorias)
    - [Tabela de produtos](#tabela-de-produtos)
    - [Tabela de cidades](#tabela-de-cidades)
    - [Tabela de localidades](#tabela-de-localidades)
    - [Tabela de tipos de cliente](#tabela-de-tipos-de-cliente)
    - [Tabela de clientes](#tabela-de-clientes)
    - [Tabela de vendas](#tabela-de-vendas)
    - [Resultado tabelas da Fonte de Dados](#resultado-tabelas-da-fonte-de-dados)
- [Instalação do Airbyte para o processo de Extração](#instalação-do-airbyte-para-o-processo-de-extração)
- [Configuração do processo de Extração no Airbyte](#configuração-do-processo-de-extração-no-airbyte)
- [Criação das tabelas do Data Warehouse](#criação-das-tabelas-do-data-warehouse)
    - [Criação tabela dimensão cliente](#criação-tabela-dimensão-cliente)
    - [Criação tabela dimensão localidade](#criação-tabela-dimensão-localidade)
    - [Criação tabela dimensão produto](#criação-tabela-dimensão-produto)
    - [Criação tabela dimensão tempo](#criação-tabela-dimensão-tempo)
    - [Criação tabela fato de vendas](#criação-tabela-fato-de-vendas)
    - [Resultado criação tabelas do Data Warehouse](#resultado-criação-tabelas-do-data-warehouse)
- [Carga das tabelas do Data Warehouse](#carga-das-tabelas-do-data-warehouse)
    - [Carga tabela dimensão cliente](#carga-tabela-dimensão-cliente)
    - [Carga tabela dimensão localidade](#carga-tabela-dimensão-localidade)
    - [Carga tabela dimensão produto](#carga-tabela-dimensão-produto)
    - [Carga tabela dimensão tempo](#carga-tabela-dimensão-tempo)
    - [Carga tabela fato de vendas](#carga-tabela-fato-de-vendas)
    - [Resultado carga tabelas do Data Warehouse](#resultado-carga-tabelas-do-data-warehouse)
- [Modificação da granularidade temporal](#modificação-da-granularidade-temporal)
    - [Alteração na Fonte de Dados](#alteração-na-fonte-de-dados)
    - [Reexecução da extração via Airbyte](#reexecução-da-extração-via-airbyte)
    - [Alterações no Data Warehouse](#alterações-no-data-warehouse)
        - [Alteração e reinserção dos dados da tabela dim_tempo](#alteração-e-reinserção-dos-dados-da-tabela-dim_tempo)
        - [Nova carga da tabela fato de vendas](#nova-carga-da-tabela-fato-de-vendas)
- [Adição de nova métrica](#adição-de-nova-métrica)
    - [Alteração e reinserção dos dados da tabela fato_vendas](#alteração-e-reinserção-dos-dados-da-tabela-fato_vendas)
    - [Resultado adição nova métrica](#resultado-adição-nova-métrica)
- [Criação de Materialized View](#criação-de-materialized-view)


## Apresentação geral do projeto

Para este projeto vamos construir um Data Warehouse para uma empresa fictícia. A empresa “TechFab Manufatura S.A”.

Uma empresa de manufatura é um tipo de empresa que transforma matérias-primas ou componentes em produtos acabados através do uso de processos industriais. Esse tipo de empresa é responsável por projetar, produzir, montar e testar produtos, tais como automóveis, eletrônicos, alimentos, roupas, máquinas e equipamentos.

As empresas de manufatura geralmente possuem fábricas ou instalações onde os processos produtivos ocorrem. Essas instalações podem envolver o uso de máquinas, equipamentos, ferramentas, robôs e mão-de-obra qualificada para criar produtos que atendam às especificações de qualidade e segurança.

As empresas de manufatura podem atender tanto ao mercado de consumo final quanto ao mercado empresarial, produzindo uma ampla variedade de produtos em diferentes setores. Algumas empresas de manufatura são especializadas em um único produto, enquanto outras produzem uma ampla gama de produtos em diferentes áreas.

A manufatura é uma das principais atividades econômicas em muitos países, empregando milhões de pessoas em todo o mundo. As empresas de manufatura desempenham um papel fundamental na economia, fornecendo produtos essenciais e criando empregos em várias áreas, desde a produção até a administração, vendas e marketing.

A TechFab precisa gerar relatórios para melhor compreensão dos seus processos de negócio. A TechFab tem os seguintes dados disponíveis:

- **Dados de produção**: informações sobre a produção de produtos, incluindo quantidades produzidas, tempo de produção, taxas de defeito, dados de qualidade e informações sobre matérias-primas e processos de fabricação.

- **Dados de vendas**: informações sobre as vendas de produtos, incluindo preços, quantidades vendidas, locais de venda e dados de clientes.

- **Dados de fornecedores**: informações sobre fornecedores de matérias-primas e outros insumos necessários para a produção de produtos.

- **Dados financeiros**: informações sobre as finanças da empresa, incluindo receita, despesas, lucro e fluxo de caixa.

Abaixo estão alguns relatórios que a empresa gostaria para entender melhor o desempenho dos negócios, a eficiência da produção e as necessidades do mercado:

- **Relatórios de vendas**: relatórios que fornecem informações sobre as vendas por região, produto, canal de vendas e período de tempo, permitindo a empresa identificar os produtos mais vendidos, as tendências de mercado e os canais de vendas mais eficazes.

- **Relatórios de estoque**: relatórios que mostram informações sobre os níveis de estoque de matérias-primas, produtos em processo e produtos acabados, permitindo que a empresa gerencie seu estoque de forma mais eficiente e reduza os custos de armazenamento.

- **Relatórios de produção**: relatórios que mostram informações sobre a eficiência da produção, incluindo o tempo de ciclo, taxa de defeito, utilização de máquinas e outros indicadores de desempenho, permitindo que a empresa identifique áreas de melhoria e aumente a eficiência da produção.

- **Relatórios de manutenção**: relatórios que fornecem informações sobre as atividades de manutenção da empresa, incluindo o tempo de inatividade, manutenção preventiva e corretiva, custos de manutenção e outros indicadores de desempenho, permitindo que a empresa gerencie seus ativos de forma mais eficiente.

- **Relatórios financeiros**: relatórios que mostram informações financeiras, como receita, despesas, margem de lucro, fluxo de caixa e outras métricas financeiras importantes, permitindo que a empresa avalie sua saúde financeira e tome decisões informadas.

- **Relatórios de qualidade**: relatórios que fornecem informações sobre a qualidade dos produtos, incluindo os dados de inspeção e teste, o índice de defeito e outros indicadores de qualidade, permitindo que a empresa identifique áreas de melhoria na qualidade e tome medidas corretivas.

Nosso trabalho agora é implementar um projeto de Data Warehouse que atenda as necessidades da empresa. Vamos ao trabalho.

## Definição do problema de negócio

A TechFab Manufatura S.A., com sede em São Paulo-SP, é uma das maiores empresas do Brasil no segmento de manufatura e venda de eletrônicos, direto ao consumidor. A empresa possui diversas lojas em todo estado de São Paulo, além de Rio de Janeiro, Minas Gerais, Pernambuco, Bahia, Goiás e Santa Catarina. Em seu sétimo ano de operação, a empresa tem conseguido manter boas margens de lucro, com um crescimento anual de faturamento na ordem de 7,4%. O CEO decidiu que é hora de expandir as operações e precisa compreender melhor o cenário atualda empresa.

Depois de extensa pesquisa, o CEO e o board de diretores decidiram que uma solução de Business Intelligence, com métricas e KPIs referentes ao negócio da empresa seriam úteis para compreender erros/acertos na gestão até aqui e ajudar na definição da estratégia de crescimento para os próximos anos.

Foi criado então o projeto DSAVANTE, com o objetivo de fornecer uma solução de Business Intelligence corporativa. A área de TI da companhia já possui licenças de software de solução de BI para geração de relatórios, de um pacote de software adquirido com um fornecedor. As licenças nunca haviam sido usadas e os diretores determinaram que o software fosse usado, como forma de reduzir custo, uma vez que o pacote já havia sido pago.

Entretanto, a empresa não possui experiência em Data Warehouses e você foi contratado para oferecer a consultoria necessária na construção da solução. Você será responsável pela criação do Data Warehouse e das interfaces ETL. A administração e suporte de primeiro nível será de responsabilidade da equipe de TI da empresa.

Em sua primeira reunião, diversos diretores explicaram em linhas gerais como funciona a operação da empresa e registraram isso em ata. Abaixo o resultado desta reunião.

A TechFab Manufatura S.A. é uma empresa que produz e vende eletrônicos, especialmente equipamentos de informática em geral. A empresa trabalha com margens agressivas e embora o investimento em Marketing seja pequeno, ele é constante. São diversas lojas em todo Brasil e aproximadamente 700 funcionários.

Cada loja possui um estoque de diversos produtos eletrônicos, tais como desktops, notebooks, tablets e smartphones, que são os principais produtos da empresa, mas diversos outros produtos são vendidos, como TVs, sistemas de som, periféricos, entre outros. São aproximadamente 250 produtos, distribuídos em 15 categorias. Um armazém situado em Barueri-SP, mantém os produtos que chegam via importação ou de fábricas em São Paulo e Minas Gerais, onde eles são catalogados, recebem um selo RFID e então são despachados para as lojas em todo Brasil. Cada produto possui um código SKU único, além de detalhes que são armazenados no sistema de cadastro de produtos, tais como nome do produto, marca, dimensões e outras especificações técnicas.

Sempre que uma venda é registrada em um ponto de venda, uma das 23 lojas em todo Brasil, os vendedores são orientados a criar um cadastro sobre o cliente e solicitar uma autorização para o cliente receber futuras promoções e campanhas de Marketing. Nome, telefone, endereço e e-mail são obrigatórios no cadastro, mas outras informações podem ser solicitadas, principalmente no caso de vendas a prazo, como emprego atual, renda, tempo de residência e número de filhos.

A empresa possui também um cadastro de cada loja, que hoje está em planilha Excel. Lá estão o nome de cada loja (uma espécie de apelido que ajuda a identificar cada loja), o endereço, a região, cidade e estado. Cada loja tem um código. Essa planilha atualiza periodicamente o sistema de vendas da empresa, já que cada venda registrada é associada a uma loja. Todas as lojas vendem todos os produtos, mas as lojas mantêm estoques diferentes, como forma de reduzir custos com logística, ou seja, não despachar muitos produtos para as lojas que possuem um volume menor de vendas, o que poderia requerer possível movimentação posterior dos produtos para lojas com volume maior. Cada loja possui um CEP cadastrado cuidadosamente, pois a empresa implementa frequentemente algoritmos de otimização de logística usando análise em grafos. Eles compararam um novo sistema recentemente, depois que ouviram dizer que o sistema, que é baseado em Inteligência Artificial, poderia reduzir em até 25% os custos de combustível otimizando as rotas dos caminhões de entrega!

Em cada loja, os funcionários atendem os clientes no showroom, onde os produtos são expostos e também no telefone. Cada loja conta com alguns vendedores, pessoal de limpeza e supervisor, trabalhando em 2 turnos. A empresa pretende começar a vender online, mas ainda não há previsão. Todos os funcionários são cadastrados no sistema interno da empresa, com número de matrícula, dados pessoais e especialidade. Uma venda é sempre feita por um vendedor, pois a empresa paga comissão pelas vendas efetuadas e a matrícula do responsável pela venda fica atrelada a cada venda realizada.

O valor e a quantidade de cada venda estão presentes nos relatórios diários da empresa, que são usados para diferentes decisões durante a semana. Mas esses relatórios são manuais, criados normalmente no Excel, e frequentemente apresentam erros. Cada diretor regional precisa saber as vendas por região, para acompanhar o desempenho da sua loja e comparar com as demais regiões. A empresa faz muitas vendas de produtos como um único pacote ou combo, mas que são produtos diferentes. Por exemplo: um desktop pode ser vendido junto com um monitor, teclado e mouse. Embora seja um pacote, os produtos possuem SKUs diferentes, valores diferentes e contribuem de forma diferente quando um desconto é concedido. A empresa calcula o percentual de cada produto em uma venda de pacotes ou combos.

Os diretores acreditam que algumas categorias de produtos podem não ser lucrativas e gostariam de confirmar esta informação com o novo sistema de BI. Essa informação também será útil para definir as estratégias de expansão e quais novas categorias de produtos devem ser consideradas.

## Esboço de modelagem

### Modelagem dimensional

![modelagem_dimensional_1](./images/modelagem_dimensional.png)

### Modelagem física

- Tabela Dimensão Cliente
```sql
CREATE TABLE `DIM_CLIENTE` (
    `SK_CLIENTE` INTEGER(20) NOT NULL,
    `NK_ID_CLIENTE` VARCHAR(20) NOT NULL,
    `NM_CLIENTE` VARCHAR(50) NOT NULL,
    `NM_CIDADE_CLIENTE` VARCHAR(50) NOT NULL,
    `BY_ACEITA_CAMPANHA` BINARY NOT NULL,
    `DESC_CEP` VARCHAR(10) NOT NULL,
    PRIMARY KEY (`SK_CLIENTE`)
);
```

- Tabela Dimensão Produto
```sql
CREATE TABLE `DIM_PRODUTO` (
    `SK_PRODUTO` INTEGER(20) NOT NULL,
    `NK_ID_PRODUTO` VARCHAR(20) NOT NULL,
    `DESC_SKU` VARCHAR(50) NOT NULL,
    `NM_PRODUTO` VARCHAR(50) NOT NULL,
    `NM_CATEGORIA_PRODUTO` VARCHAR(30) NOT NULL,
    `NM_MARCA_PRODUTO` VARCHAR(30) NOT NULL,
    PRIMARY KEY (`SK_PRODUTO`)
);
```

- Tabela Dimensão Localidade
```sql
CREATE TABLE `DIM_LOCALIDADE` (
    `SK_LOCALIDADE` INTEGER(20) NOT NULL,
    `NK_ID_LOCALIDADE` VARCHAR(20) NOT NULL,
    `NM_LOCALIDADE` VARCHAR(50) NOT NULL,
    `NM_CIDADE_LOCALIDADE` VARCHAR(50) NOT NULL,
    `NM_REGIAO_LOCALIDADE` VARCHAR(50) NOT NULL,
    PRIMARY KEY (`SK_LOCALIDADE`)
);
```

- Tabela Dimensão Tempo
```sql
CREATE TABLE `DIM_TEMPO` (
    `SK_DATA` INTEGER(20) NOT NULL,
    `DATA` DATE NOT NULL,
    `DESC_DATA_COMPLETA` VARCHAR(50) NOT NULL,
    `NR_ANO` INTEGER(4) NOT NULL,
    `NM_TRIMESTRE` VARCHAR(20) NOT NULL,
    `NR_MES` INTEGER NOT NULL,
    `NM_MES` VARCHAR(20) NOT NULL,
    `NR_SEMANA` INTEGER NOT NULL,
    `NM_ANO_SEMANA` VARCHAR(20) NOT NULL,
    `NR_DIA` INTEGER NOT NULL,
    `NM_DIA_SEMANA` VARCHAR (20) NOT NULL,
    `FLAG_FERIADO` CHAR(3) NOT NULL,
    `NM_FERIADO` VARCHAR(20) NOT NULL,
    PRIMARY KEY (`SK_DATA`)
);
```

- Tabela Fato de Vendas
```sql
CREATE TABLE `FATO_VENDA` (
    `SK_CLIENTE` INTEGER(20) NOT NULL,
    `SK_PRODUTO` INTEGER(20) NOT NULL,
    `SK_LOCALIDADE` INTEGER(20) NOT NULL,
    `SK_DATA` INTEGER(20) NOT NULL,
    `VL_VENDA` DECIMAL NOT NULL,
    `QTD_VENDA` INTEGER NOT NULL,
    PRIMARY KEY (`SK_CLIENTE`, `SK_PRODUTO`, `SK_LOCALIDADE`, `SK_DATA`)
);
```

## Arquitetura do ETL

![Arquitetura ETL](./images/arquitetura_etl.png)

- Extração de dados com Airbyte:
    - Os dados serão levados do servidor da Fonte de Dados para o servidor da Staging Area. Aqui não haverá filtro ou transformação e os dados brutos das tabelas correspondentes serão levados para a Staging Area. O objetivo é gerar a menor sobrecarga possível no servidor de origem dos dados.

- Transformação e Carga de Dados via SQL:
    - Na Staging Area os dados serão limpos, transformados e processados com linguagem SQL. A linguagem SQL também será usada para carregar os dados no Data Warehouse (o que pode ser feito com uma query).

## Criação dos containers de banco de dados

Executaremos os comandos a seguir para a criação dos containers:

- Fonte de Dados
```bash
$ docker run --name dbdsafonte -p 5433:5432 -e POSTGRES_USER=dbadmin -e POSTGRES_PASSWORD=dbadmin123 -e POSTGRES_DB=postgresDB -d postgres
```

- Staging Area/Data Warehouse
```bash
$ docker run --name dbdsadestino -p 5434:5432 -e POSTGRES_USER=dbadmin -e POSTGRES_PASSWORD=dbadmin123 -e POSTGRES_DB=postgresDB -d postgres
```

O resultado é a criação de dois containers de uma única imagem Postgres:

![Criando os containers](./images/containers.png)

## Criação dos _schemas_

- Fonte de Dados

![Schema 1](./images/schema_1.png)

- Staging Area/Data Warehouse

![Schema 2 e 3](./images/schema_2-3.png)

## Padrão de nomenclatura

As tabelas terão um prefixo na nomenclatura dependendo do schema em questão, conforme a divisão abaixo.

- Fonte de Dados
    - Prefixo `ft_` para as tabelas

- Staging Area
    - Prefixo `st_` para as tabelas

- Data Warehouse
    - Prefixo `dim_` para as tabelas de dimensão
    - Prefixo `fato_` para a tabela fato

## Criação e carga das tabelas no container de Fonte de Dados

Executaremos os comandos abaixo para criar e popular as tabelas no banco de Fonte de Dados.
É possível encontrá-los consolidados [neste arquivo .sql](./sql/tabelas_fonte.sql).

### Tabela de categorias

```sql
-- Criando a tabela
CREATE TABLE schema1.ft_categorias (
    id_categoria SERIAL PRIMARY KEY,
    nome_categoria VARCHAR(255) NOT NULL
);

-- Inserindo dados
INSERT INTO schema1.ft_categorias (nome_categoria) VALUES ('Computadores');
INSERT INTO schema1.ft_categorias (nome_categoria) VALUES ('Smartphones');
INSERT INTO schema1.ft_categorias (nome_categoria) VALUES ('Impressoras');
```

### Tabela de subcategorias

```sql
-- Criando a tabela
CREATE TABLE schema1.ft_subcategorias (
    id_subcategoria SERIAL PRIMARY KEY,
    nome_subcategoria VARCHAR(255) NOT NULL,
    id_categoria INTEGER REFERENCES schema1.ft_categorias(id_categoria)
);

-- Inserindo dados
INSERT INTO schema1.ft_subcategorias (nome_subcategoria, id_categoria) VALUES ('Notebook', 1);
INSERT INTO schema1.ft_subcategorias (nome_subcategoria, id_categoria) VALUES ('Desktop', 1);
INSERT INTO schema1.ft_subcategorias (nome_subcategoria, id_categoria) VALUES ('iPhone', 2);
INSERT INTO schema1.ft_subcategorias (nome_subcategoria, id_categoria) VALUES ('Samsung Galaxy', 2);
INSERT INTO schema1.ft_subcategorias (nome_subcategoria, id_categoria) VALUES ('Laser', 3);
INSERT INTO schema1.ft_subcategorias (nome_subcategoria, id_categoria) VALUES ('Matricial', 3);
```

### Tabela de produtos

```sql
-- Criando a tabela
CREATE TABLE schema1.ft_produtos (
    id_produto SERIAL PRIMARY KEY,
    nome_produto VARCHAR(255) NOT NULL,
    preco_produto NUMERIC(10, 2) NOT NULL,
    id_subcategoria INTEGER REFERENCES schema1.ft_subcategorias(id_subcategoria)
);

-- Inserindo dados
INSERT INTO schema1.ft_produtos (nome_produto, preco_produto, id_subcategoria) VALUES ('Apple MacBook Pro M2', 6589.99, 1);
INSERT INTO schema1.ft_produtos (nome_produto, preco_produto, id_subcategoria) VALUES ('Desktop Dell 16 GB', 1500.50, 2);
INSERT INTO schema1.ft_produtos (nome_produto, preco_produto, id_subcategoria) VALUES ('iPhone 14', 4140.00, 3);
INSERT INTO schema1.ft_produtos (nome_produto, preco_produto, id_subcategoria) VALUES ('Samsung Galaxy Z', 3500.99, 4);
INSERT INTO schema1.ft_produtos (nome_produto, preco_produto, id_subcategoria) VALUES ('HP 126A Original LaserJet Imaging Drum', 300.90, 5);
INSERT INTO schema1.ft_produtos (nome_produto, preco_produto, id_subcategoria) VALUES ('Epson LX-300 II USB', 350.99, 6);
```

### Tabela de cidades

```sql
-- Criando a tabela
CREATE TABLE schema1.ft_cidades (
    id_cidade SERIAL PRIMARY KEY,
    nome_cidade VARCHAR(255) NOT NULL
);

-- Inserindo dados
INSERT INTO schema1.ft_cidades (nome_cidade) VALUES
    ('Natal'),
    ('Rio de Janeiro'),
    ('Belo Horizonte'),
    ('Salvador'),
    ('Blumenau'),
    ('Curitiba'),
    ('Fortaleza'),
    ('Recife'),
    ('Porto Alegre'),
    ('Manaus');
```

### Tabela de localidades

```sql
-- Criando a tabela
CREATE TABLE schema1.ft_localidades (
    id_localidade SERIAL PRIMARY KEY,
    pais VARCHAR(255) NOT NULL,
    regiao VARCHAR(255) NOT NULL,
    id_cidade INTEGER REFERENCES schema1.ft_cidades(id_cidade)
);

-- Inserindo dados
INSERT INTO schema1.ft_localidades (pais, regiao, id_cidade) VALUES
    ('Brasil', 'Nordeste', 1),
    ('Brasil', 'Sudeste', 2),
    ('Brasil', 'Sudeste', 3),
    ('Brasil', 'Nordeste', 4),
    ('Brasil', 'Sul', 5),
    ('Brasil', 'Sul', 6),
    ('Brasil', 'Nordeste', 7),
    ('Brasil', 'Nordeste', 8),
    ('Brasil', 'Sul', 9),
    ('Brasil', 'Norte', 10);
```

### Tabela de tipos de cliente

```sql
-- Criando a tabela
CREATE TABLE schema1.ft_tipo_cliente (
    id_tipo SERIAL PRIMARY KEY,
    nome_tipo VARCHAR(255) NOT NULL
);

-- Inserindo dados
INSERT INTO schema1.ft_tipo_cliente (nome_tipo) VALUES ('Corporativo');
INSERT INTO schema1.ft_tipo_cliente (nome_tipo) VALUES ('Consumidor');
INSERT INTO schema1.ft_tipo_cliente (nome_tipo) VALUES ('Desativado');
```

### Tabela de clientes

```sql
-- Criando a tabela
CREATE TABLE schema1.ft_clientes (
    id_cliente SERIAL PRIMARY KEY,
    nome_cliente VARCHAR(255) NULL,
    email_cliente VARCHAR(255) NULL,
    id_cidade INTEGER REFERENCES schema1.ft_cidades(id_cidade),
    id_tipo INTEGER REFERENCES schema1.ft_tipo_cliente(id_tipo)
);

-- Inserindo dados
INSERT INTO schema1.ft_clientes (nome_cliente, email_cliente, id_cidade, id_tipo) VALUES ('João Silva', 'joao.silva@exemplo.com', 1, 1);
INSERT INTO schema1.ft_clientes (nome_cliente, email_cliente, id_cidade, id_tipo) VALUES ('Maria Santos', 'maria.santos@exemplo.com', 2, 2);
INSERT INTO schema1.ft_clientes (nome_cliente, email_cliente, id_cidade, id_tipo) VALUES ('Pedro Lima', 'pedro.lima@exemplo.com', 3, 2);
INSERT INTO schema1.ft_clientes (nome_cliente, email_cliente, id_cidade, id_tipo) VALUES ('Ana Rodrigues', 'ana.rodrigues@exemplo.com', 4, 2);
INSERT INTO schema1.ft_clientes (nome_cliente, email_cliente, id_cidade, id_tipo) VALUES ('José Oliveira', 'jose.oliveira@exemplo.com', 1, 2);
INSERT INTO schema1.ft_clientes (nome_cliente, email_cliente, id_cidade, id_tipo) VALUES ('Carla Santos', 'carla.santos@exemplo.com', 4, 1);
INSERT INTO schema1.ft_clientes (nome_cliente, email_cliente, id_cidade, id_tipo) VALUES ('Marcos Souza', 'marcos.souza@exemplo.com', 5, 2);
INSERT INTO schema1.ft_clientes (nome_cliente, email_cliente, id_cidade, id_tipo) VALUES ('Julia Silva', 'julia.silva@exemplo.com', 1, 1);
INSERT INTO schema1.ft_clientes (nome_cliente, email_cliente, id_cidade, id_tipo) VALUES ('Lucas Martins', 'lucas.martins@exemplo.com', 3, 3);
INSERT INTO schema1.ft_clientes (nome_cliente, email_cliente, id_cidade, id_tipo) VALUES ('Fernanda Lima', 'fernanda.lima@exemplo.com', 4, 2);
```

### Tabela de vendas

```sql
-- Criando a tabela
CREATE TABLE schema1.ft_vendas (
    id_transacao VARCHAR(50) NOT NULL,
    id_produto INT NOT NULL,
    id_cliente INT NOT NULL,
    id_localizacao INT NOT NULL,
    data_transacao DATE NULL,
    quantidade INT NOT NULL,
    preco_venda DECIMAL(10, 2) NOT NULL,
    custo_produto DECIMAL(10, 2) NOT NULL
);

-- Gerando dados aleatórios
WITH dados_aleatorios AS (
    SELECT 
        FLOOR(RANDOM() * 1000000)::TEXT AS id_transacao,
        FLOOR(RANDOM() * 6 + 1) AS id_produto,
        FLOOR(RANDOM() * 10 + 1) AS id_cliente,
        FLOOR(RANDOM() * 4 + 1) AS id_localizacao,
        '2022-01-01'::DATE + FLOOR(RANDOM() * 365)::INTEGER AS data_transacao,
        FLOOR(RANDOM() * 10 + 1) AS quantidade,
        ROUND(CAST(RANDOM() * 100 + 1 AS NUMERIC), 2) AS preco_venda,
        ROUND(CAST(RANDOM() * 50 + 1 AS NUMERIC), 2) AS custo_produto
    FROM GENERATE_SERIES(1, 1000)
)

-- Inserindo dados
INSERT INTO schema1.ft_vendas (id_transacao, id_produto, id_cliente, id_localizacao, data_transacao, quantidade, preco_venda, custo_produto)
SELECT 
    'TRAN-' || id_transacao AS id_transacao,
    id_produto,
    id_cliente,
    id_localizacao,
    data_transacao,
    quantidade,
    ROUND(CAST(preco_venda AS NUMERIC), 2),
    ROUND(CAST(custo_produto AS NUMERIC), 2)
FROM dados_aleatorios;
```

### Resultado tabelas da Fonte de Dados

Com todas as tabelas criadas e populadas, teremos o seguinte resultado na hierarquia:

![Tabelas Fonte de Dados](./images/tabelas_ft.png)

Abaixo, como ilustração, temos alguns registros que foram inseridos na tabela `ft_vendas`:

![Dados Tabela Vendas](./images/ft_vendas.png)

## Instalação do Airbyte para o processo de Extração

Instalaremos o Airbyte conforme instruções descritas no site oficial (https://docs.airbyte.com/deploying-airbyte/local-deployment).
Precisamos clonar o repositório do Airbyte para Docker e depois executá-lo:

```bash
# Clonando os arquivos do repositório do Airbyte no GitHub
$ git clone --depth=1 https://github.com/airbytehq/airbyte.git

# Entrando na pasta do repositório clonado
$ cd airbyte

# Executando o Airbyte
$ ./run-ab-platform.sh
```

Após a execução dos comandos acima, teremos o container do Airbyte rodando, pronto para ser acessado com as credenciais pré-definidas.

![Airbyte 1](./images/airbyte_1.png)

- **Caminho:** http://localhost:8000
- **Login:** airbyte
- **Senha:** password

![Airbyte 2](./images/airbyte_2.png)

## Configuração do processo de Extração no Airbyte

Agora vamos criar as duas conexões, usando as credenciais utilizadas no item [Criação dos containers de banco de dados](#criação-dos-containers-de-banco-de-dados):

- **fonte:** um `source` que conecta no schema `schema1` do banco `postgresDB` localizado no container `dbdsafonte`;
    - ![Source fonte](./images/airbyte_source_fonte.png)
- **staging:** um `destination` que conecta no schema `schema2` do banco `postgresDB` localizado no container `dbdsadestino`;
    - ![Destination staging](./images/airbyte_destination_staging.png)

Para finalizar, criaremos um fluxo (connection) que se utilizará das conexões anteriores para realizar a etapa de Extração para a Staging Area.

![Connection ETL Fase 1](./images/airbyte_connection_etl-fase1.png)

Como podemos notar, a conexão irá adicionar o prefixo `st_` no destino.

Abaixo temos o resultado da execução bem-sucedida do fluxo criado:

![Tabelas staging](./images/tabelas_staging.png)

## Criação das tabelas do Data Warehouse

As tabelas serão criadas utilizando um recurso chamado `Surogate Key (SK)`, que é identificado a partir do prefixo `sk_` nas tabelas.

A SK serve para desatrelarmos informações de negócio (como por exemplo os campos com prefixo `id_`) da construção dos relacionamentos no banco de dados.

Dentre vários motivos, temos alguns principais:

- **Eliminação  de  problemas  de  desempenho:**  as  chaves  naturais  podem  ser  grandes  e complexas,  o  que  pode  afetar  negativamente  o  desempenho  do  banco  de  dados.  As  chaves surrogate,  por  outro  lado,  são  geralmente  simples  e  pequenas,  o  que  torna  a  pesquisa  e  a indexação mais rápidas e eficientes.

- **Facilidade de manutenção:** as chaves naturais podem mudar com o tempo, o que pode afetar a integridade dos dados. As chaves surrogate, por outro lado, são atribuídas pelo sistema e permanecem estáveis ao longo do tempo, o que facilita a manutenção do Data Warehouse.

- **Flexibilidade:** as  chaves  surrogate  são  independentes  do  contexto  dos  dados,  o  que significa que podem ser usadas em diferentes tabelas e em diferentes modelos de dados, sem afetar a integridade dos dados. Isso permite que o Data Warehouse seja mais flexível e escalável.

- **Integraçãode dados:** as chaves surrogate permitem a integração de dados de diferentes fontes,  mesmo  que  as  chaves  naturais  sejam  diferentes.  Isso  significa  que  o  Data Warehouse  pode  ser alimentado  com  dados  de  diferentes  sistemas  e  fontes,  tornando-o  mais  completo  e  útil  para análises.

- **Segurança:**  as  chaves  surrogate  podem  ser  criptografadas,  tornando-as  mais  seguras  e protegidas contra ameaças externas.

Executaremos os comandos abaixo para criar as tabelas no Data Warehouse.
É possível encontrá-los consolidados [neste arquivo .sql](./sql/tabelas_dw.sql).

### Criação tabela dimensão cliente

```sql
CREATE TABLE schema3.dim_cliente (
    sk_cliente SERIAL PRIMARY KEY,
    id_cliente INT NOT NULL,
    nome VARCHAR(50) NOT NULL,
    tipo VARCHAR(50) NOT NULL
);
```

### Criação tabela dimensão produto

```sql
CREATE TABLE schema3.dim_produto (
    sk_produto SERIAL PRIMARY KEY,
    id_produto INT NOT NULL,
    nome_produto VARCHAR(50) NOT NULL,
    categoria VARCHAR(50) NOT NULL,
    subcategoria VARCHAR(50) NOT NULL
);
```

### Criação tabela dimensão localidade

```sql
CREATE TABLE schema3.dim_localidade (
    sk_localidade SERIAL PRIMARY KEY,
    id_localidade INT NOT NULL,
    pais VARCHAR(50) NOT NULL,
    regiao VARCHAR(50) NOT NULL,
    estado VARCHAR(50) NOT NULL,
    cidade VARCHAR(50) NOT NULL
);
```

### Criação tabela dimensão tempo

```sql
CREATE TABLE schema3.dim_tempo (
    sk_tempo SERIAL PRIMARY KEY,
    data_completa date,
    ano INT NOT NULL,
    mes INT NOT NULL,
    dia INT NOT NULL
);
```

### Criação tabela fato de vendas

```sql
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
```
### Resultado criação tabelas do Data Warehouse

Após a execução dos comandos, teremos as tabelas constando na hierarquia, como abaixo:

![Tabelas Data Warehouse](./images/tabelas_dw.png)

## Carga das tabelas do Data Warehouse

Executaremos os comandos abaixo para popular as tabelas no Data Warehouse.
É possível encontrá-los consolidados [neste arquivo .sql](./sql/carga_dw.sql).

### Carga tabela dimensão cliente

```sql
INSERT INTO schema3.dim_cliente (id_cliente, nome, tipo)
SELECT
    cli.id_cliente, 
    cli.nome_cliente, 
    tip.nome_tipo
FROM
    schema2.st_ft_clientes AS cli
    INNER JOIN schema2.st_ft_tipo_cliente AS tip USING(id_tipo);
```

### Carga tabela dimensão localidade

```sql
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
```

### Carga tabela dimensão produto

```sql
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
```

### Carga tabela dimensão tempo

```sql
INSERT INTO schema3.dim_tempo (ano, mes, dia, data_completa)
SELECT
    EXTRACT(YEAR FROM d)::INT, 
    EXTRACT(MONTH FROM d)::INT, 
    EXTRACT(DAY FROM d)::INT,
    d::DATE
FROM
    GENERATE_SERIES('2020-01-01'::DATE, '2024-12-31'::DATE, '1 day'::INTERVAL) AS d;
```

### Carga tabela fato de vendas

Duas observações em relação a essa carga:
- Pelo fato das Surogate Keys só estarem presentes na Staging Area, é necessário fazer um Join com essas tabelas;
- Como a granularidade da nossa arquitetura é diária, precisamos agrupar e agregar os dados para que não tenhamos problemas com múltiplas vendas diárias; 

```sql
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
    SUM(ROUND((CAST(st_ven.quantidade AS numeric) * CAST(st_ven.preco_venda AS numeric)), 2)) AS receita_vendas
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
```

### Resultado carga tabelas do Data Warehouse

![ERD Data Warehouse](./images/erd_dw.png)

Abaixo, como ilustração, temos alguns registros que foram inseridos na tabela `fato_vendas`:

![Dados Tabela Fato Vendas](./images/fato_vendas.png)

## Modificação da granularidade temporal

Agora vamos implementar uma mudança de granularidade na dimensão tempo.

Atualmente os dados vêm da Fonte de Dados com granularidade **diária**, e precisaremos fazer algumas modificações na Fonte de Dados, depois da Staging Area e no Data Warehouse, nesta ordem.
A nova granularidade será de **hora**.

### Alteração na Fonte de Dados

Executaremos os comandos abaixo com o intuito de recriar a tabela com informação temporal na Fonte de Dados, presente no container `dbdsafonte`. 
É possível encontrá-los consolidados [neste arquivo .sql](./sql/ajuste_granularidade_tabela_fonte.sql).

```sql
-- Deletando a tabela atual
DROP TABLE schema1.ft_vendas;

-- Criando a nova tabela com a data da transação em formato TIMESTAMP em vez de DATE
CREATE TABLE schema1.ft_vendas (
    id_transacao VARCHAR(50) NOT NULL,
    id_produto INT NOT NULL,
    id_cliente INT NOT NULL,
    id_localizacao INT NOT NULL,
    data_transacao TIMESTAMP NULL,
    quantidade INT NOT NULL,
    preco_venda DECIMAL(10, 2) NOT NULL,
    custo_produto DECIMAL(10, 2) NOT NULL
);

-- Gerando dados aleatórios
WITH dados AS (
    SELECT 
        FLOOR(RANDOM() * 1000000)::TEXT AS id_transacao,
        FLOOR(RANDOM() * 6 + 1) AS id_produto,
        FLOOR(RANDOM() * 10 + 1) AS id_cliente,
        FLOOR(RANDOM() * 4 + 1) AS id_localizacao,
        ('2022-01-01'::DATE + (RANDOM() * 365)::INTEGER) + INTERVAL '1 second' * (FLOOR(RANDOM() * 86400)::INTEGER) AS data_transacao,
        FLOOR(RANDOM() * 10 + 1) AS quantidade,
        ROUND(CAST(RANDOM() * 100 + 1 AS NUMERIC), 2) AS preco_venda,
        ROUND(CAST(RANDOM() * 50 + 1 AS NUMERIC), 2) AS custo_produto
    FROM GENERATE_SERIES(1, 1000)
)

-- Inserindo dados
INSERT INTO schema1.ft_vendas (id_transacao, id_produto, id_cliente, id_localizacao, data_transacao, quantidade, preco_venda, custo_produto)
SELECT 
    'TRAN-' || id_transacao AS id_transacao,
    id_produto,
    id_cliente,
    id_localizacao,
    data_transacao,
    quantidade,
    ROUND(CAST(preco_venda AS NUMERIC), 2),
    ROUND(CAST(custo_produto AS NUMERIC), 2)
FROM dados;
```

### Reexecução da extração via Airbyte

Como mudamos somente uma das tabelas que vão da Fonte de Dados para a Staging Area, utilizaremos a funcionalidade do Airbyte de sincronizar somente a tabela em questão.

![Airbyte sync vendas](./images/airbyte_sync_vendas.png)

Certifique-se que de acionar a opção de reset no schema da conexão de fonte de dados, uma vez que houve modificação. Caso contrário, haverá erro de conversão e não teremos o resultado desejado.

![Airbyte refresh schema](./images/airbyte_refresh.png)

Após execução do processo de carga, temos os dados atualizados na Staging Area, conforme abaixo:

![Dados schema modificado](./images/dados_schema_modificado.png)

### Alterações no Data Warehouse

Executaremos os comandos abaixo com o intuito de alterar a tabela `dim_tempo` e recarregar os dados na tabela `fato_vendas`, ambas presentes no container Data Warehouse.
É possível encontrá-los consolidados [neste arquivo .sql](./sql/ajuste_granularidade_tabela_fonte.sql).

#### Alteração e reinserção dos dados da tabela dim_tempo

```sql
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
```

#### Nova carga da tabela fato de vendas

```sql
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
        TO_CHAR(st_ven.data_transacao, 'HH') = dw_tem.hora)
    INNER JOIN schema3.dim_produto AS dw_pro ON (st_pro.id_produto = dw_pro.id_produto)
    INNER JOIN schema3.dim_localidade AS dw_loc ON (st_loc.id_localidade = dw_loc.id_localidade)
    INNER JOIN schema3.dim_cliente AS dw_cli ON (st_cli.id_cliente = dw_cli.id_cliente)
GROUP BY 
    dw_pro.sk_produto, dw_cli.sk_cliente, dw_loc.sk_localidade, dw_tem.sk_tempo;
```

## Adição de nova métrica

A área de negócio usa a tabela fato em uma ferramenta de BI e precisou criar uma nova coluna, que calcula o resultado agregado por produto da seguinte forma:

$\text{resultado}=\text{quantidade de produtos} * (\text{preço venda}-\text{custo produto})$

Foi relatado que o painel de relatórios está tendo problemas de desempenho nos gráficos e tabelas que utilizam a métrica calculada. <br> Implementaremos o cálculo do resultado já dentro da tabela `fato_vendas`, através do ETL, para que a performance da ferramenta de BI melhore.
Para isso, precisamos modificar o schema da tabela e em seguida inserir os dados novamente.

Executaremos os comandos abaixo com o intuito de alterar a tabela `fato_vendas` e recarregar os dados.
É possível encontrá-los consolidados [neste arquivo .sql](./sql/nova_metrica_fato.sql).

### Alteração e reinserção dos dados da tabela fato_vendas

```sql
-- Incluindo mais uma coluna na tabela fato de vendas
ALTER TABLE IF EXISTS schema3.fato_vendas
    ADD COLUMN resultado numeric(10, 2) NOT NULL;

-- Limpando a tabelas para reexecutar a carga
TRUNCATE TABLE schema3.fato_vendas;

-- Carregando novamente a tabela fato de vendas
INSERT INTO schema3.fato_vendas (
    sk_produto, 
    sk_cliente, 
    sk_localidade, 
    sk_tempo, 
    quantidade, 
    preco_venda, 
    custo_produto, 
    receita_vendas,
    resultado)
SELECT 
    dw_pro.sk_produto,
    dw_cli.sk_cliente,
    dw_loc.sk_localidade,
    dw_tem.sk_tempo, 
    SUM(st_ven.quantidade) AS quantidade, 
    SUM(st_ven.preco_venda) AS preco_venda, 
    SUM(st_ven.custo_produto) AS custo_produto, 
    SUM(ROUND((CAST(st_ven.quantidade AS NUMERIC) * CAST(st_ven.preco_venda AS NUMERIC)), 2)) AS receita_vendas,
    SUM(ROUND((CAST(quantidade AS NUMERIC) * CAST(preco_venda AS NUMERIC)), 2) - custo_produto) AS resultado 
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
```

### Resultado adição nova métrica

![Dados nova métrica](./images/dados_nova_metrica.png)

## Criação de Materialized View

Um usuário do Data Warehouse reportou que a consulta abaixo está com problemas de perfomance.
É possível encontrar todos os comandos SQL abaixo consolidados [neste arquivo .sql](./sql/consulta_dw.sql).

```sql
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
```

Examinando o plano de execução da consulta, podemos ver que o problema não é a construçaõ da consulta, e a criação de particionamento e/ou indexação talvez não resolvam o problema.

![Plano de execução da consulta](./images/plano_execucao_consulta.png)

Uma alternativa é utilizar uma Materialized View, que nada mais que é uma View (uma consulta encapsulada) que guarda os dados em uma tabela temporária, que atualiza sob demanda. O benefício desse recurso é que não gerar os dados a cada consulta, pois eles estarão presentes na tabela temporária criada.

Abaixo podemos ver o comando de criação, a evidência da criação e o plano de execução da Materialized View, respectivamente:

```sql
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
```

![Evidência de criação da Materialized View](./images/materialized_view_evidencia.png)

![Plano de execução Materialized View](./images/materialized_view_plano_execucao.png)

O uso de Materialized Views traz alguns pontos de atenção. Um deles, como comentado anteriormente, é a necessidade de atualizar a tabela temporária regularmente, para garantir que os dados reflitam o estado mais atual.

Uma forma de garantir essa atualização seria criar um processo recorrente, que executa o comando de atualização a partir de um agendamento.
Como estamos usando PostgreSQL, o `pg_cron` (https://github.com/citusdata/pg_cron) funciona perfeitamente para nossos proprósitos.

Podemos executar, dentro do plugin, o comando abaixo para criar um agendamento que atualiza a Materialized View todos os dias ao meio-dia.

```sql
SELECT cron.schedule('REFRESH mv_relatorio', '0 12 * * *', 'REFRESH MATERIALIZED VIEW mv_relatorio;');
```


