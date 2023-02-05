DROP TABLE lab6."TB_FATO";
DROP TABLE lab6."DIM_CLIENTE";
DROP TABLE lab6."DIM_TRANSPORTADORA";
DROP TABLE lab6."DIM_DEPOSITO";
DROP TABLE lab6."DIM_ENTREGA";
DROP TABLE lab6."DIM_PAGAMENTO";
DROP TABLE lab6."DIM_FRETE";
DROP TABLE lab6."DIM_DATA";


CREATE TABLE lab6.DIM_CLIENTE
(
    id_cliente int NOT NULL,
    nome_cliente text,
    sobrenome_cliente text,
    PRIMARY KEY (id_cliente)
);


CREATE TABLE lab6.DIM_TRANSPORTADORA
(
    id_transportadora integer NOT NULL,
    nome_transportadora text,
    PRIMARY KEY (id_transportadora)
);


CREATE TABLE lab6.DIM_DEPOSITO
(
    id_deposito bigint NOT NULL,
    nome_deposito text,
    PRIMARY KEY (id_deposito)
);


CREATE TABLE lab6.DIM_ENTREGA
(
    id_entrega bigint NOT NULL,
    endereco_entrega text,
    pais_entrega text,
    PRIMARY KEY (id_entrega)
);


CREATE TABLE lab6.DIM_PAGAMENTO
(
    id_pagamento bigint NOT NULL,
    tipo_pagamento text,
    PRIMARY KEY (id_pagamento)
);


CREATE TABLE lab6.DIM_FRETE
(
    id_frete bigint NOT NULL,
    tipo_frete text,
    PRIMARY KEY (id_frete)
);


CREATE TABLE lab6.DIM_DATA
(
    id_data bigint NOT NULL,
    data_completa text,
    dia integer,
    mes integer,
    ano integer,
    PRIMARY KEY (id_data)
);


CREATE TABLE lab6.TB_FATO
(
    id_cliente integer,
    id_transportadora integer,
    id_deposito integer,
    id_entrega integer,
    id_pagamento integer,
    id_frete integer,
    id_data integer,
    valor_entrega double precision,
    PRIMARY KEY (id_cliente, id_transportadora, id_deposito, id_entrega, id_pagamento, id_frete, id_data)
);