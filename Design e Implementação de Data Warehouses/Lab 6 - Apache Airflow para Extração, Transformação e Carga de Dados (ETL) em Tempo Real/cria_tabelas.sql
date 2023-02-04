# Criação de tabelas do Lab 6

CREATE TABLE lab6."DIM_CLIENTE"
(
    id_cliente int NOT NULL,
    nome_cliente text,
    sobrenome_cliente text,
    PRIMARY KEY (id_cliente)
);


CREATE TABLE lab6."DIM_TRANSPORTADORA"
(
    id_transportadora integer NOT NULL,
    nome_transportadora text,
    PRIMARY KEY (id_transportadora)
);


CREATE TABLE lab6."DIM_DEPOSITO"
(
    id_deposito bigint NOT NULL,
    nome_deposito text,
    PRIMARY KEY (id_deposito)
);


CREATE TABLE lab6."DIM_ENTREGA"
(
    id_entrega bigint NOT NULL,
    endereco_entrega text,
    pais_entrega text,
    PRIMARY KEY (id_entrega)
);


CREATE TABLE lab6."DIM_PAGAMENTO"
(
    id_pagamento bigint NOT NULL,
    tipo_pagamento text,
    PRIMARY KEY (id_pagamento)
);


CREATE TABLE lab6."DIM_FRETE"
(
    id_frete bigint NOT NULL,
    tipo_frete text,
    PRIMARY KEY (id_frete)
);


CREATE TABLE lab6."DIM_DATA"
(
    id_data bigint NOT NULL,
    data_completa text,
    dia integer,
    mes integer,
    ano integer,
    PRIMARY KEY (id_data)
);


CREATE TABLE lab6."TB_FATO"
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


ALTER TABLE IF EXISTS lab6."TB_FATO"
    ADD CONSTRAINT "FK_CLIENTE" FOREIGN KEY (id_cliente)
    REFERENCES lab6."DIM_CLIENTE" (id_cliente) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;


ALTER TABLE IF EXISTS lab6."TB_FATO"
    ADD CONSTRAINT "FK_TRANSPORTADORA" FOREIGN KEY (id_transportadora)
    REFERENCES lab6."DIM_TRANSPORTADORA" (id_transportadora) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;


ALTER TABLE IF EXISTS lab6."TB_FATO"
    ADD CONSTRAINT "FK_DEPOSITO" FOREIGN KEY (id_deposito)
    REFERENCES lab6."DIM_DEPOSITO" (id_deposito) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;


ALTER TABLE IF EXISTS lab6."TB_FATO"
    ADD CONSTRAINT "FK_ENTREGA" FOREIGN KEY (id_entrega)
    REFERENCES lab6."DIM_ENTREGA" (id_entrega) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;


ALTER TABLE IF EXISTS lab6."TB_FATO"
    ADD CONSTRAINT "FK_PAGAMENTO" FOREIGN KEY (id_pagamento)
    REFERENCES lab6."DIM_PAGAMENTO" (id_pagamento) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;


ALTER TABLE IF EXISTS lab6."TB_FATO"
    ADD CONSTRAINT "FK_FRETE" FOREIGN KEY (id_frete)
    REFERENCES lab6."DIM_FRETE" (id_frete) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;


ALTER TABLE IF EXISTS lab6."TB_FATO"
    ADD CONSTRAINT "FK_DATA" FOREIGN KEY (id_data)
    REFERENCES lab6."DIM_DATA" (id_data) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;





