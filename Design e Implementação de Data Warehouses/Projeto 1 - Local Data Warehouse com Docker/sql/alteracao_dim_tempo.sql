-- Incluindo mais uma coluna na tabela dim_tempo
ALTER TABLE IF EXISTS schema3.dim_tempo
    ADD COLUMN hora text;

-- Limpando as tabelas da hierarquia para reexecutar a carga
TRUNCATE TABLE schema3.fato_vendas;
TRUNCATE TABLE schema3.dim_tempo CASCADE;