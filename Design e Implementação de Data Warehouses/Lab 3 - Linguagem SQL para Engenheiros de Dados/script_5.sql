# Auditoria

CREATE TABLE lab3."TB_AUDIT" (
    schema_name TEXT not null,
    table_name TEXT not null,
    user_name TEXT,
    action_tstamp timestamp with time zone not null default current_timestamp,
    action TEXT NOT NULL check (action in ('I','D','U')),
    original_data TEXT,
    new_data TEXT,
    query TEXT
) WITH (fillfactor=100);

REVOKE all ON lab3."TB_AUDIT" FROM public;

CREATE INDEX IDX_logged_actions_schema_table
ON lab3."TB_AUDIT" (((schema_name||'.'||table_name)::TEXT));

CREATE INDEX IDX_logged_actions_action_tstamp
ON lab3."TB_AUDIT"(action_tstamp);

CREATE INDEX IDX_logged_actions_action
ON lab3."TB_AUDIT"(action);

CREATE OR REPLACE FUNCTION lab3.FUNC_AUDIT() RETURNS trigger AS $body$
DECLARE
    v_old_data TEXT;
    v_new_data TEXT;
BEGIN

    if (TG_OP = 'UPDATE') then
        v_old_data := ROW(OLD.*);
        v_new_data := ROW(NEW.*);
        INSERT INTO lab3."TB_AUDIT" (schema_name, table_name, user_name, action, original_data, new_data, query) 
        VALUES (TG_TABLE_SCHEMA::TEXT,TG_TABLE_NAME::TEXT,session_user::TEXT,substring(TG_OP,1,1), v_old_data, v_new_data, current_query());
        RETURN NEW;
    elsif (TG_OP = 'DELETE') then
        v_old_data := ROW(OLD.*);
        INSERT INTO lab3."TB_AUDIT" (schema_name, table_name, user_name, action, original_data, query)
        VALUES (TG_TABLE_SCHEMA::TEXT,TG_TABLE_NAME::TEXT,session_user::TEXT,substring(TG_OP,1,1), v_old_data, current_query());
        RETURN OLD;
    elsif (TG_OP = 'INSERT') then
        v_new_data := ROW(NEW.*);
        INSERT INTO lab3."TB_AUDIT" (schema_name, table_name, user_name, action, new_data, query)
        VALUES (TG_TABLE_SCHEMA::TEXT,TG_TABLE_NAME::TEXT,session_user::TEXT,substring(TG_OP,1,1), v_new_data, current_query());
        RETURN NEW;
    else
        RAISE WARNING '[lab3.IF_MODIFIED_FUNC] - Other action occurred: %, at %',TG_OP,now();
        RETURN NULL;
    end if;

EXCEPTION
    WHEN data_exception THEN
        RAISE WARNING '[lab3.IF_MODIFIED_FUNC] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
    WHEN unique_violation THEN
        RAISE WARNING '[lab3.IF_MODIFIED_FUNC] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
    WHEN others THEN
        RAISE WARNING '[lab3.IF_MODIFIED_FUNC] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
END;
$body$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, lab3;

CREATE TRIGGER "TG_REPORT1"
AFTER INSERT OR UPDATE OR DELETE ON lab3."TB_REPORT1"
FOR EACH ROW EXECUTE PROCEDURE lab3.FUNC_AUDIT();

SELECT * 
FROM lab3."TB_AUDIT";

SELECT * 
FROM lab3."TB_REPORT1";

INSERT INTO lab3."TB_REPORT1"("Nome_Subcategoria", "Media_Peso_Produto")
VALUES ('Fake', 100);

SELECT * 
FROM lab3."TB_AUDIT";

UPDATE lab3."TB_REPORT1"
SET "Nome_Subcategoria" = 'Roupas'
WHERE "Media_Peso_Produto" = 100;

SELECT * 
FROM lab3."TB_AUDIT";

DELETE FROM lab3."TB_REPORT1"
WHERE "Media_Peso_Produto" = 100;

SELECT * 
FROM lab3."TB_AUDIT";





