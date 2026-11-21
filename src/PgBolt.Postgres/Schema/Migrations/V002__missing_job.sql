-- Ошибка на сервере откатывает всю пачку lease, включая уже прочитанные клиентом строки.
CREATE FUNCTION {schema}.missing_job(job_id uuid) RETURNS uuid
LANGUAGE plpgsql STRICT AS $$
BEGIN
    RAISE EXCEPTION 'PgBolt: missing jobs row for leased job %', job_id USING ERRCODE = 'no_data_found';
END;
$$;
