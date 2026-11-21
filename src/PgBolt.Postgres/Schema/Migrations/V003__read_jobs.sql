-- EXECUTE планирует чтение по фактическим UUID даже при generic plan внешнего запроса.
CREATE FUNCTION {schema}.read_jobs(uuid[])
RETURNS TABLE (id uuid, type text, args jsonb, max_attempts integer, retry_delays integer[],
               parent_id uuid, batch_id uuid, trace_parent text, recurring_id text,
               scheduled_at timestamptz, continuation smallint)
-- VOLATILE даёт внутреннему SELECT свежий снимок после ожидания замены партиции при компактации.
LANGUAGE plpgsql VOLATILE STRICT ROWS 32 AS $$
BEGIN
    RETURN QUERY EXECUTE
        'SELECT id, type, args, max_attempts, retry_delays, parent_id, batch_id, trace_parent,
                recurring_id, scheduled_at, continuation FROM {schema}.jobs AS leased_jobs WHERE id = ANY($1)'
        USING $1;
END;
$$;
