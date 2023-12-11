CREATE FUNCTION pgtle.register_background_worker(
    name text,
    fn regprocedure,
    db text)
RETURNS text STRICT
AS 'MODULE_PATHNAME', 'register_background_worker'
LANGUAGE C;

CREATE FUNCTION pgtle.describe_bgw_shmem()
RETURNS void STRICT
AS 'MODULE_PATHNAME', 'describe_bgw_shmem'
LANGUAGE C;
