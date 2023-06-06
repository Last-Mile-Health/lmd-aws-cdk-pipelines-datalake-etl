CREATE TABLE "<schema>"."<table>" (
    id bigint identity(1, 1) ENCODE az64,
    date_inserted timestamp without time zone DEFAULT now() ENCODE az64,
    last_update_date timestamp without time zone DEFAULT now() ENCODE az64
) DISTSTYLE AUTO;