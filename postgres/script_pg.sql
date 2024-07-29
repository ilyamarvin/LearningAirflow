CREATE SCHEMA report;
CREATE SCHEMA sync;

CREATE ROLE airflow WITH PASSWORD 'airflow' LOGIN;
GRANT USAGE ON SCHEMA sync to airflow;

CREATE TABLE IF NOT EXISTS report.issued_qty
(
    dt_load    TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    dt_date    DATE                        NOT NULL,
    wh_id      INT                         NOT NULL,
    qty_issued BIGINT                      NOT NULL,
    PRIMARY KEY (dt_date, wh_id)
);

CREATE OR REPLACE FUNCTION report.issued_qty_get(_date_from DATE, _date_to DATE, _wh_id INTEGER[]) RETURNS JSONB
    SECURITY DEFINER
    LANGUAGE plpgsql
AS
$$
BEGIN
    SET TIME ZONE 'Europe/Moscow';

    RETURN JSONB_BUILD_OBJECT('data', JSONB_AGG(ROW_TO_JSON(res)))
        FROM (SELECT wh_id,
                     qty_issued,
                     dt_date
              FROM report.issued_qty ass
              WHERE ass.dt_date >= _date_from::TIMESTAMP
                AND ass.dt_date < _date_to::TIMESTAMP + interval '1 day'
                AND ass.wh_id = ANY (_wh_id)) res;
END;
$$;

CREATE OR REPLACE PROCEDURE sync.issued_qty_importfromclick(_src JSONB)
    SECURITY DEFINER
    LANGUAGE plpgsql
AS
$$
BEGIN
    WITH cte AS (SELECT DISTINCT ON (src.dt_date, src.wh_id) src.dt_load,
                                                             src.dt_date,
                                                             src.wh_id,
                                                             src.qty_issued
                 FROM JSONB_TO_RECORDSET(_src) AS src(dt_load TIMESTAMP WITHOUT TIME ZONE,
                                                      dt_date DATE,
                                                      wh_id INT,
                                                      qty_issued BIGINT)
                 ORDER BY src.dt_date, src.wh_id, src.dt_load DESC)
    INSERT
    INTO report.issued_qty AS ass(dt_load,
                                     dt_date,
                                     wh_id,
                                     qty_issued)
    SELECT c.dt_load, c.dt_date, c.wh_id, c.qty_issued
    FROM cte c
    ON CONFLICT (dt_date, wh_id) DO UPDATE
        SET qty_issued = excluded.qty_issued,
            dt_load    = excluded.dt_load
    WHERE ass.dt_load < excluded.dt_load;
END;
$$;

select * from report.issued_qty;

select report.issued_qty_get('2024-07-28'::DATE, '2024-07-30'::DATE, ARRAY[10,11,12]);