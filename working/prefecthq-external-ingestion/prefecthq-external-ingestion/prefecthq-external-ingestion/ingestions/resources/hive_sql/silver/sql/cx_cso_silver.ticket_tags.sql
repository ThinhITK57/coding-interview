WITH last_fact_cso_tickets AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY updated_at_ts DESC, id DESC
               ) rn
        FROM hive.cx_cso_raw.fact_cso_tickets ss
        where deleted is null
    ) t
    WHERE rn = 1
)

SELECT
    -- ===== identifiers =====
    t.id                                   AS ticket_id,
   
	trim(regexp_replace(tag, '✨', '')) AS tag,
    from_unixtime(created_at_ts) AS created_at,
    from_unixtime(updated_at_ts) AS updated_at
FROM last_fact_cso_tickets t
CROSS JOIN UNNEST(t.tags) AS u(tag)