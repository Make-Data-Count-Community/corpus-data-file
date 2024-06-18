BEGIN;

-- Delete non-citation relation types from assertions
DELETE FROM public.assertions
WHERE relation_type_id is not null
    AND source_id = '3644e65a-1696-4cdf-9868-64e7539598d2'
    AND relation_type_id not in (
        'cites',
        'is-cited-by',
        'references',
        'is-referenced-by',
        'is-supplemented-by',
        'is-supplement-to'
    );

COMMIT;

