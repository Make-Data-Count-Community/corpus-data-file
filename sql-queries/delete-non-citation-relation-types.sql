BEGIN;
DELETE FROM public.assertions
WHERE (relation_type_id is not null
AND relation_type_id not in ('cites', 'is-cited-by', 'references', 'is-referenced-by', 'is-supplemented-by', 'is-supplement-to'))
COMMIT;

