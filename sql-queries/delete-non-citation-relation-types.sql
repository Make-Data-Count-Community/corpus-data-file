BEGIN;

-- Delete non-citation relation types from assertions
DELETE FROM public.assertions
WHERE (relation_type_id is not null
AND relation_type_id not in ('cites', 'is-cited-by', 'references', 'is-referenced-by', 'is-supplemented-by', 'is-supplement-to'));

-- Cascade delete from assertions_affiliations
DELETE FROM public.assertions_affiliations
WHERE assertion_id NOT IN (SELECT id FROM public.assertions);

-- Cascade delete from assertions_funders
DELETE FROM public.assertions_funders
WHERE assertion_id NOT IN (SELECT id FROM public.assertions);

-- Cascade delete from assertions_subjects
DELETE FROM public.assertions_subjects
WHERE assertion_id NOT IN (SELECT id FROM public.assertions);

COMMIT;

