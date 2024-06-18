BEGIN;

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