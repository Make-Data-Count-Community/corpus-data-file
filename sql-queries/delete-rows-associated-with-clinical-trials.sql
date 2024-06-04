BEGIN;

-- Delete non-citation relation types from assertions
DELETE FROM public.assertions
WHERE repository_id not in ('fef75a3c-6e48-4170-be9d-415601efb689', '2638e611-ff6f-49db-9b3e-702ecd16176b');

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

