BEGIN;

-- Delete clinical trials registries from assertions
DELETE FROM public.assertions
WHERE repository_id in ('fef75a3c-6e48-4170-be9d-415601efb689', '2638e611-ff6f-49db-9b3e-702ecd16176b');

COMMIT;

