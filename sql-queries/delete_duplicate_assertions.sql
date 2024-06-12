BEGIN;

WITH ranked_assertions AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY obj_id, subj_id, repository_id, publisher_id, journal_id, accession_number, source_id ORDER BY updated DESC) AS rn
    FROM assertions
)
DELETE FROM assertions
WHERE id IN (
    SELECT id
    FROM ranked_assertions
    WHERE rn > 1
);

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
