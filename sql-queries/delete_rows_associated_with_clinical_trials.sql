BEGIN;

DROP TABLE IF EXISTS assertions_affiliations_temp;
CREATE TEMPORARY TABLE assertions_affiliations_temp AS
SELECT * FROM assertions_affiliations;

DROP TABLE IF EXISTS assertions_funders_temp;
CREATE TEMPORARY TABLE assertions_funders_temp AS
SELECT * FROM assertions_funders;

DROP TABLE IF EXISTS assertions_subjects_temp;
CREATE TEMPORARY TABLE assertions_subjects_temp AS
SELECT * FROM assertions_subjects;

DROP TABLE IF EXISTS non_clinical_trial_assertions;
CREATE TEMPORARY TABLE non_clinical_trial_assertions AS
    SELECT *
    FROM assertions
	WHERE repository_id NOT IN ('fef75a3c-6e48-4170-be9d-415601efb689', '2638e611-ff6f-49db-9b3e-702ecd16176b') OR repository_id IS NULL;

TRUNCATE TABLE assertions CASCADE;

INSERT INTO assertions
SELECT *
FROM non_clinical_trial_assertions;

INSERT INTO assertions_affiliations
SELECT *
FROM assertions_affiliations_temp
WHERE assertion_id IN (SELECT id FROM assertions);

INSERT INTO assertions_subjects
SELECT *
FROM assertions_subjects_temp
WHERE assertion_id IN (SELECT id FROM assertions);

INSERT INTO assertions_funders
SELECT *
FROM assertions_funders_temp
WHERE assertion_id IN (SELECT id FROM assertions);

DROP TABLE non_clinical_trial_assertions;
DROP TABLE assertions_affiliations_temp;
DROP TABLE assertions_funders_temp;
DROP TABLE assertions_subjects_temp;

COMMIT;
