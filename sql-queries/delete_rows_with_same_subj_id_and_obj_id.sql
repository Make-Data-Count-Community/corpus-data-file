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

DROP TABLE IF EXISTS subj_id_obj_id;
CREATE TEMPORARY TABLE subj_id_obj_id AS
    SELECT *
    FROM assertions
	WHERE subj_id <> obj_id;

TRUNCATE TABLE assertions CASCADE;

INSERT INTO assertions
SELECT *
FROM subj_id_obj_id;

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