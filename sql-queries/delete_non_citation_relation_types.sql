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

DROP TABLE IF EXISTS citation_relation_types_assertions;
CREATE TEMPORARY TABLE citation_relation_types_assertions AS
    SELECT *
    FROM public.assertions
    WHERE relation_type_id is null
        OR relation_type_id in (
            'cites',
            'is-cited-by',
            'references',
            'is-referenced-by',
            'is-supplemented-by',
            'is-supplement-to'
        );

TRUNCATE TABLE assertions CASCADE;

INSERT INTO assertions
SELECT *
FROM citation_relation_types_assertions;

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

DROP TABLE citation_relation_types_assertions;
DROP TABLE assertions_affiliations_temp;
DROP TABLE assertions_funders_temp;
DROP TABLE assertions_subjects_temp;

COMMIT;
