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

DROP TABLE IF EXISTS ranked_assertions;
CREATE TEMPORARY TABLE ranked_assertions AS
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY obj_id, subj_id, repository_id, publisher_id, journal_id, accession_number, source_id ORDER BY updated DESC) AS rn
    FROM assertions;

TRUNCATE TABLE assertions CASCADE;

INSERT INTO assertions (id,
                        type,
                        created,
                        updated,
                        activity_id,
                        repository_id,
                        publisher_id,
                        journal_id,
                        source_type,
                        title,
                        obj_id,
                        subj_id,
                        published_date,
                        accession_number,
                        doi,
                        relation_type_id,
                        source_id,
                        not_found,
                        retried
                    )
SELECT id,
        type,
        created,
        updated,
        activity_id,
        repository_id,
        publisher_id,
        journal_id,
        source_type,
        title,
        obj_id,
        subj_id,
        published_date,
        accession_number,
        doi,
        relation_type_id,
        source_id,
        not_found,
        retried
FROM ranked_assertions
WHERE rn = 1;

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

DROP TABLE ranked_assertions;
DROP TABLE assertions_affiliations_temp;
DROP TABLE assertions_funders_temp;
DROP TABLE assertions_subjects_temp;

COMMIT;
