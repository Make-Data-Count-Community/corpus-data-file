BEGIN;

DELETE FROM assertions_affiliations
WHERE assertion_id IN (
    SELECT id FROM assertions
    WHERE source_type = 'crossref' AND relation_type_id = 'is-referenced-by'
);

DELETE FROM assertions_funders
WHERE assertion_id IN (
    SELECT id FROM assertions
    WHERE source_type = 'crossref' AND relation_type_id = 'is-referenced-by'
);

DELETE FROM assertions_subjects
WHERE assertion_id IN (
    SELECT id FROM assertions
    WHERE source_type = 'crossref' AND relation_type_id = 'is-referenced-by'
);

DELETE FROM assertions
WHERE source_type = 'crossref' AND relation_type_id = 'is-referenced-by';

COMMIT;