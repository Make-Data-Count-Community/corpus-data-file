BEGIN;

DELETE FROM funders
WHERE id NOT IN (
	SELECT funder_id FROM assertions_funders
);

DELETE FROM subjects
WHERE id NOT IN (
	SELECT subject_id FROM assertions_subjects
);

DELETE FROM affiliations
WHERE id NOT IN (
	SELECT affiliation_id FROM assertions_affiliations
);

REFRESH MATERIALIZED VIEW last_10_years_assertions;
REFRESH MATERIALIZED VIEW count_growth_per_day;
REFRESH MATERIALIZED VIEW facet_unique_counts;

COMMIT;