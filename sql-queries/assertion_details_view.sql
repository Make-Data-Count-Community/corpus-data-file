BEGIN;
  CREATE VIEW assertion_details_view AS
  SELECT
    assertions.id,
    assertions.created as created,
    assertions.updated as updated,
    COALESCE(
        (
            SELECT json_build_object('title', repo.title, 'external_id', repo.external_id)
            FROM repositories AS repo
            WHERE repo.id = assertions.repository_id
        ),
        '{}'::json
    ) AS repository,
    COALESCE(
        (
            SELECT json_build_object('title', publishers.title, 'external_id', publishers.external_id)
            FROM publishers
            WHERE publishers.id = assertions.publisher_id
        ),
        '{}'::json
    ) AS publisher,
    COALESCE(
        (
            SELECT json_build_object('title', journals.title, 'external_id', journals.external_id)
            FROM journals
            WHERE journals.id = assertions.journal_id
        ),
        '{}'::json
    ) AS journal,
    assertions.title,
    assertions.obj_id as objId,
    assertions.subj_id as subjId,
    assertions.published_date as publishedDate,
    assertions.accession_number as accessionNumber,
    assertions.doi,
    assertions.relation_type_id as relationTypeId,
    COALESCE(
        (
            SELECT sources.abbreviation
            FROM sources
            WHERE sources.id = assertions.source_id
        ),
        ''
    ) AS source,
    COALESCE(
        (
            SELECT JSON_AGG(sub.title)
            FROM subjects AS sub
            INNER JOIN assertions_subjects AS ass_sub ON sub.id = ass_sub.subject_id
            WHERE ass_sub.assertion_id = assertions.id
        ),
        '[]'::json
    ) AS subjects,
    COALESCE(
        (
            SELECT JSON_AGG(json_build_object('title', aff.title, 'external_id', aff.external_id))
            FROM affiliations AS aff
            INNER JOIN assertions_affiliations AS aa ON aff.id = aa.affiliation_id
            WHERE aa.assertion_id = assertions.id
        ),
        '[]'
    ) AS affiliations,
    COALESCE(
        (
            SELECT JSON_AGG(json_build_object('title', funders.title, 'external_id', funders.external_id))
            FROM funders
            INNER JOIN assertions_affiliations AS aa ON funders.id = aa.affiliation_id
            WHERE aa.assertion_id = assertions.id
        ),
        '[]'
    ) AS funders
  FROM
      assertions;
COMMIT;