BEGIN;
CREATE TABLE assertion_details_formatted AS
select
a.id,
a.created as created,
a.updated as updated,
case
    when r.title is not null or r.external_id is not null
        then json_build_object('title', r.title, 'external_id', r.external_id)
    else '{}'::json
end repository,
case
    when p.title is not null or p.external_id is not null
        then json_build_object('title', p.title, 'external_id', p.external_id)
    else '{}'::json
end publisher,
case
    when j.title is not null or j.external_id is not null
        then json_build_object('title', j.title, 'external_id', j.external_id)
    else '{}'::json
end journal,
a.title,
a.obj_id as objId,
a.subj_id as subjId,
a.published_date as publishedDate,
a.accession_number as accessionNumber,
a.doi,
a.relation_type_id as relationTypeId,
s.abbreviation as source
from assertions as a
left join repositories as r
    on r.id = a.repository_id
left join publishers as p
    on p.id = a.publisher_id
left join journals as j
    on j.id = a.journal_id
left join sources as s
    on s.id = a.source_id
group by
    a.id,
    r.title,
    r.external_id,
    p.title,
    p.external_id,
    j.title,
    j.external_id,
    s.abbreviation;
COMMIT;

BEGIN;
ALTER TABLE assertion_details_formatted add column affiliations json;
ALTER TABLE assertion_details_formatted ALTER COLUMN affiliations set DEFAULT '[]'::json;
CREATE TEMPORARY TABLE grouped_affiliations AS
    SELECT a.id,
    coalesce(json_agg(json_build_object('title', aff.title, 'external_id', aff.external_id)) filter (where aff.title is not null or aff.external_id is not null), '[]'::json) as affiliations
    FROM assertions a
    join assertions_affiliations aa
    on a.id = aa.assertion_id
    join affiliations aff
    ON aff.id = aa.affiliation_id
    GROUP BY a.id;
UPDATE assertion_details_formatted as adf
    set affiliations = ga.affiliations
    from grouped_affiliations ga
    where adf.id=ga.id;
COMMIT;
BEGIN;
ALTER TABLE assertion_details_formatted add column funders json;
ALTER TABLE assertion_details_formatted ALTER COLUMN funders set DEFAULT '[]'::json;
CREATE TEMPORARY TABLE grouped_funders AS
    SELECT a.id,
    coalesce(json_agg(json_build_object('title', f.title, 'external_id', f.external_id)) filter (where f.title is not null or f.external_id is not null), '[]'::json) as funders
    FROM assertions a
    join assertions_funders af
    on a.id = af.assertion_id
    join funders f
    ON f.id = af.funder_id
    GROUP BY a.id
UPDATE assertion_details_formatted as adf
    set funders = gf.funders
    from grouped_funders gf
    where adf.id=gf.id;
COMMIT;
ALTER TABLE assertion_details_formatted add column subjects json;
ALTER TABLE assertion_details_formatted ALTER COLUMN subjects set DEFAULT '[]'::json;
CREATE TEMPORARY TABLE grouped_subjects AS
    SELECT a.id,
    coalesce(json_agg(s.title) filter (where s.title is not null), '[]'::json) as subjects
    FROM assertions a
    join assertions_subjects asub
    on a.id = asub.assertion_id
    join subjects s
    ON s.id = asub.subject_id
    GROUP BY a.id
UPDATE assertion_details_formatted as adf
    set subjects = gs.subjects
    from grouped_subjects gs
    where adf.id=gs.id;
COMMIT;



