BEGIN;

-- Step 1: Delete affiliations where the title starts with '@' and remove corresponding rows from assertions_affiliations
DELETE FROM assertions_affiliations
WHERE affiliation_id IN (
    SELECT id FROM affiliations WHERE title ~ '^@[^\s]+(\s*[/.-]?\s*@[^\s]+)*$'
);

DELETE FROM affiliations 
WHERE title ~ '^@[^\s]+(\s*[/.-]?\s*@[^\s]+)*$';


-- Step 2: Delete affiliations with titles that contain only an email address and remove corresponding rows from assertions_affiliations
DELETE FROM assertions_affiliations
WHERE affiliation_id IN (
    '921126d3-3cae-4ba9-8827-da071363c7d6',
    '6d083246-2d86-4070-a155-949ada1e6ed4',
    'bb89798e-a740-42c2-b9e4-21581e70137b',
    '6c465df4-8c61-4198-ad82-40a067a80d32',
    'd5a9d4b5-901d-4a83-af3a-d8dd8d3cacb8',
    'be5c7808-ff2a-433d-8d82-b7f3764ef245',
    '1d62d4f3-4e0e-4c70-8a55-2bad9b10b699',
    '25071587-3848-47f8-ac52-8c8ac64fdc61',
    '84776ba1-a058-49dd-95ec-ac4175fa4591',
    '5cf0f0be-d1cf-4f75-874e-8ab4c41e9587'
);

DELETE FROM affiliations
WHERE id IN (
    '921126d3-3cae-4ba9-8827-da071363c7d6',
    '6d083246-2d86-4070-a155-949ada1e6ed4',
    'bb89798e-a740-42c2-b9e4-21581e70137b',
    '6c465df4-8c61-4198-ad82-40a067a80d32',
    'd5a9d4b5-901d-4a83-af3a-d8dd8d3cacb8',
    'be5c7808-ff2a-433d-8d82-b7f3764ef245',
    '1d62d4f3-4e0e-4c70-8a55-2bad9b10b699',
    '25071587-3848-47f8-ac52-8c8ac64fdc61',
    '84776ba1-a058-49dd-95ec-ac4175fa4591',
    '5cf0f0be-d1cf-4f75-874e-8ab4c41e9587'
);


-- Step 3: Update titles to remove email addresses from titles containing an email along with other text
UPDATE affiliations
SET title = regexp_replace(
                title, 
                '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}', 
                '', 
                'g'
            )
WHERE title ~ '^(?!.*\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b$).*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}).*$';


-- Step 4: Update rows where the title contains 'E‐mail:' or 'Electronic address:' to emptry string
WITH prefix_removed AS (
    -- Remove specific prefixes from titles
    SELECT 
        id,
        REGEXP_REPLACE(
            title, 
            'E‐mail:|Electronic address:|E-mail: |Electronic address|E-mail', 
            '', 
            'g'
        ) AS title_without_prefixes
    FROM 
        affiliations
    WHERE 
        title ~ 'E‐mail:|Electronic address:|E-mail: |Electronic address|E-mail'
)
UPDATE affiliations
SET title = prefix_removed.title_without_prefixes
FROM prefix_removed
WHERE affiliations.id = prefix_removed.id;


-- Step 5: Update titles to leading/trailing spaces, and special characters
WITH cleaned AS (
    SELECT 
        id,
        title,
        regexp_replace(
            regexp_replace(
                trim(both '\n' from trim(both ' ' from title)), -- Trim whitespace/newlines
                '^[\.,\*;:\s]+|[\.,\*;:\s]+$', '' -- Remove leading/trailing special characters ., *, ;, :, and spaces but keep parentheses
            ),
            '\s+', ' ', 'g' -- Normalize multiple spaces to a single space
        ) AS cleaned_title
    FROM 
        affiliations
    WHERE 
        title ~ '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}'
)
UPDATE affiliations
SET title = cleaned.cleaned_title
FROM cleaned
WHERE affiliations.id = cleaned.id;

-- Step 6: Remove trailing special characters including dots.

WITH cleaned AS (
    SELECT 
        id,
        title,
        regexp_replace(
            regexp_replace(
                trim(both '\n' from trim(both ' ' from title)), -- Trim whitespace/newlines
                '[\.,\*;:\s]+$', '' -- Remove trailing special characters ., *, ;, :, and spaces
            ),
            '\s+', ' ', 'g' -- Normalize multiple spaces to a single space
        ) AS cleaned_title
    FROM 
        affiliations
    WHERE 
        title ~ '[\.,\*;:\s]+$' -- Only target titles with trailing special characters
)
UPDATE affiliations
SET title = cleaned.cleaned_title
FROM cleaned
WHERE affiliations.id = cleaned.id;


-- Step 7: Update titles to remove outer parentheses
UPDATE affiliations
SET title = REGEXP_REPLACE(title, '^\((.*)\)$', '\1')
WHERE title ~ '^\(.*\)$';

-- Step 8: Remove leading special chars

WITH cleaned AS (
    SELECT 
        id,
        title,
        regexp_replace(
            regexp_replace(
                title, 
                '^[\.,\*;:\s]+', '' -- Remove leading special characters ., *, ;, :, and spaces
            ),
            '\s+', ' ', 'g' -- Normalize multiple spaces to a single space
        ) AS cleaned_title
    FROM 
        affiliations
    WHERE 
        title ~ '^[\.,\*;:\s]+' -- Only target titles with leading special characters
)
UPDATE affiliations
SET title = cleaned.cleaned_title
FROM cleaned
WHERE affiliations.id = cleaned.id;


-- # Funders
-- Step 9: Update titles to leading/trailing spaces, and special characters

WITH cleaned AS (
    SELECT id, title, 
        regexp_replace(
            regexp_replace(
                trim(both '\n' from trim(both ' ' from title)), 
                '^[\.,\*;:\s]+|[\.,\*;:\s]+$', '' -- Remove leading/trailing ., *, ;, :, and spaces
            ),
            '\s+', ' ', 'g' -- Normalize multiple spaces to a single space
        ) AS cleaned_title
    FROM funders
    WHERE title ~ '[\.,\*;:\s]+$|^[\.,\*;:\s]+'
)
UPDATE funders
SET title = cleaned.cleaned_title
FROM cleaned
WHERE funders.id = cleaned.id;

-- Step 10: Remove trailing special characters including dots.
WITH cleaned AS (
    SELECT 
        id,
        title,
        regexp_replace(
            regexp_replace(
                trim(both '\n' from trim(both ' ' from title)), -- Trim whitespace/newlines
                '[\.,\*;:\s]+$', '' -- Remove trailing special characters ., *, ;, :, and spaces
            ),
            '\s+', ' ', 'g' -- Normalize multiple spaces to a single space
        ) AS cleaned_title
    FROM 
        funders
    WHERE 
        title ~ '[\.,\*;:\s]+$' -- Only target titles with trailing special characters
)
UPDATE funders
SET title = cleaned.cleaned_title
FROM cleaned
WHERE funders.id = cleaned.id;

COMMIT;
