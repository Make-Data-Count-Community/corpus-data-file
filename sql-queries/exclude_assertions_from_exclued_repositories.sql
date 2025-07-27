-- delete only the records from excluded repositories, https://docs.google.com/spreadsheets/d/1urkBTlWQYrjZp46yYNqwnQTf6i2ME5BMlT5YUUULZh8/edit?gid=63854662#gid=63854662

DO $$
DECLARE
    batch_size INT := 1000;
    deleted_count INT;
    total_deleted INT := 0;
BEGIN
    LOOP
        -- Delete batch from child tables first
        WITH assertions_batch AS (
            SELECT a.id as assertion_id
            FROM assertions a
            WHERE a.repository_id IN (
                '9dedd73c-1938-447d-a7cf-5a46f7ba1ade', 'fef75a3c-6e48-4170-be9d-415601efb689',
                '7109c7ca-dd40-4dfc-a883-c3bd2ad93ea5', '2638e611-ff6f-49db-9b3e-702ecd16176b',
                'a4855df9-755e-41d6-bddf-4589461e303c', 'e387c003-d7e5-455d-b7b0-544e9251b1d0',
                'c2f38c5b-7a8b-4e95-a2db-5d5578570627', 'b46c885e-c755-4b03-b443-d174aacc68f0',
                'ed99c2e4-de5a-41a1-8925-55f3daa88606', '0ccefa3f-5c25-4191-945e-715ce1816f57',
                'ed46ce7e-be69-4c89-a82f-4f777fad96bd', '54753603-c263-4cc0-bd65-57c39b5a20f6'
            ) AND a.doi IS NULL
            LIMIT batch_size
        )
        DELETE FROM assertions_affiliations
        WHERE assertion_id IN (SELECT assertion_id FROM assertions_batch);

        WITH assertions_batch AS (
            SELECT a.id as assertion_id
            FROM assertions a
            WHERE a.repository_id IN (
                '583ce2c8-95fc-4033-bdf0-3884edf2d4d9', 'fef75a3c-6e48-4170-be9d-415601efb689',
                '7109c7ca-dd40-4dfc-a883-c3bd2ad93ea5', '2638e611-ff6f-49db-9b3e-702ecd16176b',
                'a4855df9-755e-41d6-bddf-4589461e303c', 'e387c003-d7e5-455d-b7b0-544e9251b1d0',
                'c2f38c5b-7a8b-4e95-a2db-5d5578570627', 'b46c885e-c755-4b03-b443-d174aacc68f0',
                'ed99c2e4-de5a-41a1-8925-55f3daa88606', '0ccefa3f-5c25-4191-945e-715ce1816f57',
                'e8991e13-ba77-45cb-8254-675f35fb349e', '54753603-c263-4cc0-bd65-57c39b5a20f6'
            ) AND a.doi IS NULL
            LIMIT batch_size
        )
        DELETE FROM assertions_subjects
        WHERE assertion_id IN (SELECT assertion_id FROM assertions_batch);

        WITH assertions_batch AS (
            SELECT a.id as assertion_id
            FROM assertions a
            WHERE a.repository_id IN (
                '583ce2c8-95fc-4033-bdf0-3884edf2d4d9', 'fef75a3c-6e48-4170-be9d-415601efb689',
                '7109c7ca-dd40-4dfc-a883-c3bd2ad93ea5', '2638e611-ff6f-49db-9b3e-702ecd16176b',
                'a4855df9-755e-41d6-bddf-4589461e303c', 'e387c003-d7e5-455d-b7b0-544e9251b1d0',
                'c2f38c5b-7a8b-4e95-a2db-5d5578570627', 'b46c885e-c755-4b03-b443-d174aacc68f0',
                'ed99c2e4-de5a-41a1-8925-55f3daa88606', '0ccefa3f-5c25-4191-945e-715ce1816f57',
                'e8991e13-ba77-45cb-8254-675f35fb349e', '54753603-c263-4cc0-bd65-57c39b5a20f6'
            ) AND a.doi IS NULL
            LIMIT batch_size
        )
        DELETE FROM assertions_funders
        WHERE assertion_id IN (SELECT assertion_id FROM assertions_batch);

        -- Delete from main table and count
        WITH assertions_batch AS (
            SELECT a.id as assertion_id
            FROM assertions a
            WHERE a.repository_id IN (
                '583ce2c8-95fc-4033-bdf0-3884edf2d4d9', 'fef75a3c-6e48-4170-be9d-415601efb689',
                '7109c7ca-dd40-4dfc-a883-c3bd2ad93ea5', '2638e611-ff6f-49db-9b3e-702ecd16176b',
                'a4855df9-755e-41d6-bddf-4589461e303c', 'e387c003-d7e5-455d-b7b0-544e9251b1d0',
                'c2f38c5b-7a8b-4e95-a2db-5d5578570627', 'b46c885e-c755-4b03-b443-d174aacc68f0',
                'ed99c2e4-de5a-41a1-8925-55f3daa88606', '0ccefa3f-5c25-4191-945e-715ce1816f57',
                'e8991e13-ba77-45cb-8254-675f35fb349e', '54753603-c263-4cc0-bd65-57c39b5a20f6'
            ) AND a.doi IS NULL
            LIMIT batch_size
        )
        DELETE FROM assertions
        WHERE id IN (SELECT assertion_id FROM assertions_batch);

        GET DIAGNOSTICS deleted_count = ROW_COUNT;
        total_deleted := total_deleted + deleted_count;

        COMMIT;

        RAISE NOTICE 'Deleted % records, total so far: %', deleted_count, total_deleted;

        -- Exit when no more records to delete
        EXIT WHEN deleted_count = 0;

        -- Small pause to reduce load
        PERFORM pg_sleep(0.1);
    END LOOP;

    RAISE NOTICE 'Deletion complete. Total records deleted: %', total_deleted;
END $$;
