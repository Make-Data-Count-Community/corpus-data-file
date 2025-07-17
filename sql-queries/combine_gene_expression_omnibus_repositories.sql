BEGIN;
	UPDATE assertions 
		SET repository_id = 'b2a4aa2b-db3f-456a-8e2b-7d935343385e' -- Gene Expression Omnibus (GEO) - Old/Existing
		WHERE repository_id = '80a1d446-267a-4468-a474-b69a6242a2b7'; -- Gene Expression Omnibus - new

	DELETE FROM repositories WHERE id = '80a1d446-267a-4468-a474-b69a6242a2b7';
COMMIT;