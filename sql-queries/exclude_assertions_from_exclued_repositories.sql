CREATE TABLE public.assertions_new (LIKE public.assertions INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES);

-- Insert only the records from valid repositories, https://docs.google.com/spreadsheets/d/1urkBTlWQYrjZp46yYNqwnQTf6i2ME5BMlT5YUUULZh8/edit?gid=63854662#gid=63854662
INSERT INTO public.assertions_new 
SELECT * FROM public.assertions 
WHERE repository_id IN (
    'f99f9396-1377-4423-8165-6731ba593ddf', '524e4405-f959-4e3c-ab4e-eecaa8ed23d5', '509bf805-f2a6-4758-94e4-ca28f49b7b54',
       'e5960008-7a81-46b1-b526-d7dbea7e2c93', '5f36c68f-bb46-4a21-9b95-6bb87de12aa0', '04c26ce0-5583-4dbc-98f0-465ab046bf1c',
       'f43825eb-5b72-4f1a-b716-dc7eec6d4206', 'c908c286-c01b-44c7-bac9-3bd53148d898', '876f8718-c8b2-4d05-95ff-ab00ba6fa56b',
       '345977e0-6fb8-476e-9742-0b8987e2fce8', '6087b2e9-ecbf-4898-8047-5f484f1bce2f', 'ffe70f6b-6db0-402e-86b7-cc07bacbbdc8',
       '21f8dd4d-4b5f-4922-91ad-c290da30797c', '8d9c72f8-7b96-4b5c-86b0-b3f0dd7d0b0d', '00363b65-f3ef-4fa9-8255-23ab269f4930',
       '9b207e9a-b4b8-439b-9464-f2c218c6af5f', 'b2a4aa2b-db3f-456a-8e2b-7d935343385e', '79760077-45df-4626-9675-60ee459aa283',
       '3df1efe9-85d0-4c29-850a-6f062e065bf1', '4371937f-226a-4381-b07a-1ccc0085f0fd', '490f73dd-7532-453b-bd37-a96a566d60ba',
       'c82c6040-e644-4d94-a54f-97f0236c7147', '66807551-597e-4088-9743-32690481f6ff', '75800c1d-b982-4542-b996-033781f70fa1',
       '8748538d-965e-4440-85cc-d9d1722e7ca9', 'b4440b59-ca28-4a67-a65f-2dc02fb0aa23', '1edec4bf-cfee-4296-8893-d1b0ca528f92',
       'd3ee57d1-bce4-437d-b054-e686d9abc727', '19ad31a7-e6d0-4547-ad14-1201d3c96dca', '1f463165-6957-491b-a1e1-e484540200f0',
       'd975db25-1124-4f7c-af45-54a5b7c6bb15', '0a60b1a9-041a-444e-bd6a-94caaab7591b', 'd201d41a-2b84-4ebc-91a4-afab8c481944',
       '806d986d-5756-4e20-a119-f78e83c626bc', 'b5966ef4-8bd3-4de8-aafb-396df8e75b0b', '23b16007-328b-46a1-84ee-19cd32995091',
       '31ffd918-669b-4d61-9470-784226277b5b', '87646104-e5ef-494b-b2f3-a46c9572e003', '69bb4f86-1d75-487e-971a-f446a2ef0792',
       'c56fbe18-f93b-478b-9674-00056bdeb887', '58d689da-7c8c-4ac1-90c9-69253d28f81f'
);

BEGIN;
ALTER TABLE public.assertions RENAME TO assertions_old;
ALTER TABLE public.assertions_new RENAME TO assertions;
COMMIT;