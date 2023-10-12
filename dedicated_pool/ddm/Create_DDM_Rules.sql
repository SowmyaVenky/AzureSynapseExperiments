CREATE USER MaskingTestUser WITHOUT LOGIN;

GRANT SELECT ON SCHEMA::dbo TO MaskingTestUser;
  
-- impersonate for testing:
EXECUTE AS USER = 'MaskingTestUser';

SELECT top 10 * FROM dbo.users;

ALTER TABLE dbo.users
ALTER COLUMN firstName ADD MASKED WITH (FUNCTION = 'partial(2,"xxxx",0)');

ALTER TABLE dbo.users
ALTER COLUMN lastName ADD MASKED WITH (FUNCTION = 'partial(2,"xxxx",0)');

ALTER TABLE dbo.users
ALTER COLUMN streetName ADD MASKED WITH (FUNCTION = 'partial(2,"xxxx",0)');

ALTER TABLE dbo.users
ALTER COLUMN phone ADD MASKED WITH (FUNCTION = 'partial(2,"xxxx",0)');

ALTER TABLE dbo.users
ALTER COLUMN creditCard ADD MASKED WITH (FUNCTION = 'partial(2,"xxxx",0)');

ALTER TABLE dbo.users
ALTER COLUMN accountNumber ADD MASKED WITH (FUNCTION = 'default()');

ALTER TABLE dbo.users
ALTER COLUMN emailAddr ADD MASKED WITH (FUNCTION = 'email()');

ALTER TABLE dbo.users
ALTER COLUMN zip ADD MASKED WITH (FUNCTION = 'random(10000,99999)');

REVERT;