SELECT usename, usesuper, usecreatedb 
FROM pg_user ;

CREATE USER new_admin_user WITH PASSWORD 'new_password';
ALTER USER new_admin_user WITH createuser;

GRANT ALL PRIVILEGES ON DATABASE dev TO new_admin_user;

GRANT SELECT, INSERT, UPDATE, DELETE, REFERENCES ON ALL TABLES IN SCHEMA public TO new_admin_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA public 
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO new_admin_user;