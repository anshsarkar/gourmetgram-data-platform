CREATE DATABASE nimtable;
CREATE USER nimtable_user WITH PASSWORD 'password';
GRANT ALL PRIVILEGES ON DATABASE nimtable TO nimtable_user;

\c nimtable
GRANT ALL ON SCHEMA public TO nimtable_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO nimtable_user;
