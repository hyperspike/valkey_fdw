
DROP EXTENSION IF EXISTS valkey_fdw CASCADE;
CREATE EXTENSION valkey_fdw;
CREATE SERVER valkey_server
	FOREIGN DATA WRAPPER valkey_fdw
	OPTIONS (url 'valkey+cluster://@VALKEY_IP@:6379');
CREATE USER MAPPING FOR PUBLIC SERVER valkey_server;
CREATE FOREIGN TABLE valkey_db0 (key text, val text) SERVER valkey_server;
INSERT INTO valkey_db0 (key, val) VALUES ( 'test', 'test');
