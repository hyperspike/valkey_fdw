/*-------------------------------------------------------------------------
 *
 *                foreign-data wrapper for Valkey
 *
 * Copyright (c) 2011, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence
 *
 * Author: Dave Page <dpage@pgadmin.org>
 *         Dan Molik <dan@hyperspike.io>
 *
 * IDENTIFICATION
 *                valkey_fdw/valkey_fdw--1.0.sql
 *
 *-------------------------------------------------------------------------
 */

CREATE FUNCTION valkey_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION valkey_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER valkey_fdw
  HANDLER valkey_fdw_handler
  VALIDATOR valkey_fdw_validator;
