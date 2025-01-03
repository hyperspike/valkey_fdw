
/*-------------------------------------------------------------------------
 *
 *		  foreign-data wrapper for Valkey
 *
 * Copyright (c) 2011,2013 PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence
 *
 * Authors: Dave Page <dpage@pgadmin.org>
 *			Andrew Dunstan <andrew@dunslane.net>
 *			Dan Molik <dan@hyperspike.io>
 *
 * IDENTIFICATION
 *		  valkey_fdw/valkey_fdw.c
 *
 *-------------------------------------------------------------------------
 */

/* Debug mode */
/* #define DEBUG */

#include "postgres.h"

/* check that we are compiling for the right postgres version */
#if PG_VERSION_NUM < 170000 || PG_VERSION_NUM >= 180000
#error wrong Postgresql version this branch is only for 17.
#endif


#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#include <valkey/valkey.h>
#include <valkey/cluster.h>

#include "funcapi.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "mb/pg_wchar.h"
#include "nodes/pathnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/appendinfo.h"
#include "optimizer/inherit.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "parser/parsetree.h"
#include "storage/fd.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

#define PROCID_TEXTEQ 67

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct ValkeyFdwOption
{
	const char *optname;
	Oid			optcontext;		/* Oid of catalog in which option may appear */
};

/*
 * Valid options for valkey_fdw.
 *
 */
static struct ValkeyFdwOption valid_options[] =
{

	/* Connection options */
	{"address", ForeignServerRelationId},
	{"url", ForeignServerRelationId},
	{"port", ForeignServerRelationId},
	{"password", UserMappingRelationId},
	{"database", ForeignTableRelationId},

	/* table options */
	{"singleton_key", ForeignTableRelationId},
	{"tablekeyprefix", ForeignTableRelationId},
	{"tablekeyset", ForeignTableRelationId},
	{"tabletype", ForeignTableRelationId},

	/* Sentinel */
	{NULL, InvalidOid}
};

typedef enum
{
	PG_VALKEY_SCALAR_TABLE = 0,
	PG_VALKEY_HASH_TABLE,
	PG_VALKEY_LIST_TABLE,
	PG_VALKEY_SET_TABLE,
	PG_VALKEY_ZSET_TABLE
} valkey_table_type;

typedef enum
{
	VALKEY = 0,
	VALKEY_CLUSTER,
	VALKEY_TLS,
	VALKEY_TLS_CLUSTER,
} valkey_protocol;

typedef struct valkeyTableOptions
{
	char	   *address;
	int			port;
	char	   *username;
	char	   *password;
	valkey_protocol protocol;
	int			database;
	char	   *keyprefix;
	char	   *keyset;
	char	   *singleton_key;
	valkey_table_type table_type;
} valkeyTableOptions;



typedef struct
{
	char	   *svr_address;
	int			svr_port;
	char	   *svr_password;
	int			svr_database;
} ValkeyFdwPlanState;

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */

typedef struct ValkeyFdwExecutionState
{
	AttInMetadata        *attinmeta;
	valkeyContext        *context;
	valkeyClusterContext *cc;
	valkeyReply          *reply;
	long long             row;

	bool  cluster;
	bool  tls;
	bool  tls_verify;
	char *tls_ca_file;
	char *tls_ca;
	char *tls_cert_file;
	char *tls_cert;
	char *tls_key_file;
	char *tls_key;

	char	   *address;
	int			port;
	char	   *password;
	int			database;
	char	   *keyprefix;
	char	   *keyset;
	char	   *qual_value;
	char	   *singleton_key;

	valkey_table_type table_type;
	char	   *cursor_search_string;
	char	   *cursor_id;

	MemoryContext mctxt;
} ValkeyFdwExecutionState;

typedef struct ValkeyFdwModifyState
{
	valkeyContext        *context;
	valkeyClusterContext *cc;

	char	   *address;
	int			port;
	char	   *password;
	int			database;
	char	   *keyprefix;
	char	   *keyset;
	char	   *qual_value;
	char	   *singleton_key;
	Relation	rel;
	valkey_table_type table_type;
	List	   *target_attrs;
	int		   *targetDims;
	int			p_nums;
	int			keyAttno;
	Oid			array_elem_type;
	FmgrInfo   *p_flinfo;
} ValkeyFdwModifyState;

/* initial cursor */
#define ZERO "0"
/* valkey default is 10 - let's fetch 1000 at a time */
#define COUNT " COUNT 1000"

/*
 * SQL functions
 */
extern Datum valkey_fdw_handler(PG_FUNCTION_ARGS);
extern Datum valkey_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(valkey_fdw_handler);
PG_FUNCTION_INFO_V1(valkey_fdw_validator);

/*
 * FDW callback routines
 */
static void valkeyGetForeignRelSize(PlannerInfo *root,
					   RelOptInfo *baserel,
					   Oid foreigntableid);
static void valkeyGetForeignPaths(PlannerInfo *root,
					 RelOptInfo *baserel,
					 Oid foreigntableid);
static ForeignScan *valkeyGetForeignPlan(PlannerInfo *root,
					RelOptInfo *baserel,
					Oid foreigntableid,
					ForeignPath *best_path,
					List *tlist,
					List *scan_clauses,
					Plan *outer_plan);

static void valkeyExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void valkeyBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *valkeyIterateForeignScan(ForeignScanState *node);
static inline TupleTableSlot *valkeyIterateForeignScanMulti(ForeignScanState *node);
static inline TupleTableSlot *valkeyIterateForeignScanSingleton(ForeignScanState *node);
static void valkeyReScanForeignScan(ForeignScanState *node);
static void valkeyEndForeignScan(ForeignScanState *node);
static valkeyReply * valkeyClusterCommandToNodes(valkeyClusterContext *cc, const char *cmd, ...);
static valkeyReply * valkeyCopyReply(valkeyReply *reply);
static valkeyReply * valkeyAppendReply(valkeyReply *first, valkeyReply *second);

static valkey_protocol valkeyGetProtocol(char *address);
static char *valkeyGetAddress(char *url);
static char *valkeyGetUsername(char *url);
static char *valkeyGetPassword(char *url);

static List *valkeyPlanForeignModify(PlannerInfo *root,
					   ModifyTable *plan,
					   Index resultRelation,
					   int subplan_index);

static void valkeyBeginForeignModify(ModifyTableState *mtstate,
						ResultRelInfo *rinfo,
						List *fdw_private,
						int subplan_index,
						int eflags);

static TupleTableSlot *valkeyExecForeignInsert(EState *estate,
					   ResultRelInfo *rinfo,
					   TupleTableSlot *slot,
					   TupleTableSlot *planSlot);
static void valkeyEndForeignModify(EState *estate,
					  ResultRelInfo *rinfo);
static void valkeyAddForeignUpdateTargets(PlannerInfo *root,
										 Index rtindex,
										 RangeTblEntry *target_rte,
										 Relation target_relation);
static TupleTableSlot *valkeyExecForeignDelete(EState *estate,
					   ResultRelInfo *rinfo,
					   TupleTableSlot *slot,
					   TupleTableSlot *planSlot);
static TupleTableSlot *valkeyExecForeignUpdate(EState *estate,
					   ResultRelInfo *rinfo,
					   TupleTableSlot *slot,
					   TupleTableSlot *planSlot);

/*
 * Helper functions
 */
static bool valkeyIsValidOption(const char *option, Oid context);
static void valkeyGetOptions(Oid foreigntableid, valkeyTableOptions *options);
static void valkeyGetQual(Node *node, TupleDesc tupdesc, char **key,
						 char **value, bool *pushdown);
static char *process_valkey_array(valkeyReply *reply, valkey_table_type type);
static void check_reply(valkeyReply *reply, valkeyContext *context,
						int error_code, char *message, char *arg);
static void check_cluster_reply(valkeyReply *reply, valkeyClusterContext *cc,
						int error_code, char *message, char *arg);

/*
 * Name we will use for the junk attribute that holds the valkey key
 * for update and delete operations.
 */
#define VALKEYMODKEYNAME "__valkey_mod_key_name"

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
valkey_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

#ifdef DEBUG
	elog(NOTICE, "valkey_fdw_handler");
#endif

	fdwroutine->GetForeignRelSize = valkeyGetForeignRelSize;
	fdwroutine->GetForeignPaths = valkeyGetForeignPaths;
	fdwroutine->GetForeignPlan = valkeyGetForeignPlan;
	/* can't ANALYSE valkey */
	fdwroutine->AnalyzeForeignTable = NULL;
	fdwroutine->ExplainForeignScan = valkeyExplainForeignScan;
	fdwroutine->BeginForeignScan = valkeyBeginForeignScan;
	fdwroutine->IterateForeignScan = valkeyIterateForeignScan;
	fdwroutine->ReScanForeignScan = valkeyReScanForeignScan;
	fdwroutine->EndForeignScan = valkeyEndForeignScan;

	fdwroutine->PlanForeignModify = valkeyPlanForeignModify;		/* I U D */
	fdwroutine->BeginForeignModify = valkeyBeginForeignModify;	/* I U D */
	fdwroutine->ExecForeignInsert = valkeyExecForeignInsert;		/* I */
	fdwroutine->EndForeignModify = valkeyEndForeignModify;		/* I U D */

	fdwroutine->ExecForeignUpdate = valkeyExecForeignUpdate;		/* U */
	fdwroutine->ExecForeignDelete = valkeyExecForeignDelete;		/* D */
	fdwroutine->AddForeignUpdateTargets = valkeyAddForeignUpdateTargets; /* U D */

	PG_RETURN_POINTER(fdwroutine);
}

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses file_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
Datum
valkey_fdw_validator(PG_FUNCTION_ARGS)
{
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	char	   *svr_address = NULL;
	int			svr_port = 0;
	char	   *svr_password = NULL;
	int			svr_database = 0;
	valkey_table_type tabletype = PG_VALKEY_SCALAR_TABLE;
	char	   *tablekeyprefix = NULL;
	char	   *tablekeyset = NULL;
	char	   *singletonkey = NULL;
	ListCell   *cell;

#ifdef DEBUG
	elog(NOTICE, "valkey_fdw_validator");
#endif

	/*
	 * Check that only options supported by valkey_fdw, and allowed for the
	 * current object type, are given.
	 */
	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if (!valkeyIsValidOption(def->defname, catalog))
		{
			struct ValkeyFdwOption *opt;
			StringInfoData buf;

			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with list of valid options for the object.
			 */
			initStringInfo(&buf);
			for (opt = valid_options; opt->optname; opt++)
			{
				if (catalog == opt->optcontext)
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
									 opt->optname);
			}

			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", def->defname),
					 errhint("Valid options in this context are: %s",
							 buf.len ? buf.data : "<none>")
					 ));
		}

		if (strcmp(def->defname, "address") == 0)
		{
			if (svr_address)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("conflicting or redundant options: "
									   "address (%s)", defGetString(def))
								));

			svr_address = defGetString(def);
		}
		else if (strcmp(def->defname, "port") == 0)
		{
			if (svr_port)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: port (%s)",
								defGetString(def))
						 ));

			svr_port = atoi(defGetString(def));
		}
		if (strcmp(def->defname, "password") == 0)
		{
			if (svr_password)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: password")
								));

			svr_password = defGetString(def);
		}
		else if (strcmp(def->defname, "database") == 0)
		{
			if (svr_database)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: database "
								"(%s)", defGetString(def))
						 ));

			svr_database = atoi(defGetString(def));
		}
		else if (strcmp(def->defname, "singleton_key ") == 0)
		{
			if (tablekeyset)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting options: tablekeyset(%s) and "
								"singleton_key (%s)", tablekeyset,
								defGetString(def))
						 ));
			if (tablekeyprefix)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting options: tablekeyprefix(%s) and "
								"singleton_key (%s)", tablekeyprefix,
								defGetString(def))
						 ));
			if (singletonkey)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: "
								"singleton_key (%s)", defGetString(def))
						 ));

			singletonkey = defGetString(def);
		}
		else if (strcmp(def->defname, "tablekeyprefix") == 0)
		{
			if (tablekeyset)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting options: tablekeyset(%s) and "
								"tablekeyprefix (%s)", tablekeyset,
								defGetString(def))
						 ));
			if (singletonkey)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting options: singleton_key(%s) and "
								"tablekeyprefix (%s)", singletonkey,
								defGetString(def))
						 ));
			if (tablekeyprefix)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: "
								"tablekeyprefix (%s)", defGetString(def))
						 ));

			tablekeyprefix = defGetString(def);
		}
		else if (strcmp(def->defname, "tablekeyset") == 0)
		{
			if (tablekeyprefix)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
					   errmsg("conflicting options: tablekeyprefix (%s) and "
							  "tablekeyset (%s)", tablekeyprefix,
							  defGetString(def))
						 ));
			if (singletonkey)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting options: singleton_key(%s) and "
								"tablekeyset (%s)", singletonkey,
								defGetString(def))
						 ));
			if (tablekeyset)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: "
								"tablekeyset (%s)", defGetString(def))
						 ));

			tablekeyset = defGetString(def);
		}
		else if (strcmp(def->defname, "tabletype") == 0)
		{
			char	   *typeval = defGetString(def);

			if (tabletype)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: tabletype "
								"(%s)", typeval)));
			if (strcmp(typeval, "hash") == 0)
				tabletype = PG_VALKEY_HASH_TABLE;
			else if (strcmp(typeval, "list") == 0)
				tabletype = PG_VALKEY_LIST_TABLE;
			else if (strcmp(typeval, "set") == 0)
				tabletype = PG_VALKEY_SET_TABLE;
			else if (strcmp(typeval, "zset") == 0)
				tabletype = PG_VALKEY_ZSET_TABLE;
			else
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid tabletype (%s) - must be hash, "
								"list, set or zset", typeval)));
		}
	}

	PG_RETURN_VOID();
}


/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool
valkeyIsValidOption(const char *option, Oid context)
{
	struct ValkeyFdwOption *opt;

#ifdef DEBUG
	elog(NOTICE, "valkeyIsValidOption");
#endif

	for (opt = valid_options; opt->optname; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
			return true;
	}
	return false;
}

/*
 * Fetch the options for a valkey_fdw foreign table.
 */
static void
valkeyGetOptions(Oid foreigntableid, valkeyTableOptions *table_options)
{
	ForeignTable *table;
	ForeignServer *server;
	UserMapping *mapping;
	List	   *options;
	ListCell   *lc;
	char *url = NULL;

#ifdef DEBUG
	elog(NOTICE, "valkeyGetOptions");
#endif

	/*
	 * Extract options from FDW objects. We only need to worry about server
	 * options for Valkey
	 *
	 */
	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	mapping = GetUserMapping(GetUserId(), table->serverid);

	options = NIL;
	options = list_concat(options, table->options);
	options = list_concat(options, server->options);
	options = list_concat(options, mapping->options);

	/* Loop through the options, and get the server/port */
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "url") == 0)
		{
			url = defGetString(def);
			table_options->protocol = valkeyGetProtocol(url);
			table_options->address = valkeyGetAddress(url);
			table_options->username = valkeyGetUsername(url);
			table_options->password = valkeyGetPassword(url);
		}

		if (strcmp(def->defname, "tablekeyprefix") == 0)
			table_options->keyprefix = defGetString(def);

		if (strcmp(def->defname, "tablekeyset") == 0)
			table_options->keyset = defGetString(def);

		if (strcmp(def->defname, "singleton_key") == 0)
			table_options->singleton_key = defGetString(def);

		if (strcmp(def->defname, "tabletype") == 0)
		{
			char	   *typeval = defGetString(def);

			if (strcmp(typeval, "hash") == 0)
				table_options->table_type = PG_VALKEY_HASH_TABLE;
			else if (strcmp(typeval, "list") == 0)
				table_options->table_type = PG_VALKEY_LIST_TABLE;
			else if (strcmp(typeval, "set") == 0)
				table_options->table_type = PG_VALKEY_SET_TABLE;
			else if (strcmp(typeval, "zset") == 0)
				table_options->table_type = PG_VALKEY_ZSET_TABLE;
			/* XXX detect error here */
		}
	}

	/* Default values, if required */
	if (!table_options->address)
		table_options->address = "127.0.0.1";

	if (!table_options->port)
		table_options->port = 6379;

	if (!table_options->database)
		table_options->database = 0;
}

/*
 * get protocol from address
 */
static valkey_protocol valkeyGetProtocol(char *address)
{
	valkey_protocol protocol = VALKEY; /* default */
	char *colon = strchr(address, ':');
	if (colon)
	{
		if (strncmp(address, "valkey", colon - address) == 0)
			protocol = VALKEY;
		else if (strncmp(address, "valkey+cluster", colon - address) == 0)
			protocol = VALKEY_CLUSTER;
		else if (strncmp(address, "valkey+tls", colon - address) == 0)
			protocol = VALKEY_TLS;
		else if (strncmp(address, "valkey+cluster+tls", colon - address) == 0)
			protocol = VALKEY_TLS_CLUSTER;
		else if (strncmp(address, "valkey+tls+cluster", colon - address) == 0)
			protocol = VALKEY_TLS_CLUSTER;
		else
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid protocol in address: %s", address)
					 ));
	}
	return protocol;
}

/*
 * get address from url
 */
static char *valkeyGetAddress(char *url)
{
	char *colon = strchr(url, ':');
	char *at = strchr(url, '@');
	char *slash;
	if (at)
	{
		slash = strchr(pstrdup(at + 1), '/');
		if (slash)
			return pnstrdup(at + 1, slash - at - 1);
		else
			return pstrdup(at + 1);
	}
	else
	{
		if (colon)
		{
			slash = strchr(colon + 3, '/');
			if (slash)
				return pnstrdup(colon + 3, slash - colon - 3);
			else
				return pstrdup(colon + 3);
		}
		else
		{
			slash = strchr(url, '/');
			if (slash)
				return pnstrdup(url, slash - url);
			else
				return pstrdup(url);
		}
	}
}

/*
 * Get username from url
 */
static char *valkeyGetUsername(char *url)
{
	char *colon = strchr(url, ':');
	char *at = strchr(url, '@');
	if (at)
	{
		if (colon)
			return pnstrdup(url, colon - url);
		else
			return pstrdup(url);
	}
	else
		return NULL;
}

/*
 * Get password from url
 */
static char *valkeyGetPassword(char *url)
{
	char *colon = strchr(url, ':');
	char *at = strchr(url, '@');
	if (at)
	{
		if (colon)
			return pnstrdup(colon + 1, at - colon - 1);
		else
			return pnstrdup(url, at - url);
	}
	else
		return NULL;
}

static void
valkeyGetForeignRelSize(PlannerInfo *root,
					   RelOptInfo *baserel,
					   Oid foreigntableid)
{
	ValkeyFdwPlanState *fdw_private;
	valkeyTableOptions table_options;

	valkeyContext        *context;
	valkeyClusterContext *cc;
	valkeyReply          *reply;
	valkeyClusterNode    *node;

	valkeyClusterNodeIterator iter;

	long long int dbsize    = 0;
	bool          iscluster = false;
	struct timeval timeout = {1, 500000};

#ifdef DEBUG
	elog(NOTICE, "valkeyGetForeignRelSize");
#endif


	/*
	 * Fetch options. Get everything so we don't need to re-fetch it later in
	 * planning.
	 */
	fdw_private = (ValkeyFdwPlanState *) palloc(sizeof(ValkeyFdwPlanState));
	baserel->fdw_private = (void *) fdw_private;

	table_options.address = NULL;
	table_options.port = 0;
	table_options.password = NULL;
	table_options.database = 0;
	table_options.keyprefix = NULL;
	table_options.keyset = NULL;
	table_options.singleton_key = NULL;
	table_options.table_type = PG_VALKEY_SCALAR_TABLE;

	valkeyGetOptions(foreigntableid, &table_options);
	fdw_private->svr_address = table_options.address;
	fdw_private->svr_password = table_options.password;
	fdw_private->svr_port = table_options.port;
	fdw_private->svr_database = table_options.database;

	/* Connect to the database */
	if (table_options.protocol == VALKEY_CLUSTER || table_options.protocol == VALKEY_TLS_CLUSTER)
	{
		iscluster = true;
		cc = valkeyClusterContextInit();
		//valkeyClusterContextSetTimeout(cc, timeout);
		valkeyClusterSetOptionAddNodes(cc, table_options.address);
		if (valkeyClusterConnect2(cc) == VALKEY_ERR)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
					 errmsg("failed to connect to Valkey Cluster %s: %s",
							table_options.address,
							cc->errstr)
					 ));
	}
	else
	{
		context = valkeyConnectWithTimeout(table_options.address,
											table_options.port, timeout);
		if (context->err)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
					 errmsg("failed to connect to Valkey: %s", context->errstr)
					 ));
	}

	/* Authenticate */
	if (table_options.password)
	{
		if (table_options.protocol == VALKEY_CLUSTER || table_options.protocol == VALKEY_TLS_CLUSTER)
			reply = valkeyClusterCommand(cc, "AUTH %s", table_options.password);
		else
			reply = valkeyCommand(context, "AUTH %s", table_options.password);

		if (!reply)
		{
			valkeyFree(context);
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
					 errmsg("failed to authenticate to valkey: %d",
							context->err)));
		}

		freeReplyObject(reply);
	}

	/* Select the appropriate database */
	if (!(table_options.protocol == VALKEY_CLUSTER || table_options.protocol == VALKEY_TLS_CLUSTER))
	{
		reply = valkeyCommand(context, "SELECT %d", table_options.database);
		if (!reply)
		{
			valkeyFree(context);
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
					 errmsg("failed to select database %d: %d",
							table_options.database, context->err)
					 ));
		}
		freeReplyObject(reply);
	}

	/* Execute a query to get the table size */
#if 0

	/*
	 * KEYS is potentiallyexpensive, so this test is disabled and we use a
	 * fairly dubious heuristic instead.
	 */
	if (table_options.keyprefix)
	{
		/* it's a pity there isn't an NKEYS command in Valkey */
		int			len = strlen(table_options.keyprefix) + 2;
		char	   *buff = palloc(len * sizeof(char));

		snprintf(buff, len, "%s*", table_options.keyprefix);
		reply = valkeyCommand(context, "KEYS %s", buff);
	}
	else
#endif
	if (table_options.singleton_key)
	{
		switch (table_options.table_type)
		{
			case PG_VALKEY_SCALAR_TABLE:
				baserel->rows = 1;
				return;
			case PG_VALKEY_HASH_TABLE:
				if (table_options.protocol == VALKEY_CLUSTER || table_options.protocol == VALKEY_TLS_CLUSTER)
					reply = valkeyClusterCommand(cc, "HLEN %s", table_options.singleton_key);
				else
					reply = valkeyCommand(context, "HLEN %s", table_options.singleton_key);
				break;
			case PG_VALKEY_LIST_TABLE:
				if (table_options.protocol == VALKEY_CLUSTER || table_options.protocol == VALKEY_TLS_CLUSTER)
					reply = valkeyClusterCommand(cc, "LLEN %s", table_options.singleton_key);
				else
					reply = valkeyCommand(context, "LLEN %s", table_options.singleton_key);
				break;
			case PG_VALKEY_SET_TABLE:
				if (table_options.protocol == VALKEY_CLUSTER || table_options.protocol == VALKEY_TLS_CLUSTER)
					reply = valkeyClusterCommand(cc, "SCARD %s", table_options.singleton_key);
				else
					reply = valkeyCommand(context, "SCARD %s", table_options.singleton_key);
				break;
			case PG_VALKEY_ZSET_TABLE:
				if (table_options.protocol == VALKEY_CLUSTER || table_options.protocol == VALKEY_TLS_CLUSTER)
					reply = valkeyClusterCommand(cc, "ZCARD %s", table_options.singleton_key);
				else
					reply = valkeyCommand(context, "ZCARD %s", table_options.singleton_key);
				break;
			default:
				;
		}
	}
	else if (table_options.keyset)
	{
		if (table_options.protocol == VALKEY_CLUSTER || table_options.protocol == VALKEY_TLS_CLUSTER)
			reply = valkeyClusterCommand(cc, "SCARD %s", table_options.keyset);
		else
			reply = valkeyCommand(context, "SCARD %s", table_options.keyset);
	}
	else
	{
		if (!(table_options.protocol == VALKEY_CLUSTER || table_options.protocol == VALKEY_TLS_CLUSTER))
			reply = valkeyCommand(context, "DBSIZE");
		else
		{

			valkeyClusterInitNodeIterator(&iter, cc);
			while ((node = valkeyClusterNodeNext(&iter)) != NULL)
			{
				reply = valkeyClusterCommandToNode(cc, node, "DBSIZE");
				if (reply)
				{
					dbsize += reply->integer;
					freeReplyObject(reply);
				}
				else
				{
					ereport(ERROR,
							(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
							 errmsg("failed to get the database size: %d, %s", cc->err, cc->err ? cc->errstr : "")
							 ));
					valkeyClusterFree(cc);
				}
			}
		}
	}

	if (!reply)
	{
		if (!(table_options.protocol == VALKEY_CLUSTER || table_options.protocol == VALKEY_TLS_CLUSTER))
		{
			ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("failed to get the database size: %d", context->err)
				 ));
			valkeyFree(context);
		}
	}

#if 0
	if (reply->type == VALKEY_REPLY_ARRAY)
		baserel->rows = reply->elements;
	else
#endif
	if (table_options.keyprefix)
		baserel->rows = reply->integer / 20;
	else
		baserel->rows = iscluster ? dbsize : reply->integer;

	if (table_options.protocol == VALKEY_CLUSTER || table_options.protocol == VALKEY_TLS_CLUSTER)
		valkeyClusterFree(cc);
	else
	{
		freeReplyObject(reply);
		valkeyFree(context);
	}
}

/*
 * valkeyGetForeignPaths
 *		Create possible access paths for a scan on the foreign table
 *
 *		Currently we don't support any push-down feature, so there is only one
 *		possible access path, which simply returns all records in valkey.
 */
static void
valkeyGetForeignPaths(PlannerInfo *root,
					 RelOptInfo *baserel,
					 Oid foreigntableid)
{
	ValkeyFdwPlanState *fdw_private = baserel->fdw_private;

	Cost		startup_cost,
				total_cost;

#ifdef DEBUG
	elog(NOTICE, "valkeyGetForeignPaths");
#endif

	if (strcmp(fdw_private->svr_address, "127.0.0.1") == 0 ||
		strcmp(fdw_private->svr_address, "localhost") == 0)
		startup_cost = 10;
	else
		startup_cost = 25;

	total_cost = startup_cost + baserel->rows;


	/* Create a ForeignPath node and add it as only possible path */
	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
									 NULL,      /* default pathtarget */
									 baserel->rows,
									 startup_cost,
									 total_cost,
									 NIL,		/* no pathkeys */
									 NULL,		/* no outer rel either */
									 NULL,      /* no extra plan */
									 NIL,       /* no fdw_restrictinfo list */
									 NIL));		/* no fdw_private data */

}

static ForeignScan *
valkeyGetForeignPlan(PlannerInfo *root,
					RelOptInfo *baserel,
					Oid foreigntableid,
					ForeignPath *best_path,
					List *tlist,
					List *scan_clauses,
					Plan *outer_plan)
{
	Index		scan_relid = baserel->relid;

#ifdef DEBUG
	elog(NOTICE, "valkeyGetForeignPlan");
#endif

	/*
	 * We have no native ability to evaluate restriction clauses, so we just
	 * put all the scan_clauses into the plan node's qual list for the
	 * executor to check.  So all we have to do here is strip RestrictInfo
	 * nodes from the clauses and ignore pseudoconstants (which will be
	 * handled elsewhere).
	 */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Create the ForeignScan node */
	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							NIL,	/* no expressions to evaluate */
							NIL,	/* no private state either */
							NIL,    /* no custom tlist */
							NIL,    /* no remote quals */
							outer_plan);
}

/*
 * fileExplainForeignScan
 *		Produce extra output for EXPLAIN
 */
static void
valkeyExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	valkeyReply *reply;

	ValkeyFdwExecutionState *festate = (ValkeyFdwExecutionState *) node->fdw_state;

	bool iscluster = false;
	if (festate->cc)
		iscluster = true;
#ifdef DEBUG
	elog(NOTICE, "valkeyExplainForeignScan");
#endif

	if (!es->costs)
		return;

	/*
	 * Execute a query to get the table size
	 *
	 * See above for more details.
	 */

	if (festate->keyset)
	{
		if (iscluster)
			reply = valkeyClusterCommand(festate->cc, "SCARD %s", festate->keyset);
		else
			reply = valkeyCommand(festate->context, "SCARD %s", festate->keyset);
	}
	else
	{
		if (!iscluster)
			reply = valkeyCommand(festate->context, "DBSIZE");
	}

	if (!reply)
	{
		if (!iscluster)
		{
			valkeyFree(festate->context);
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
				errmsg("failed to get the table size: %d", festate->context->err)
					 ));
		}
	}

	if (reply->type == VALKEY_REPLY_ERROR)
	{
		char	   *err = pstrdup(reply->str);

		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
				 errmsg("failed to get the table size: %s", err)
				 ));
	}

	if (iscluster) // this is complicated and we'll have to build an iterator
	{
		ExplainPropertyInteger("Foreign Valkey Table Size", "b",
						festate->keyprefix ? reply->integer / 20 :
						12,
						es);
	}
	else
	{
		ExplainPropertyInteger("Foreign Valkey Table Size", "b",
						festate->keyprefix ? reply->integer / 20 :
						reply->integer,
						es);

		freeReplyObject(reply);
	}
}

/*
 * valkeyBeginForeignScan
 *		Initiate access to the database
 */
static void
valkeyBeginForeignScan(ForeignScanState *node, int eflags)
{
	valkeyTableOptions table_options;
	valkeyContext *context;
	valkeyClusterContext *cc;
	valkeyReply *reply;
	bool iscluster = false;
	char	   *qual_key = NULL;
	char	   *qual_value = NULL;
	bool		pushdown = false;
	ValkeyFdwExecutionState *festate;
	struct timeval timeout = {1, 500000};

#ifdef DEBUG
	elog(NOTICE, "BeginForeignScan");
#endif

	table_options.address = NULL;
	table_options.port = 0;
	table_options.password = NULL;
	table_options.database = 0;
	table_options.keyprefix = NULL;
	table_options.keyset = NULL;
	table_options.singleton_key = NULL;
	table_options.table_type = PG_VALKEY_SCALAR_TABLE;


	/* Fetch options  */
	valkeyGetOptions(RelationGetRelid(node->ss.ss_currentRelation),
					&table_options);
	if (table_options.protocol == VALKEY_CLUSTER || table_options.protocol == VALKEY_TLS_CLUSTER)
	{
		iscluster = true;
		cc = valkeyClusterContextInit();
		//valkeyClusterContextSetTimeout(cc, timeout);
		valkeyClusterSetOptionAddNodes(cc, table_options.address);
		if (valkeyClusterConnect2(cc) == VALKEY_ERR)
		{
			ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
				 errmsg("Valkey Cluster not supported for scans %s", cc->errstr)
				 ));
			valkeyClusterFree(cc);
		}
	}
	else
	{
		/* Connect to the server */
		context = valkeyConnectWithTimeout(table_options.address,
										  table_options.port, timeout);
		if (context->err)
		{
			valkeyFree(context);
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
					 errmsg("failed to connect to Valkey: %s", context->errstr)
					 ));
		}
	}
	/* Authenticate */
	if (table_options.password)
	{
		if (iscluster)
			reply = valkeyClusterCommand(cc, "AUTH %s", table_options.password);
		else
			reply = valkeyCommand(context, "AUTH %s", table_options.password);

		if (!reply)
		{
			if (iscluster)
			{
				valkeyClusterFree(cc);
				ereport(ERROR,
						(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
					errmsg("failed to authenticate to valkey: %s", cc->errstr)
					 ));
			}
			else
			{
				valkeyFree(context);
				ereport(ERROR,
						(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
					errmsg("failed to authenticate to valkey: %s", context->errstr)
					 ));
			}
		}

		freeReplyObject(reply);
	}

	/* Select the appropriate database does not apply in cluster mode*/
	if (!iscluster)
	{
		reply = valkeyCommand(context, "SELECT %d", table_options.database);
		if (!reply)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
					 errmsg("failed to select database %d: %s",
							table_options.database, context->errstr)
					 ));
			valkeyFree(context);
		}
		if (reply->type == VALKEY_REPLY_ERROR)
		{
			char	   *err = pstrdup(reply->str);

			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
					 errmsg("failed to select database %d: %s",
							table_options.database, err)
					 ));
		}
		freeReplyObject(reply);
	}

	/* See if we've got a qual we can push down */
	if (node->ss.ps.plan->qual)
	{
		ListCell   *lc;

		foreach(lc, node->ss.ps.plan->qual)
		{
			/* Only the first qual can be pushed down to Valkey */
			Expr  *state = lfirst(lc);

			valkeyGetQual((Node *) state,
						 node->ss.ss_currentRelation->rd_att,
						 &qual_key, &qual_value, &pushdown);
			if (pushdown)
				break;
		}
	}

	/* Stash away the state info we have already */
	festate = (ValkeyFdwExecutionState *) palloc(sizeof(ValkeyFdwExecutionState));
	node->fdw_state = (void *) festate;
	festate->context = context;
	festate->cc = cc;
	festate->reply = NULL;
	festate->row = 0;
	festate->address = table_options.address;
	festate->port = table_options.port;
	festate->keyprefix = table_options.keyprefix;
	festate->keyset = table_options.keyset;
	festate->singleton_key = table_options.singleton_key;
	festate->table_type = table_options.table_type;
	festate->cursor_id = NULL;
	festate->cursor_search_string = NULL;

	festate->qual_value = pushdown ? qual_value : NULL;

	/* OK, we connected. If this is an EXPLAIN, bail out now */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/*
	 * We're going to use the current scan-lived context to
	 * store the pstrduped cusrsor id.
	 */
	festate->mctxt = CurrentMemoryContext;

	/* Execute the query */
	if (festate->singleton_key)
	{
		/*
		 * We're not using cursors for now for singleton key tables. The
		 * theory is that we don't expect them to be so large in normal use
		 * that we would get any significant benefit from doing so, and in any
		 * case scanning them in a single step is not going to tie things up
		 * like scannoing the whole Valkey database could.
		 */

		switch (table_options.table_type)
		{
			case PG_VALKEY_SCALAR_TABLE:
				if (iscluster)
					reply = valkeyClusterCommand(cc, "GET %s", festate->singleton_key);
				else
					reply = valkeyCommand(context, "GET %s", festate->singleton_key);
				break;
			case PG_VALKEY_HASH_TABLE:
				/* the singleton case where a qual pushdown makes most sense */
				if (qual_value && pushdown)
					if (iscluster)
						reply = valkeyClusterCommand(cc, "HGET %s %s", festate->singleton_key, qual_value);
					else
						reply = valkeyCommand(context, "HGET %s %s", festate->singleton_key, qual_value);
				else
					if (iscluster)
						reply = valkeyClusterCommand(cc, "HGETALL %s", festate->singleton_key);
					else
						reply = valkeyCommand(context, "HGETALL %s", festate->singleton_key);
				break;
			case PG_VALKEY_LIST_TABLE:
				if (iscluster)
					reply = valkeyClusterCommand(cc, "LRANGE %s 0 -1", table_options.singleton_key);
				else
					reply = valkeyCommand(context, "LRANGE %s 0 -1", table_options.singleton_key);
				break;
			case PG_VALKEY_SET_TABLE:
				if (iscluster)
					reply = valkeyClusterCommand(cc, "SMEMBERS %s", table_options.singleton_key);
				else
					reply = valkeyCommand(context, "SMEMBERS %s", table_options.singleton_key);
				break;
			case PG_VALKEY_ZSET_TABLE:
				if (iscluster)
					reply = valkeyClusterCommand(cc, "ZRANGEBYSCORE %s -inf inf WITHSCORES", table_options.singleton_key);
				else
					reply = valkeyCommand(context, "ZRANGEBYSCORE %s -inf inf WITHSCORES", table_options.singleton_key);
				break;
			default:
				;
		}
	}
	else if (qual_value && pushdown)
	{
		/*
		 * if we have a qual, make sure it's a member of the keyset or has the
		 * right prefix if either of these options is specified.
		 *
		 * If not set row to -1 to indicate failure
		 */
		if (festate->keyset)
		{
			valkeyReply *sreply;

			if (iscluster)
				sreply = valkeyClusterCommand(cc, "SISMEMBER %s %s",
								  festate->keyset, qual_value);
			else
				sreply = valkeyCommand(context, "SISMEMBER %s %s",
								  festate->keyset, qual_value);
			if (!sreply)
			{
				if (iscluster)
				{
					ereport(ERROR,
						(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
						 errmsg("SISMEMBER failed to list keys: %s", cc->errstr)
						 ));
					valkeyClusterFree(cc);
				}
				else
				{
					ereport(ERROR,
						(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
						 errmsg("SISMEMBER failed to list keys: %s", context->errstr)
						 ));
					valkeyFree(festate->context);
				}
			}
			if (sreply->type == VALKEY_REPLY_ERROR)
			{
				char	   *err = pstrdup(sreply->str);

				freeReplyObject(sreply);
				ereport(ERROR,
						(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
						 errmsg("SISMEMBER failed to list keys: %s", err)
						 ));

			}

			if (sreply->integer != 1)
				festate->row = -1;

		}
		else if (festate->keyprefix)
		{
			if (strncmp(qual_value, festate->keyprefix,
						strlen(festate->keyprefix)) != 0)
				festate->row = -1;
		}

		/*
		 * For a qual we don't want to scan at all, just check that the key
		 * exists. We do this check in adddition to the keyset/keyprefix
		 * checks, is any, so we know the item is really there.
		 */

		if (iscluster)
			reply = valkeyClusterCommand(cc, "EXISTS %s", qual_value);
		else
			reply = valkeyCommand(context, "EXISTS %s", qual_value);
		if (reply->integer == 0)
			festate->row = -1;

	}
	else
	{
		/* no qual - do a cursor scan */
		if (festate->keyset)
		{
			festate->cursor_search_string = "SSCAN %s %s" COUNT;
			if (iscluster)
				reply = valkeyClusterCommand(cc, festate->cursor_search_string, festate->keyset, ZERO);
			else
				reply = valkeyCommand(context, festate->cursor_search_string,  festate->keyset, ZERO);
		}
		else if (festate->keyprefix)
		{
			festate->cursor_search_string = "SCAN %s MATCH %s*" COUNT;
			if (iscluster)
				reply = valkeyClusterCommand(cc, festate->cursor_search_string,
								 ZERO, festate->keyprefix);
			else
				reply = valkeyCommand(context, festate->cursor_search_string,
								 ZERO, festate->keyprefix);
		}
		else
		{
			festate->cursor_search_string = "SCAN %s" COUNT;
			if (iscluster) {
#ifdef DEBUG
				elog(NOTICE, "BeginForeignScan assign festate->reply, cursor_search_string");
#endif
				reply = valkeyClusterCommandToNodes(cc, festate->cursor_search_string, ZERO);
			} else
				reply = valkeyCommand(context, festate->cursor_search_string, ZERO);
		}
	}

	if (!reply)
	{
		if (iscluster)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
					 errmsg("failed to list keys: %s/%s", festate->cursor_search_string, cc->errstr)
					 ));
			valkeyClusterFree(cc);
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
					 errmsg("failed to list keys: %s/%s", festate->cursor_search_string, context->errstr)
					 ));
			valkeyFree(festate->context);
		}
	}
	else if (reply->type == VALKEY_REPLY_ERROR)
	{
		char	   *err = pstrdup(reply->str);

		freeReplyObject(reply);
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
				 errmsg("failed somehow: %s", err)
				 ));
	}

	/* Store the additional state info */
	festate->attinmeta =
		TupleDescGetAttInMetadata(node->ss.ss_currentRelation->rd_att);

	if (festate->singleton_key)
	{
#ifdef DEBUG
		elog(NOTICE, "BeginForeignScan assign festate->reply, singleton_key");
#endif
		festate->reply = reply;
	}
	else if (festate->row > -1 && festate->qual_value == NULL)
	{
		valkeyReply *cursor = reply->element[0];
		if (cursor->type == VALKEY_REPLY_STRING)
		{
			if (cursor->len == 1 && cursor->str[0] == '0')
				festate->cursor_id = NULL;
			else
				festate->cursor_id = pstrdup(cursor->str);
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
					 errmsg("wrong reply type %d", cursor->type)
					 ));
		}
		/* for cursors, this is the list of elements */
		festate->reply = reply->element[1];
	}
}

static valkeyReply * valkeyClusterCommandToNodes(valkeyClusterContext *cc, const char *format, ...)
{
	valkeyReply               *reply;
	valkeyClusterNodeIterator  iter;
	valkeyClusterNode         *node;
	valkeyReply               *clusterReply = NULL;

	va_list  ap;
	char    *cmd;
	int      i;
	int      err = 0;

	va_start(ap, format);
	err = vasprintf(&cmd, format, ap);
	if (err < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("failed to format command: %d", err)
				 ));
	}
	va_end(ap);
#ifdef DEBUG
	elog(NOTICE, "valkeyClusterCommandToNodes %s", cmd);
#endif
	valkeyClusterInitNodeIterator(&iter, cc);
	while ((node = valkeyClusterNodeNext(&iter)) != NULL)
	{
#ifdef DEBUG
		elog(NOTICE, "valkeyClusterCommandToNode %d, %s %s", i++, node->name, cmd);
#endif
		reply = valkeyClusterCommandToNode(cc, node, cmd);
		if (reply && reply->type != VALKEY_REPLY_ERROR)
		{
			clusterReply = valkeyAppendReply(clusterReply, reply);
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
					 errmsg("failed to get the database size: %d, %s", cc->err, cc->err ? cc->errstr : "")
					 ));
			//valkeyClusterFree(cc);
		}
#ifdef DEBUG
		elog(NOTICE, "valkeyClusterCommandToNode %d, %s %s done, freeing", i, node->name, cmd);
#endif
		freeReplyObject(reply);
	}
	//pfree(cmd);
	return clusterReply;
}

static valkeyReply * valkeyCopyReply(valkeyReply *src) {
	valkeyReply *dst = NULL;
#ifdef DEBUG
	elog(NOTICE, "valkeyCopyReply");
#endif
	if (src == NULL) {
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("failed to copy reply: src is NULL")
				 ));
		return NULL;
	}
	if (dst == NULL) {
		dst = palloc(sizeof(valkeyReply));
		memset(dst, 0, sizeof(valkeyReply));
		Assert(dst != NULL);
	}
	dst->type = src->type;
	dst->elements = src->elements;
	switch (dst->type) {
	case VALKEY_REPLY_STRING:
		dst->str = palloc(src->len);
		memset(dst->str, 0, src->len);
		Assert(dst->str != NULL);
		dst->len = src->len;
		memcpy(dst->str, src->str, src->len);
		break;
	case VALKEY_REPLY_INTEGER:
		dst->integer = src->integer;
		break;
	case VALKEY_REPLY_ARRAY:
		dst->element = palloc(sizeof(valkeyReply *) * dst->elements);
		memset(dst->element, 0, sizeof(valkeyReply *) * dst->elements);
		Assert(dst->element != NULL);
		// printf("src->elements: %ld\n", dst->elements);
		for (int i = 0; i < dst->elements; i++) {
			dst->element[i] = NULL;
			dst->element[i] = valkeyCopyReply(src->element[i]);
		}
		break;
	default:
		break;
	}
#ifdef DEBUG
	elog(NOTICE, "valkeyCopyReply done");
#endif
	return dst;
}

static valkeyReply *
valkeyAppendReply(valkeyReply *first, valkeyReply *second)
{
	valkeyReply *dst;
#ifdef DEBUG
	elog(NOTICE, "valkeyAppendReply");
#endif
	if (second == NULL) {
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("failed to append reply: second is NULL")
				 ));
		return NULL;
	}
	if (first == NULL) {
		return valkeyCopyReply(second);
	}
	if (first->type != VALKEY_REPLY_ARRAY || second->type != VALKEY_REPLY_ARRAY) {
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("failed to append reply: %d, %d", first->type, second->type)
				 ));
		return NULL;
	}
#ifdef DEBUG
	elog(NOTICE, "valkeyAppendReply building dst");
#endif
	dst = palloc(sizeof(valkeyReply));
	memset(dst, 0, sizeof(valkeyReply));
	Assert(dst != NULL);
	dst->type = VALKEY_REPLY_ARRAY;
	dst->elements = 2;
	dst->element = palloc(sizeof(valkeyReply *) * dst->elements);
	memset(dst->element, 0, sizeof(valkeyReply *) * dst->elements);
	Assert(dst->element != NULL);
	dst->element[0] = NULL;
	dst->element[0] = valkeyCopyReply(first->element[0]);

#ifdef DEBUG
	elog(NOTICE, "valkeyAppendReply building dst->element[1]");
#endif
	dst->element[1] = NULL;
	dst->element[1] = valkeyCopyReply(first->element[1]);
	dst->element[1]->elements += second->element[1]->elements;
	dst->element[1]->element = palloc(sizeof(valkeyReply *) * dst->element[1]->elements);
	memset(dst->element[1]->element, 0, sizeof(valkeyReply *) * dst->element[1]->elements);
	Assert(dst->element[1]->element != NULL);
#ifdef DEBUG
	elog(NOTICE, "valkeyAppendReply copying first elements");
#endif
	for (int i = 0; i < first->element[1]->elements; i++) {
		dst->element[1]->element[i] = NULL;
		dst->element[1]->element[i] = valkeyCopyReply(first->element[1]->element[i]);
	}
#ifdef DEBUG
	elog(NOTICE, "valkeyAppendReply copying second elements");
#endif
	for (int i = 0; i < second->element[1]->elements; i++) {
		dst->element[1]->element[first->element[1]->elements + i] = NULL;
		dst->element[1]->element[first->element[1]->elements + i] = valkeyCopyReply(second->element[1]->element[i]);
	}
#ifdef DEBUG
	elog(NOTICE, "valkeyAppendReply done");
#endif
/*
	freeReplyObject(first);
	elog(NOTICE, "valkeyAppendReply done free first");
*/
	return dst;
}

/*
 * valkeyIterateForeignScan
 *		Read next record from the data file and store it into the
 *		ScanTupleSlot as a virtual tuple
 *
 * We have now spearated this into two streams of logic - one
 * for singleton key tables and one for multi-key tables.
 */

static TupleTableSlot *
valkeyIterateForeignScan(ForeignScanState *node)
{
	ValkeyFdwExecutionState *festate = (ValkeyFdwExecutionState *) node->fdw_state;
#ifdef DEBUG
	elog(NOTICE, "valkeyIterateForeignScan");
#endif
	if (festate->singleton_key)
		return valkeyIterateForeignScanSingleton(node);
	else
		return valkeyIterateForeignScanMulti(node);
}

static inline TupleTableSlot *
valkeyIterateForeignScanMulti(ForeignScanState *node)
{
	bool		found;
	valkeyReply *reply = 0;
	char	   *key;
	char	   *data = 0;
	char	  **values;
	HeapTuple	tuple;
	bool        iscluster = false;

	ValkeyFdwExecutionState *festate = (ValkeyFdwExecutionState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	if (festate->cc != NULL)
		iscluster = true;

#ifdef DEBUG
	elog(NOTICE, "valkeyIterateForeignScanMulti");
#endif

	/* Cleanup */
	ExecClearTuple(slot);

	/* Get the next record, and set found */
	found = false;

	/*
	 * If we're out of rows on the cursor, fetch the next set. Keep going
	 * until we get a result back that actually has some rows.
	 */
	while (festate->cursor_id != NULL &&
		   festate->row >= festate->reply->elements)
	{
#ifdef DEBUG
		elog(NOTICE, "valkeyIterateForeignScanMulti fetch next set");
#endif
		valkeyReply *creply;
		valkeyReply *cursor;

		Assert(festate->qual_value == NULL);

		if (festate->keyset)
		{
#ifdef DEBUG
			elog(NOTICE, "valkeyIterateForeignScanMulti get next set '%s'", festate->cursor_search_string);
#endif

			if (iscluster)
				creply = valkeyClusterCommand(festate->cc,
								  festate->cursor_search_string,
								  festate->keyset, festate->cursor_id);
			else
				creply = valkeyCommand(festate->context,
								  festate->cursor_search_string,
								  festate->keyset, festate->cursor_id);
		}
		else if (festate->keyprefix)
		{
#ifdef DEBUG
			elog(NOTICE, "valkeyIterateForeignScanMulti get next set '%s'", festate->cursor_search_string);
#endif
			if (iscluster)
				creply = valkeyClusterCommand(festate->cc,
								  festate->cursor_search_string,
								  festate->keyprefix, festate->cursor_id);
			else
				creply = valkeyCommand(festate->context,
								  festate->cursor_search_string,
								  festate->keyprefix, festate->cursor_id);
		}
		else
		{
#ifdef DEBUG
			elog(NOTICE, "valkeyIterateForeignScanMulti get next set '%s'", festate->cursor_search_string);
#endif
			if (iscluster)
				creply = valkeyClusterCommand(festate->cc,
								  festate->cursor_search_string,
								  festate->cursor_id);
			else
				creply = valkeyCommand(festate->context,
								  festate->cursor_search_string,
								  festate->cursor_id);
		}

		if (!creply)
		{
			if (iscluster)
			{
				ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
					 errmsg("failed to list keys: %s",
							festate->cc->errstr)
					 ));
				valkeyClusterFree(festate->cc);
			}
			else
			{
				ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
					 errmsg("failed to list keys: %s",
							festate->context->errstr)
					 ));
				valkeyFree(festate->context);
			}
		}
		else if (creply->type == VALKEY_REPLY_ERROR)
		{
			char	   *err = pstrdup(creply->str);

			freeReplyObject(creply);
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
					 errmsg("failed somehow: %s", err)
					 ));
		}

		cursor = creply->element[0];

		if (cursor->type == VALKEY_REPLY_STRING)
		{

			MemoryContext oldcontext;
			oldcontext = MemoryContextSwitchTo(festate->mctxt);
			pfree(festate->cursor_id);
			if (cursor->len == 1 && cursor->str[0] == '0')
				festate->cursor_id = NULL;
			else
				festate->cursor_id = pstrdup(cursor->str);
			MemoryContextSwitchTo(oldcontext);
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
					 errmsg("wrong reply type %d", cursor->type)
					 ));
		}

		festate->reply = creply->element[1];
		festate->row = 0;
	}

#ifdef DEBUG
	elog(NOTICE, "valkeyIterateForeignScanMulti get next row");
#endif
	/*
	 * -1 means we failed the qual test, so there are no rows or we've already
	 * processed the qual
	 */

	if (festate->row > -1 &&
		(festate->qual_value != NULL ||
		 (festate->row < festate->reply->elements)))
	{
		/*
		 * Get the row, check the result type, and handle accordingly. If it's
		 * nil, we go ahead and get the next row.
		 */
#ifdef DEBUG
		elog(NOTICE, "valkeyIterateForeignScanMulti get next row loop");
#endif
		do
		{

			key = festate->qual_value != NULL ?
				festate->qual_value :
				festate->reply->element[festate->row]->str;
			switch (festate->table_type)
			{
				case PG_VALKEY_HASH_TABLE:
					if (iscluster)
						reply = valkeyClusterCommand(festate->cc,
										 "HGETALL %s", key);
					else
						reply = valkeyCommand(festate->context,
										 "HGETALL %s", key);
					break;
				case PG_VALKEY_LIST_TABLE:
					if (iscluster)
						reply = valkeyClusterCommand(festate->cc,
										 "LRANGE %s 0 -1", key);
					else
						reply = valkeyCommand(festate->context,
										 "LRANGE %s 0 -1", key);
					break;
				case PG_VALKEY_SET_TABLE:
					if (iscluster)
						reply = valkeyClusterCommand(festate->cc,
										 "SMEMBERS %s", key);
					else
						reply = valkeyCommand(festate->context,
										 "SMEMBERS %s", key);
					break;
				case PG_VALKEY_ZSET_TABLE:
					if (iscluster)
						reply = valkeyClusterCommand(festate->cc,
										 "ZRANGE %s 0 -1", key);
					else
						reply = valkeyCommand(festate->context,
										 "ZRANGE %s 0 -1", key);
					break;
				case PG_VALKEY_SCALAR_TABLE:
				default:
					if (iscluster)
						reply = valkeyClusterCommand(festate->cc,
										 "GET %s", key);
					else
						reply = valkeyCommand(festate->context,
										 "GET %s", key);
			}

			if (!reply)
			{
				if (iscluster)
				{
					valkeyClusterFree(festate->cc);
					ereport(ERROR, (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_REPLY),
						 errmsg("failed to get the value for key \"%s\": %s",
								key, festate->cc->errstr)
								));
				}
				else
				{
					valkeyFree(festate->context);
					ereport(ERROR, (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_REPLY),
						 errmsg("failed to get the value for key \"%s\": %s",
								key, festate->context->errstr)
								));
				}
				freeReplyObject(festate->reply);
			}

			festate->row++;

		} while ((reply->type == VALKEY_REPLY_NIL ||
				  reply->type == VALKEY_REPLY_STATUS ||
				  reply->type == VALKEY_REPLY_ERROR) &&
				 festate->qual_value == NULL &&
				 festate->row < festate->reply->elements);

#ifdef DEBUG
		elog(NOTICE, "valkeyIterateForeignScanMulti get next row done");
#endif
		if (festate->qual_value != NULL || festate->row <= festate->reply->elements)
		{
			/*
			 * Now, deal with the different data types we might have got from
			 * Valkey.
			 */

			switch (reply->type)
			{
				case VALKEY_REPLY_INTEGER:
					data = (char *) palloc(sizeof(char) * 64);
					snprintf(data, 64, "%lld", reply->integer);
					found = true;
					break;

				case VALKEY_REPLY_STRING:
					data = reply->str;
					found = true;
					break;

				case VALKEY_REPLY_ARRAY:
					data = process_valkey_array(reply, festate->table_type);
					found = true;
					break;
			}
		}

		/* make sure we don't try to process the qual row twice */
		if (festate->qual_value != NULL)
			festate->row = -1;
	}

#ifdef DEBUG
	elog(NOTICE, "valkeyIterateForeignScanMulti build tuple");
#endif

	/* Build the tuple */
	if (found)
	{
		values = (char **) palloc(sizeof(char *) * 2);
		values[0] = key;
		values[1] = data;
		tuple = BuildTupleFromCStrings(festate->attinmeta, values);
		ExecStoreHeapTuple(tuple, slot, false);
	}
#ifdef DEBUG
	elog(NOTICE, "valkeyIterateForeignScan cleanup");
#endif
	/* Cleanup */
	if (reply)
		freeReplyObject(reply);

	return slot;
}

static inline TupleTableSlot *
valkeyIterateForeignScanSingleton(ForeignScanState *node)
{
	bool		found;
	char	   *key = NULL;
	char	   *data = NULL;
	char	  **values;
	HeapTuple	tuple;
	bool        iscluster = false;

	ValkeyFdwExecutionState *festate = (ValkeyFdwExecutionState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	if (festate->cc != NULL)
		iscluster = true;

#ifdef DEBUG
	elog(NOTICE, "valkeyIterateForeignScanSingleton");
#endif

	/* Cleanup */
	ExecClearTuple(slot);

	if (festate->row < 0)
		return slot;

	/* Get the next record, and set found */
	found = false;

	if (festate->table_type == PG_VALKEY_SCALAR_TABLE)
	{
		festate->row = -1;		/* just one row for a scalar */
		switch (festate->reply->type)
		{
			case VALKEY_REPLY_INTEGER:
				key = (char *) palloc(sizeof(char) * 64);
				snprintf(key, 64, "%lld", festate->reply->integer);
				found = true;
				break;

			case VALKEY_REPLY_STRING:
				key = festate->reply->str;
				found = true;
				break;

			case VALKEY_REPLY_ARRAY:
				freeReplyObject(festate->reply);
				if (iscluster)
				{
					valkeyClusterFree(festate->cc);
					ereport(ERROR, (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_REPLY),
						 errmsg("not expecting an array for a singleton scalar table")
						 ));
				}
				else
				{
					valkeyFree(festate->context);
					ereport(ERROR, (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_REPLY),
						 errmsg("not expecting an array for a singleton scalar table")
						 ));
				}
				break;
		}
	}
	else if (festate->table_type == PG_VALKEY_HASH_TABLE && festate->qual_value)
	{
		festate->row = -1;		/* just one row for qual'd search in a hash */
		key = festate->qual_value;
		switch (festate->reply->type)
		{
			case VALKEY_REPLY_INTEGER:
				data = (char *) palloc(sizeof(char) * 64);
				snprintf(data, 64, "%lld", festate->reply->integer);
				found = true;
				break;

			case VALKEY_REPLY_STRING:
				data = festate->reply->str;
				found = true;
				break;

			case VALKEY_REPLY_ARRAY:
				freeReplyObject(festate->reply);
				if (iscluster)
				{
					valkeyClusterFree(festate->cc);
					ereport(ERROR, (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_REPLY),
						 errmsg("not expecting an array for a single hash property: %s", festate->qual_value)
						 ));
				}
				else
				{
					valkeyFree(festate->context);
					ereport(ERROR, (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_REPLY),
						 errmsg("not expecting an array for a single hash property: %s", festate->qual_value)
						 ));
				}
				break;
		}
	}
	else if (festate->row < festate->reply->elements)
	{
		/* everything else comes in as an array reply type */
		found = true;
		key = festate->reply->element[festate->row]->str;
		festate->row++;
		if (festate->table_type == PG_VALKEY_HASH_TABLE ||
			festate->table_type == PG_VALKEY_ZSET_TABLE)
		{
			valkeyReply *dreply = festate->reply->element[festate->row];

			switch (dreply->type)
			{
				case VALKEY_REPLY_INTEGER:
					data = (char *) palloc(sizeof(char) * 64);
					snprintf(key, 64, "%lld", dreply->integer);
					break;

				case VALKEY_REPLY_STRING:
					data = dreply->str;
					break;

				case VALKEY_REPLY_ARRAY:
					freeReplyObject(festate->reply);
					if (iscluster)
					{
						valkeyClusterFree(festate->cc);
						ereport(ERROR, (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_REPLY),
							 errmsg("not expecting array for a hash value or zset score")
							 ));
					}
					else
					{
						valkeyFree(festate->context);
						ereport(ERROR, (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_REPLY),
									errmsg("not expecting array for a hash value or zset score")
									));
					}
					break;
			}
			festate->row++;
		}
	}

	/* Build the tuple */
	values = (char **) palloc(sizeof(char *) * 2);

	if (found)
	{
		values[0] = key;
		values[1] = data;
		tuple = BuildTupleFromCStrings(festate->attinmeta, values);
		ExecStoreHeapTuple(tuple, slot, false);
	}

	return slot;
}

/*
 * valkeyEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void
valkeyEndForeignScan(ForeignScanState *node)
{
	ValkeyFdwExecutionState *festate = (ValkeyFdwExecutionState *) node->fdw_state;
#ifdef DEBUG
	elog(NOTICE, "valkeyEndForeignScan start");
#endif

	/* if festate is NULL, we are in EXPLAIN; nothing to do */
	if (festate)
	{
#ifdef DEBUG
		elog(NOTICE, "valkeyEndForeignScan freereply");
#endif
		//if (festate->reply)
		//	freeReplyObject(festate->reply);

#ifdef DEBUG
		elog(NOTICE, "valkeyEndForeignScan freeclustercontext");
#endif
		if (festate->cc)
			valkeyClusterFree(festate->cc);
		else
		{
#ifdef DEBUG
			elog(NOTICE, "valkeyEndForeignScan freecontext");
#endif
			if (festate->context)
				valkeyFree(festate->context);
		}
	}
#ifdef DEBUG
	elog(NOTICE, "valkeyEndForeignScan done");
#endif
}

/*
 * valkeyReScanForeignScan
 *		Rescan table, possibly with new parameters
 */
static void
valkeyReScanForeignScan(ForeignScanState *node)
{
	ValkeyFdwExecutionState *festate = (ValkeyFdwExecutionState *) node->fdw_state;

#ifdef DEBUG
	elog(NOTICE, "valkeyReScanForeignScan");
#endif

	if (festate->row > -1)
		festate->row = 0;
}

static void
valkeyGetQual(Node *node, TupleDesc tupdesc, char **key, char **value, bool *pushdown)
{
	*key = NULL;
	*value = NULL;
	*pushdown = false;

	if (!node)
		return;

	if (IsA(node, OpExpr))
	{
		OpExpr	   *op = (OpExpr *) node;
		Node	   *left,
				   *right;
		Index		varattno;

		if (list_length(op->args) != 2)
			return;

		left = list_nth(op->args, 0);

		if (!IsA(left, Var))
			return;

		varattno = ((Var *) left)->varattno;

		right = list_nth(op->args, 1);

		if (IsA(right, Const))
		{
			StringInfoData buf;

			initStringInfo(&buf);

			/* And get the column and value... */
			*key = NameStr(TupleDescAttr(tupdesc, varattno - 1)->attname);
			*value = TextDatumGetCString(((Const *) right)->constvalue);

			/*
			 * We can push down this qual if: - The operatory is TEXTEQ - The
			 * qual is on the key column
			 */
			if (op->opfuncid == PROCID_TEXTEQ && strcmp(*key, "key") == 0)
				*pushdown = true;

			return;
		}
	}

	return;
}


static char *
process_valkey_array(valkeyReply *reply, valkey_table_type type)
{
	StringInfo	res = makeStringInfo();
	bool		need_sep = false;

	appendStringInfoChar(res, '{');
	for (int i = 0; i < reply->elements; i++)
	{
		valkeyReply *ir = reply->element[i];

		if (need_sep)
			appendStringInfoChar(res, ',');
		need_sep = true;
		if (ir->type == VALKEY_REPLY_ARRAY)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),	/* ??? */
					 errmsg("nested array returns not yet supported")));
		switch (ir->type)
		{
			case VALKEY_REPLY_STATUS:
			case VALKEY_REPLY_STRING:
				{
					char	   *buff;
					char	   *crs;

					pg_verifymbstr(ir->str, ir->len, false);
					buff = palloc(ir->len * 2 + 3);
					crs = buff;
					*crs++ = '"';
					for (int j = 0; j < ir->len; j++)
					{
						if (ir->str[j] == '"' || ir->str[j] == '\\')
							*crs++ = '\\';
						*crs++ = ir->str[j];
					}
					*crs++ = '"';
					*crs = '\0';
					appendStringInfoString(res, buff);
					pfree(buff);
				}
				break;
			case VALKEY_REPLY_INTEGER:
				appendStringInfo(res, "%lld", ir->integer);
				break;
			case VALKEY_REPLY_NIL:
				appendStringInfoString(res, "NULL");
				break;
			default:
				break;
		}
	}
	appendStringInfoChar(res, '}');

	return res->data;
}



static void
valkeyAddForeignUpdateTargets(PlannerInfo *root,
							 Index rtindex,
							 RangeTblEntry *target_rte,
							 Relation target_relation)
{
	Var		   *var;

	/* assumes that this isn't attisdropped */
	Form_pg_attribute attr =
		TupleDescAttr(RelationGetDescr(target_relation), 0);

#ifdef DEBUG
	elog(NOTICE, "valkeyAddForeignUpdateTargets");
#endif

	/*
	 * Code adapted from  postgres_fdw
	 *
	 * In Valkey, we need the key name. It's the first column in the table
	 * regardless of the table type. Knowing the key, we can update or delete
	 * it.
	 */

	/* Make a Var representing the desired value */
	var = makeVar(rtindex,
				  1,
				  attr->atttypid,
				  attr->atttypmod,
				  InvalidOid,
				  0);

	/* register it as a row-identity column needed by this target rel */
	add_row_identity_var(root, var, rtindex, VALKEYMODKEYNAME);

}

static List *
valkeyPlanForeignModify(PlannerInfo *root,
					   ModifyTable *plan,
					   Index resultRelation,
					   int subplan_index)
{
	CmdType		operation = plan->operation;
	RangeTblEntry *rte = planner_rt_fetch(resultRelation, root);
	Relation	rel;
	List	   *targetAttrs = NIL;
	List	   *array_elem_list = NIL;
	TupleDesc	tupdesc;
	Oid			array_element_type = InvalidOid;

#ifdef DEBUG
	elog(NOTICE, "valkeyPlanForeignModify");
#endif

	/*
	 * RETURNING list not supported
	 */
	if (plan->returningLists)
		elog(ERROR, "RETURNING is not supported by this FDW");

	rel = table_open(rte->relid, NoLock);
	tupdesc = RelationGetDescr(rel);

	/* if the second attribute exists and it's an array, get the element type */
	if (tupdesc->natts > 1)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, 1);

		array_element_type = get_element_type(attr->atttypid);
	}

	array_elem_list = lappend_oid(array_elem_list, array_element_type);


	if (operation == CMD_INSERT)
	{
		int			attnum;

		for (attnum = 1; attnum <= tupdesc->natts; attnum++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

			if (!attr->attisdropped)
				targetAttrs = lappend_int(targetAttrs, attnum);
		}

	}
	else if (operation == CMD_UPDATE)
	{

		/* code borrowed from mysql fdw */

		RelOptInfo *rrel = find_base_rel(root, resultRelation);
		Bitmapset  *tmpset = get_rel_all_updated_cols(root, rrel);
		int	colidx = -1;

		while ((colidx = bms_next_member(tmpset, colidx)) >= 0)
		{
			AttrNumber col = colidx + FirstLowInvalidHeapAttributeNumber;
			if (col <= InvalidAttrNumber)		/* shouldn't happen */
				elog(ERROR, "system-column update is not supported");

			targetAttrs = lappend_int(targetAttrs, col);
		}

	}

	/* nothing extra needed for DELETE - all it needs is the resjunk column */

	table_close(rel, NoLock);

	return list_make2(targetAttrs, array_elem_list);
}

static void
valkeyBeginForeignModify(ModifyTableState *mtstate,
						ResultRelInfo *rinfo,
						List *fdw_private,
						int subplan_index,
						int eflags)
{
	valkeyTableOptions    table_options;
	valkeyContext        *context;
	valkeyClusterContext *cc;
	valkeyReply          *reply;
	ValkeyFdwModifyState *fmstate;
	valkeyClusterNodeIterator iter;
	valkeyClusterNode *node;

	char *err;
	struct timeval timeout = {1, 500000};
	Relation	rel = rinfo->ri_RelationDesc;
	ListCell   *lc;
	Oid			typefnoid;
	bool		isvarlena;
	CmdType		op = mtstate->operation;
	int			n_attrs;
	List	   *array_elem_list;

#ifdef DEBUG
	elog(NOTICE, "valkeyBeginForeignModify");
#endif

	table_options.address = NULL;
	table_options.port = 0;
	table_options.password = NULL;
	table_options.database = 0;
	table_options.keyprefix = NULL;
	table_options.keyset = NULL;
	table_options.singleton_key = NULL;
	table_options.table_type = PG_VALKEY_SCALAR_TABLE;


	/* Fetch options  */
	valkeyGetOptions(RelationGetRelid(rel), &table_options);


	fmstate = (ValkeyFdwModifyState *) palloc(sizeof(ValkeyFdwModifyState));
	rinfo->ri_FdwState = fmstate;
	fmstate->rel = rel;
	fmstate->address = table_options.address;
	fmstate->port = table_options.port;
	fmstate->keyprefix = table_options.keyprefix;
	fmstate->keyset = table_options.keyset;
	fmstate->singleton_key = table_options.singleton_key;
	fmstate->table_type = table_options.table_type;
	fmstate->target_attrs = (List *) list_nth(fdw_private, 0);

	n_attrs = list_length(fmstate->target_attrs);
	fmstate->p_flinfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo) * (n_attrs + 1));
	fmstate->targetDims = (int *) palloc0(sizeof(int) * (n_attrs + 1));

	array_elem_list = (List *) list_nth(fdw_private, 1);
	fmstate->array_elem_type = list_nth_oid(array_elem_list, 0);

	fmstate->p_nums = 0;
	err = palloc(256);
	memset(err, 0, 256);
	if (op == CMD_UPDATE || op == CMD_DELETE)
	{
		Plan	   *subplan = outerPlanState(mtstate)->plan;
		Form_pg_attribute attr = TupleDescAttr(RelationGetDescr(rel), 0);		/* key is first */

		fmstate->keyAttno = ExecFindJunkAttributeInTlist(subplan->targetlist,
														 VALKEYMODKEYNAME);

		getTypeOutputInfo(attr->atttypid, &typefnoid, &isvarlena);
		fmgr_info(typefnoid, &fmstate->p_flinfo[fmstate->p_nums]);
		fmstate->p_nums++;

	}

	if (op == CMD_UPDATE || op == CMD_INSERT)
	{

		fmstate->targetDims = (int *) palloc0(sizeof(int) * (n_attrs + 1));

		foreach(lc, fmstate->target_attrs)
		{
			int			attnum = lfirst_int(lc);
			Form_pg_attribute attr = TupleDescAttr(RelationGetDescr(rel), attnum - 1);
			Oid			elem = attr->attndims ?
			get_element_type(attr->atttypid) :
			attr->atttypid;

			/*
			 * most non-singleton table types require an array, not text as
			 * value
			 */
			if (op == CMD_UPDATE && attnum > 1 &&
				attr->attndims == 0 && !fmstate->singleton_key &&
				fmstate->table_type != PG_VALKEY_SCALAR_TABLE)
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				  errmsg("value update not supported for this type of table")
						 ));
			}

			/*
			 * If the item is an array, store the output details for its
			 * element type, otherwise for the actual type. This saves us
			 * doing lookups later on.
			 */
			fmstate->targetDims[fmstate->p_nums] = attr->attndims;
			getTypeOutputInfo(elem, &typefnoid, &isvarlena);
			fmgr_info(typefnoid, &fmstate->p_flinfo[fmstate->p_nums]);
			fmstate->p_nums++;
		}
	}

	/*
	 * Now do some sanity checking on the number of table attributes. Since we
	 * do these here we can assume everthing is OK when we do the per row
	 * functions.
	 */

	if (op == CMD_INSERT)
	{
		if (table_options.singleton_key)
		{
			if (table_options.table_type == PG_VALKEY_ZSET_TABLE && fmstate->p_nums < 2)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("operation not supported for singleton zset "
								"table without priorities column")
						 ));
			else if (fmstate->p_nums != ((table_options.table_type == PG_VALKEY_HASH_TABLE || table_options.table_type == PG_VALKEY_ZSET_TABLE) ? 2 : 1))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("table has incorrect number of columns: %d for type %d", fmstate->p_nums, table_options.table_type)
						 ));
		}
		else if (fmstate->p_nums != 2)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("table has incorrect number of columns")
					 ));
		}
	}
	else if (op == CMD_UPDATE)
	{
		if (table_options.singleton_key && fmstate->table_type == PG_VALKEY_LIST_TABLE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("update not supported for this type of table")
					 ));
	}
	else	/* DELETE */
	{
		if (table_options.singleton_key && fmstate->table_type == PG_VALKEY_LIST_TABLE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("delete not supported for this type of table")
					 ));
	}

	/*
	 * all the checks have been done but no actual work done or connections
	 * made. That makes this the right spot to return if we're doing explain
	 * only.
	 */

	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/* Finally, Connect to the server and set the Valkey execution context */
	if (table_options.protocol == VALKEY_CLUSTER || table_options.protocol == VALKEY_TLS_CLUSTER)
	{
		cc = valkeyClusterContextInit();
		//valkeyClusterContextSetTimeout(cc, timeout);
		valkeyClusterSetOptionAddNodes(cc, table_options.address);
		if (valkeyClusterConnect2(cc) == VALKEY_ERR)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
					 errmsg("failed to connect to Valkey cluster [%s]: %s", table_options.address, cc->errstr)
					 ));
			valkeyClusterFree(cc);
		}
		if (cc == NULL || cc->err)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
					 errmsg("failed to connect to Valkey cluster [%s]: %s", table_options.address, cc->errstr)
					 ));
			valkeyClusterFree(cc);
		}
		valkeyClusterInitNodeIterator(&iter, cc);
		node = valkeyClusterNodeNext(&iter);
		while (node != NULL) {
			reply = valkeyClusterCommandToNode(cc, node, "PING");
			if (reply == NULL || reply->type == VALKEY_REPLY_ERROR)
			{
				if (sprintf(err, "failed to ping cluster [%s/%s]", table_options.address, node->host) < 0)
				{
					check_cluster_reply(reply, cc, ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION,
									err, NULL);
				}
				else
				{
					check_cluster_reply(reply, cc, ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION,
									"failed to ping cluster", NULL);
				}
			}
			freeReplyObject(reply);
			node = valkeyClusterNodeNext(&iter);
		}
	}
	else
	{
		context = valkeyConnectWithTimeout(table_options.address,
									  table_options.port, timeout);

		if (context->err)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
					 errmsg("failed to connect to Valkey: %s", context->errstr)
					 ));
			valkeyFree(context);
		}
		reply = valkeyCommand(context, "PING");
		check_reply(reply, context, ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION,
					"failed to ping", NULL);
	}

	/* Authenticate */
	if (table_options.password)
	{
		if (table_options.protocol == VALKEY_CLUSTER || table_options.protocol == VALKEY_TLS_CLUSTER)
			reply = valkeyClusterCommand(cc, "AUTH %s", table_options.password);
		else
			reply = valkeyCommand(context, "AUTH %s", table_options.password);

		if (!reply)
		{
			if (table_options.protocol == VALKEY_CLUSTER || table_options.protocol == VALKEY_TLS_CLUSTER)
			{
				valkeyClusterFree(cc);
				ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
						errmsg("failed to authenticate to valkey: %s", cc->errstr)
					));
			}
			else
			{
				valkeyFree(context);
				ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
						errmsg("failed to authenticate to valkey: %s", context->errstr)
					));
			}
		}

		freeReplyObject(reply);
	}

	/* Select the appropriate database */
	if (table_options.protocol == VALKEY_CLUSTER || table_options.protocol == VALKEY_TLS_CLUSTER)
	{
		/* Cluster Mode does not support multiple databases, so we don't need to select
		 * reply = valkeyClusterCommand(cc, "SELECT %d", table_options.database);
		 * check_cluster_reply(reply, cc, ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION,
		 *					"failed to select database", NULL);
		 */
		fmstate->cc = cc;
	}
	else
	{
		reply = valkeyCommand(context, "SELECT %d", table_options.database);
		check_reply(reply, context, ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION,
					"failed to select database", NULL);
		fmstate->context = context;
		freeReplyObject(reply);
	}
}

static void
check_reply(valkeyReply *reply, valkeyContext *context, int error_code, char *message, char *arg)
{
	char	   *err;
	char	   *xmessage;
	int         msglen;

	if (!reply)
	{
		err = pstrdup(context->errstr);
		valkeyFree(context);
	}
	else if (reply->type == VALKEY_REPLY_ERROR)
	{
		err = pstrdup(reply->str);
		freeReplyObject(reply);
	}
	else
		return;

	msglen = strlen(message);
	xmessage = palloc(msglen + 6);
	strncpy(xmessage, message, msglen + 1);
	strcat(xmessage, ": %s");

	if (arg != NULL)
		ereport(ERROR,
				(errcode(error_code),
				 errmsg(xmessage, arg, err)
				 ));
	else
		ereport(ERROR,
				(errcode(error_code),
				 errmsg(xmessage, err)
				 ));
}

static void
check_cluster_reply(valkeyReply *reply, valkeyClusterContext *cc, int error_code, char *message, char *arg)
{
	char	   *err;
	char	   *xmessage;
	int         msglen;

	if (!reply)
	{
		err = pstrdup(cc->errstr);
		valkeyClusterFree(cc);
	}
	else if (reply->type == VALKEY_REPLY_ERROR)
	{
		err = pstrdup(reply->str);
		freeReplyObject(reply);
	}
	else
		return;

	msglen = strlen(message);
	xmessage = palloc(msglen + 6);
	strncpy(xmessage, message, msglen + 1);
	strcat(xmessage, ": %s");

	if (arg != NULL)
		ereport(ERROR,
				(errcode(error_code),
				 errmsg(xmessage, arg, err)
				 ));
	else
		ereport(ERROR,
				(errcode(error_code),
				 errmsg(xmessage, err)
				 ));
}

static TupleTableSlot *
valkeyExecForeignInsert(EState *estate,
					   ResultRelInfo *rinfo,
					   TupleTableSlot *slot,
					   TupleTableSlot *planSlot)
{
	ValkeyFdwModifyState *fmstate =
	(ValkeyFdwModifyState *) rinfo->ri_FdwState;
	valkeyContext *context = fmstate->context;
	valkeyClusterContext *cc = fmstate->cc;
	valkeyReply *sreply = NULL;
	bool		isnull;
	bool		iscluster = false;
	Datum		key;
	char	   *keyval;
	char	   *extraval = "";	/* hash value or zset priority */

	if (cc != NULL)
		iscluster = true;

#ifdef DEBUG
	elog(NOTICE, "valkeyExecForeignInsert");
#endif

	key = slot_getattr(slot, 1, &isnull);
	keyval = OutputFunctionCall(&fmstate->p_flinfo[0], key);

	if (fmstate->singleton_key)
	{

		char	   *rkeyval;

		if (fmstate->table_type == PG_VALKEY_SCALAR_TABLE)
			rkeyval = fmstate->singleton_key;
		else
			rkeyval = keyval;

		/*
		 * Check if key is there using EXISTS / HEXISTS / SISMEMBER / ZRANK.
		 * It is not an error for a list type singleton as they don't have to
		 * be unique.
		 */


		switch (fmstate->table_type)
		{
			case PG_VALKEY_SCALAR_TABLE:
				if (iscluster)
					sreply = valkeyClusterCommand(cc, "EXISTS %s",		/* 1 or 0 */
									fmstate->singleton_key);
				else
					sreply = valkeyCommand(context, "EXISTS %s",		/* 1 or 0 */
									fmstate->singleton_key);
				break;
			case PG_VALKEY_HASH_TABLE:
				if (iscluster)
					sreply = valkeyClusterCommand(cc, "HEXISTS %s %s",	/* 1 or 0 */
									fmstate->singleton_key, keyval);
				else
					sreply = valkeyCommand(context, "HEXISTS %s %s",	/* 1 or 0 */
									fmstate->singleton_key, keyval);
				break;
			case PG_VALKEY_SET_TABLE:
				if (iscluster)
					sreply = valkeyClusterCommand(cc, "SISMEMBER %s %s",	/* 1 or 0 */
									fmstate->singleton_key, keyval);
				else
					sreply = valkeyCommand(context, "SISMEMBER %s %s",	/* 1 or 0 */
									fmstate->singleton_key, keyval);
				break;
			case PG_VALKEY_ZSET_TABLE:
				if (iscluster)
					sreply = valkeyClusterCommand(cc, "ZRANK %s %s",	/* n or nil */
									fmstate->singleton_key, keyval);
				else
					sreply = valkeyCommand(context, "ZRANK %s %s",	/* n or nil */
									fmstate->singleton_key, keyval);
				break;
			case PG_VALKEY_LIST_TABLE:
			default:
				break;
		}

		if (fmstate->table_type != PG_VALKEY_LIST_TABLE)
		{

			bool		ok = true;

			if (iscluster)
				check_cluster_reply(sreply, cc, ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
						"failed checking key existence", NULL);
			else
				check_reply(sreply, context, ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
						"failed checking key existence", NULL);

			if (fmstate->table_type != PG_VALKEY_ZSET_TABLE)
				ok = sreply->type == VALKEY_REPLY_INTEGER &&
					sreply->integer == 0;
			else
				ok = sreply->type == VALKEY_REPLY_NIL;

			freeReplyObject(sreply);

			if (!ok)
			{
				ereport(ERROR,
						(errcode(ERRCODE_UNIQUE_VIOLATION),
						 errmsg("key already exists: %s", rkeyval)));
			}
		}

		/* if OK add the value using SET / HSET / SADD / ZADD / RPUSH */

		/* get the second value for appropriate table types */

		if (fmstate->table_type == PG_VALKEY_ZSET_TABLE ||
			fmstate->table_type == PG_VALKEY_HASH_TABLE)
		{
			Datum		extra;

			extra = slot_getattr(slot, 2, &isnull);
			extraval = OutputFunctionCall(&fmstate->p_flinfo[1], extra);
		}

		switch (fmstate->table_type)
		{
			case PG_VALKEY_SCALAR_TABLE:
				if (iscluster)
					sreply = valkeyClusterCommand(cc, "SET %s %s",
									  fmstate->singleton_key, keyval);
				else
					sreply = valkeyCommand(context, "SET %s %s",
									  fmstate->singleton_key, keyval);
				break;
			case PG_VALKEY_SET_TABLE:
				if (iscluster)
					sreply = valkeyClusterCommand(cc, "SADD %s %s",
									  fmstate->singleton_key, keyval);
				else
					sreply = valkeyCommand(context, "SADD %s %s",
									  fmstate->singleton_key, keyval);
				break;
			case PG_VALKEY_LIST_TABLE:
				if (iscluster)
					sreply = valkeyClusterCommand(cc, "RPUSH %s %s",
									  fmstate->singleton_key, keyval);
				else
					sreply = valkeyCommand(context, "RPUSH %s %s",
									  fmstate->singleton_key, keyval);
				break;
			case PG_VALKEY_HASH_TABLE:
				if (iscluster)
					sreply = valkeyClusterCommand(cc, "HSET %s %s %s",
									  fmstate->singleton_key, keyval, extraval);
				else
					sreply = valkeyCommand(context, "HSET %s %s %s",
								   fmstate->singleton_key, keyval, extraval);
				break;
			case PG_VALKEY_ZSET_TABLE:

				/*
				 * score comes BEFORE value in ZADD, which seems slightly
				 * perverse
				 */
				if (iscluster)
					sreply = valkeyClusterCommand(cc, "ZADD %s %s %s",
									  fmstate->singleton_key, extraval, keyval);
				else
					sreply = valkeyCommand(context, "ZADD %s %s %s",
								   fmstate->singleton_key, extraval, keyval);
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("insert not supported for this type of table")
						 ));
		}

		if (iscluster)
			check_cluster_reply(sreply, cc, ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
					"cannot insert value for key %s", keyval);
		else
			check_reply(sreply, context, ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
					"cannot insert value for key %s", keyval);
		freeReplyObject(sreply);
	}
	else
	{
		/*
		 * not a singleton key table
		 *
		 */

		Datum		value;
		char	   *valueval = NULL;
		int			nitems;
		Datum	   *elements;
		bool	   *nulls;
		int16		typlen;
		bool		typbyval;
		char		typalign;

		bool		is_array = fmstate->array_elem_type != InvalidOid;

		value = slot_getattr(slot, 2, &isnull);

		if (isnull)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot insert NULL into a Valkey table")
					 ));

		if (is_array && fmstate->table_type == PG_VALKEY_SCALAR_TABLE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot insert array into into a Valkey scalar table")
					 ));
		else if (!is_array && fmstate->table_type != PG_VALKEY_SCALAR_TABLE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot insert into this type of Valkey table - needs an array")
					 ));

		/* make sure the key has the right prefix, if any */
		if (fmstate->keyprefix &&
			strncmp(keyval, fmstate->keyprefix,
					strlen(fmstate->keyprefix)) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("key '%s' does not match table key prefix '%s'",
							keyval, fmstate->keyprefix)
					 ));

		/* Check if key is there using EXISTS  */
		if (iscluster) {
			sreply = valkeyClusterCommand(cc, "EXISTS %s",		/* 1 or 0 */
							  keyval);
			check_cluster_reply(sreply, cc, ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
					"failed checking key existence", NULL);
		}
		else
		{
			sreply = valkeyCommand(context, "EXISTS %s",		/* 1 or 0 */
							  keyval);
			check_reply(sreply, context, ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
					"failed checking key existence", NULL);
		}

		if (sreply->type != VALKEY_REPLY_INTEGER || sreply->integer != 0)
		{
			freeReplyObject(sreply);
			ereport(ERROR,
					(errcode(ERRCODE_UNIQUE_VIOLATION),
					 errmsg("key already exists: %s", keyval)));
		}

		freeReplyObject(sreply);

		/* if OK add values using SET / HSET / SADD / ZADD / RPUSH */

		if (fmstate->table_type == PG_VALKEY_SCALAR_TABLE)
		{
			/* everything else will be an array */
			valueval = OutputFunctionCall(&fmstate->p_flinfo[1], value);
		}
		else
		{
			int			i;

			get_typlenbyvalalign(fmstate->array_elem_type,
								 &typlen, &typbyval, &typalign);

			deconstruct_array(DatumGetArrayTypeP(value),
							  fmstate->array_elem_type, typlen, typbyval,
							  typalign, &elements, &nulls,
							  &nitems);

			if (nitems == 0)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot store empty list in a Valkey table")
						 ));

			if (fmstate->table_type == PG_VALKEY_HASH_TABLE && nitems % 2 != 0)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot decompose odd number of items into a Valkey hash")
						 ));

			for (i = 0; i < nitems; i++)
			{
				if (nulls[i])
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot insert NULL into a Valkey table")
							 ));
			}
		}

		switch (fmstate->table_type)
		{
			case PG_VALKEY_SCALAR_TABLE:
				if (iscluster)
				{
					sreply = valkeyClusterCommand(cc, "SET %s %s",
									  keyval, valueval);
					check_cluster_reply(sreply, cc, ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
							"could not add key %s", keyval);
				}
				else
				{
					sreply = valkeyCommand(context, "SET %s %s",
										  keyval, valueval);
					check_reply(sreply, context,
								ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
								"could not add key %s", keyval);
				}
				freeReplyObject(sreply);
				break;
			case PG_VALKEY_SET_TABLE:
				{
					int			i;

					for (i = 0; i < nitems; i++)
					{
						valueval = OutputFunctionCall(&fmstate->p_flinfo[1],
													  elements[i]);
						if (iscluster)
						{
							sreply = valkeyClusterCommand(cc, "SADD %s %s",
											  keyval, valueval);
							check_cluster_reply(sreply, cc,
									ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
									"could not add set member %s", valueval);
						}
						else
						{
							sreply = valkeyCommand(context, "SADD %s %s",
											  keyval, valueval);
							check_reply(sreply, context,
										ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
										"could not add set member %s", valueval);
						}
						freeReplyObject(sreply);
					}
				}
				break;
			case PG_VALKEY_LIST_TABLE:
				{
					int			i;

					for (i = 0; i < nitems; i++)
					{
						valueval = OutputFunctionCall(&fmstate->p_flinfo[1],
													  elements[i]);
						if (iscluster)
						{
							sreply = valkeyClusterCommand(cc, "RPUSH %s %s",
											  keyval, valueval);
							check_cluster_reply(sreply, cc,
									ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
									"could not add value %s", valueval);
						}
						else
						{
							sreply = valkeyCommand(context, "RPUSH %s %s",
												  keyval, valueval);
							check_reply(sreply, context,
										ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
										"could not add value %s", valueval);
						}
					}
				}
				break;
			case PG_VALKEY_HASH_TABLE:
				{
					int			i;
					char	   *hk,
							   *hv;

					for (i = 0; i < nitems; i += 2)
					{
						hk = OutputFunctionCall(&fmstate->p_flinfo[1],
												elements[i]);
						hv = OutputFunctionCall(&fmstate->p_flinfo[1],
												elements[i + 1]);
						if (iscluster)
						{
							sreply = valkeyClusterCommand(cc, "HSET %s %s %s",
											  keyval, hk, hv);
							check_cluster_reply(sreply, cc,
									ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
									"could not add key %s", hk);
						}
						else
						{
							sreply = valkeyCommand(context, "HSET %s %s %s",
											  keyval, hk, hv);
							check_reply(sreply, context,
									ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
									"could not add key %s", hk);
						}
						freeReplyObject(sreply);
					}
				}
				break;
			case PG_VALKEY_ZSET_TABLE:
				{
					int			i;
					char		ibuff[100];

					for (i = 0; i < nitems; i++)
					{
						valueval = OutputFunctionCall(&fmstate->p_flinfo[1],
													  elements[i]);
						sprintf(ibuff, "%d", i);

						/*
						 * score comes BEFORE value in ZADD, which seems
						 * slightly perverse
						 */
						if (iscluster)
						{
							sreply = valkeyClusterCommand(cc, "ZADD %s %s %s",
											  keyval, ibuff, valueval);
							check_cluster_reply(sreply, cc,
									ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
									"could not add key %s", valueval);
						}
						else
						{
							sreply = valkeyCommand(context, "ZADD %s %s %s",
												  keyval, ibuff, valueval);
							check_reply(sreply, context,
										ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
										"could not add key %s", valueval);
						}
						freeReplyObject(sreply);
					}
				}
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("insert not supported for this type of table")
						 ));
		}

		/* if it's a keyset organized table, add key to keyset using SADD */

		if (fmstate->keyset)
		{
			if (iscluster)
			{
				sreply = valkeyClusterCommand(cc, "SADD %s %s",
								  fmstate->keyset, keyval);
				check_cluster_reply(sreply, cc,
					ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
					"could not add keyset element %s", valueval);
			}
			else
			{
				sreply = valkeyCommand(context, "SADD %s %s",
									  fmstate->keyset, keyval);
				check_reply(sreply, context,
							ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
							"could not add keyset element %s", valueval);
			}
			freeReplyObject(sreply);
		}
	}
	return slot;
}

static TupleTableSlot *
valkeyExecForeignDelete(EState *estate,
					   ResultRelInfo *rinfo,
					   TupleTableSlot *slot,
					   TupleTableSlot *planSlot)
{
	ValkeyFdwModifyState *fmstate =
	(ValkeyFdwModifyState *) rinfo->ri_FdwState;
	valkeyContext *context = fmstate->context;
	valkeyClusterContext *cc = fmstate->cc;
	bool		iscluster = false;
	valkeyReply *reply = NULL;
	bool		isNull;
	Datum		datum;
	char	   *keyval;

	if (cc != NULL)
		iscluster = true;

#ifdef DEBUG
	elog(NOTICE, "valkeyExecForeignDelete");
#endif

	/* Get the key that was passed up as a resjunk column */
	datum = ExecGetJunkAttribute(planSlot,
								 fmstate->keyAttno,
								 &isNull);

	keyval = OutputFunctionCall(&fmstate->p_flinfo[0], datum);

	/* elog(NOTICE,"deleting keyval %s",keyval); */

	if (fmstate->singleton_key)
	{
		switch (fmstate->table_type)
		{
			case PG_VALKEY_SCALAR_TABLE:
				if (iscluster)
					reply = valkeyClusterCommand(cc, "DEL %s",
									 fmstate->singleton_key);
				else
					reply = valkeyCommand(context, "DEL %s",
									 fmstate->singleton_key);
				break;
			case PG_VALKEY_SET_TABLE:
				if (iscluster)
					reply = valkeyClusterCommand(cc, "SREM %s %s",
									 fmstate->singleton_key, keyval);
				else
					reply = valkeyCommand(context, "SREM %s %s",
									 fmstate->singleton_key, keyval);
				break;
			case PG_VALKEY_HASH_TABLE:
				if (iscluster)
					reply = valkeyClusterCommand(cc, "HDEL %s %s",
									 fmstate->singleton_key, keyval);
				else
					reply = valkeyCommand(context, "HDEL %s %s",
									 fmstate->singleton_key, keyval);
				break;
			case PG_VALKEY_ZSET_TABLE:
				if (iscluster)
					reply = valkeyClusterCommand(cc, "ZREM %s %s",
									 fmstate->singleton_key, keyval);
				else
					reply = valkeyCommand(context, "ZREM %s %s",
									 fmstate->singleton_key, keyval);
				break;
			default:
				/* Note: List table has already generated an error */
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("delete not supported for this type of table")
						 ));
		}
	}
	else	/* not a singleton */
	{
		/* use DEL regardless of table type */
		if (iscluster)
			reply = valkeyClusterCommand(cc, "DEL %s", keyval);
		else
			reply = valkeyCommand(context, "DEL %s", keyval);
	}

	check_reply(reply, context,
				ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
				"failed to delete key %s", keyval);
	freeReplyObject(reply);

	if (fmstate->keyset)
	{
		if (iscluster)
		{
			reply = valkeyClusterCommand(cc, "SREM %s %s",
							 fmstate->keyset, keyval);
			check_cluster_reply(reply, cc,
					ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
					"failed to delete keyset element %s", keyval);
		}
		else
		{
			reply = valkeyCommand(context, "SREM %s %s",
								 fmstate->keyset, keyval);

			check_reply(reply, context,
						ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
						"failed to delete keyset element %s", keyval);
		}
		freeReplyObject(reply);

	}

	return slot;
}

static TupleTableSlot *
valkeyExecForeignUpdate(EState *estate,
					   ResultRelInfo *rinfo,
					   TupleTableSlot *slot,
					   TupleTableSlot *planSlot)
{
	ValkeyFdwModifyState *fmstate =
	(ValkeyFdwModifyState *) rinfo->ri_FdwState;

	valkeyContext        *context  = fmstate->context;
	valkeyClusterContext *cc       = fmstate->cc;
	valkeyReply          *ereply   = NULL;
	bool                 iscluster = false;

	Datum		datum;
	char	   *keyval;
	char	   *newkey;
	char	   *newval = NULL;
	char	  **array_vals = NULL;
	bool		isNull;
	ListCell   *lc = NULL;
	int			flslot = 1;
	int			nitems = 0;

	if (cc != NULL)
		iscluster = true;

#ifdef DEBUG
	elog(NOTICE, "valkeyExecForeignUpdate");
#endif

	/* Get the key that was passed up as a resjunk column */
	datum = ExecGetJunkAttribute(planSlot,
								 fmstate->keyAttno,
								 &isNull);

	keyval = OutputFunctionCall(&fmstate->p_flinfo[0], datum);

	newkey = keyval;

	Assert(keyval != NULL);

	/* extract the updated values */
	foreach(lc, fmstate->target_attrs)
	{
		int			attnum = lfirst_int(lc);

		datum = slot_getattr(slot, attnum, &isNull);

		if (isNull)
			elog(ERROR, "NULL update not supported");

		if (attnum == 1)
		{
			newkey = OutputFunctionCall(&fmstate->p_flinfo[flslot], datum);
		}
		else if (fmstate->singleton_key ||
				 fmstate->table_type == PG_VALKEY_SCALAR_TABLE)
		{
			/*
			 * non-singleton scalar value, or singleton hash value, or
			 * singleton zset priority.
			 */
			newval = OutputFunctionCall(&fmstate->p_flinfo[flslot], datum);
		}
		else
		{
			/*
			 * must be a non-singleton non-scalar table. so it must be an
			 * array.
			 */
			int			i;
			Datum	   *elements;
			bool	   *nulls;
			int16		typlen;
			bool		typbyval;
			char		typalign;

			get_typlenbyvalalign(fmstate->array_elem_type,
								 &typlen, &typbyval, &typalign);

			deconstruct_array(DatumGetArrayTypeP(datum),
							  fmstate->array_elem_type, typlen, typbyval,
							  typalign, &elements, &nulls,
							  &nitems);

			if (nitems == 0)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot store empty list in a Valkey table")
						 ));

			if (fmstate->table_type == PG_VALKEY_HASH_TABLE && nitems % 2 != 0)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot decompose odd number of items into a Valkey hash")
						 ));

			array_vals = palloc(nitems * sizeof(char *));

			for (i = 0; i < nitems; i++)
			{
				if (nulls[i])
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot set NULL in a Valkey table")
							 ));

				array_vals[i] = OutputFunctionCall(&fmstate->p_flinfo[flslot],
												   elements[i]);
			}
		}

		flslot++;
	}

	/* now we have all the data we need */

	/* if newkey = keyval then we're not updating the key */

	if (strcmp(keyval, newkey) != 0)
	{
		bool		ok = true;

		ereply = NULL;

		/* make sure the new key doesn't exist */
		if (!fmstate->singleton_key)
		{
			if (iscluster)
			{
				ereply = valkeyClusterCommand(cc, "EXISTS %s", newkey);
				ok = ereply->type == VALKEY_REPLY_INTEGER && ereply->integer == 0;
				check_cluster_reply(ereply, cc,
						ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
						"failed checking key existence %s", newkey);
			}
			else
			{
				ereply = valkeyCommand(context, "EXISTS %s", newkey);
				ok = ereply->type == VALKEY_REPLY_INTEGER && ereply->integer == 0;
				check_reply(ereply, context,
						ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
						"failed checking key existence %s", newkey);
			}
		}
		else
		{
			switch (fmstate->table_type)
			{
				case PG_VALKEY_SET_TABLE:
					if (iscluster)
						ereply = valkeyClusterCommand(cc, "SISMEMBER %s %s",
										  fmstate->singleton_key, newkey);
					else
						ereply = valkeyCommand(context, "SISMEMBER %s %s",
										  fmstate->singleton_key, newkey);
					break;
				case PG_VALKEY_ZSET_TABLE:
					if (iscluster)
						ereply = valkeyClusterCommand(cc, "ZRANK %s %s",
										  fmstate->singleton_key, newkey);
					else
						ereply = valkeyCommand(context, "ZRANK %s %s",
										  fmstate->singleton_key, newkey);
					break;
				case PG_VALKEY_HASH_TABLE:
					if (iscluster)
						ereply = valkeyClusterCommand(cc, "HEXISTS %s %s",
										  fmstate->singleton_key, newkey);
					else
						ereply = valkeyCommand(context, "HEXISTS %s %s",
										  fmstate->singleton_key, newkey);
					break;
				default:
					break;
			}
			if (fmstate->table_type != PG_VALKEY_SCALAR_TABLE)
			{
				if (fmstate->table_type != PG_VALKEY_ZSET_TABLE)
					ok = ereply->type == VALKEY_REPLY_INTEGER &&
						ereply->integer == 0;
				else
					ok = ereply->type == VALKEY_REPLY_NIL;

				check_reply(ereply, context,
							ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
							"failed checking key existence %s", keyval);
			}
		}

		if (ereply != NULL)
			freeReplyObject(ereply);

		if (!ok)
			ereport(ERROR,
					(errcode(ERRCODE_UNIQUE_VIOLATION),
					 errmsg("key already exists: %s", newkey)));

		if (!fmstate->singleton_key)
		{
			if (fmstate->keyprefix && strncmp(fmstate->keyprefix, newkey,
											strlen(fmstate->keyprefix)) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_UNIQUE_VIOLATION),
					  errmsg("key prefix condition violation: %s", newkey)));

			if (iscluster)
			{
				ereply = valkeyClusterCommand(cc, "RENAME %s %s", keyval, newkey);
				check_cluster_reply(ereply, cc,
						ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
						"failure renaming key %s", keyval);
			} else {
				ereply = valkeyCommand(context, "RENAME %s %s", keyval, newkey);
				check_reply(ereply, context,
						ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
						"failure renaming key %s", keyval);
			}
			freeReplyObject(ereply);

			if (newval && fmstate->table_type == PG_VALKEY_SCALAR_TABLE)
			{
				if (iscluster)
				{
					ereply = valkeyClusterCommand(cc, "SET %s %s", newkey, newval);
					check_cluster_reply(ereply, cc,
							ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
							"upating key %s", newkey);
				}
				else
				{
					ereply = valkeyCommand(context, "SET %s %s", newkey, newval);

					check_reply(ereply, context,
								ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
								"upating key %s", newkey);
				}
				freeReplyObject(ereply);
			}

			if (fmstate->keyset)
			{
				if (iscluster)
				{
					ereply = valkeyClusterCommand(cc, "SREM %s %s", fmstate->keyset,
									  keyval);
					check_cluster_reply(ereply, cc,
							ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
							"deleting keyset element %s", keyval);
				}
				else
				{
					ereply = valkeyCommand(context, "SREM %s %s", fmstate->keyset,
									  keyval);
					check_reply(ereply, context,
								ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
								"deleting keyset element %s", keyval);
				}
				freeReplyObject(ereply);

				if (iscluster)
				{
					ereply = valkeyClusterCommand(cc, "SADD %s %s", fmstate->keyset,
									  newkey);
					check_cluster_reply(ereply, cc,
							ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
							"adding keyset element %s", newkey);
				}
				else
				{
					ereply = valkeyCommand(context, "SADD %s %s", fmstate->keyset,
										  newkey);

					check_reply(ereply, context,
								ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
								"adding keyset element %s", newkey);
				}
				freeReplyObject(ereply);
			}
		}
		else	/* is a singleton */
		{
			switch (fmstate->table_type)
			{
				case PG_VALKEY_SCALAR_TABLE:
					if (iscluster)
					{
						ereply = valkeyClusterCommand(cc, "SET %s %s",
										  fmstate->singleton_key, newkey);
						check_cluster_reply(ereply, cc,
								ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
								"setting value %s", newkey);
					}
					else
					{
						ereply = valkeyCommand(context, "SET %s %s",
											  fmstate->singleton_key, newkey);
						check_reply(ereply, context,
									ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
									"setting value %s", newkey);
					}
					freeReplyObject(ereply);
					break;
				case PG_VALKEY_SET_TABLE:
					if (iscluster)
					{
						ereply = valkeyClusterCommand(cc, "SREM %s %s",
										  fmstate->singleton_key, keyval);
						check_cluster_reply(ereply, cc,
								ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
								"removing value %s", keyval);
					}
					else
					{
						ereply = valkeyCommand(context, "SREM %s %s",
										  fmstate->singleton_key, keyval);
						check_reply(ereply, context,
									ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
									"removing value %s", keyval);
					}
					freeReplyObject(ereply);
					if (iscluster)
					{
						ereply = valkeyClusterCommand(cc, "SADD %s %s",
										  fmstate->singleton_key, newkey);
						check_cluster_reply(ereply, cc,
								ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
								"setting value %s", newkey);
					}
					else
					{
						ereply = valkeyCommand(context, "SADD %s %s",
										  fmstate->singleton_key, newkey);
						check_reply(ereply, context,
									ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
									"setting value %s", newkey);
					}
					freeReplyObject(ereply);
					break;
				case PG_VALKEY_ZSET_TABLE:
					{
						char	   *priority = newval;

						if (!priority)
						{
							if (iscluster)
							{
								ereply = valkeyClusterCommand(cc, "ZSCORE %s %s",
												  fmstate->singleton_key,
												  keyval);
								check_cluster_reply(ereply, cc,
										ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
										"getting score for key %s", keyval);
							}
							else
							{
								ereply = valkeyCommand(context, "ZSCORE %s %s",
												  fmstate->singleton_key,
												  keyval);
								check_reply(ereply, context,
										ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
										"getting score for key %s", keyval);
							}
							priority = pstrdup(ereply->str);
							freeReplyObject(ereply);
						}
						if (iscluster)
						{
							ereply = valkeyClusterCommand(cc, "ZREM %s %s",
												  fmstate->singleton_key, keyval);
							check_cluster_reply(ereply, cc,
									ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
									"removing set element %s", keyval);
						}
						else
						{
							ereply = valkeyCommand(context, "ZREM %s %s",
												  fmstate->singleton_key, keyval);
							check_reply(ereply, context,
										ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
										"removing set element %s", keyval);
						}
						freeReplyObject(ereply);

						if (iscluster)
						{
							ereply = valkeyClusterCommand(cc, "ZADD %s %s %s",
												  fmstate->singleton_key,
												  priority, newkey);
							check_cluster_reply(ereply, cc,
									ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
									"setting element %s", newkey);
						}
						else
						{
							ereply = valkeyCommand(context, "ZADD %s %s %s",
												  fmstate->singleton_key,
												  priority, newkey);
							check_reply(ereply, context,
										ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
										"setting element %s", newkey);
						}
						freeReplyObject(ereply);
					}
					break;
				case PG_VALKEY_HASH_TABLE:
					{
						char	   *nval = newval;

						if (!nval)
						{
							if (iscluster)
							{
								ereply = valkeyClusterCommand(cc, "HGET %s %s",
												  fmstate->singleton_key,
												  keyval);
								check_cluster_reply(ereply, cc,
										ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
										"fetching value for key %s", keyval);
							}
							else
							{
								ereply = valkeyCommand(context, "HGET %s %s",
													  fmstate->singleton_key,
													  keyval);
								check_reply(ereply, context,
										  ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
											"fetching vcalue for key %s", keyval);
							}
							nval = pstrdup(ereply->str);
							freeReplyObject(ereply);
						}
						if (iscluster)
						{
							ereply = valkeyClusterCommand(cc, "HDEL %s %s",
												  fmstate->singleton_key, keyval);
							check_cluster_reply(ereply, cc,
									ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
									"removing hash element %s", keyval);
						}
						else
						{
							ereply = valkeyCommand(context, "HDEL %s %s",
												  fmstate->singleton_key, keyval);
							check_reply(ereply, context,
										ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
										"removing hash element %s", keyval);
						}
						freeReplyObject(ereply);

						if (iscluster)
						{
							ereply = valkeyClusterCommand(cc, "HSET %s %s %s",
												  fmstate->singleton_key, newkey,
												  nval);
							check_cluster_reply(ereply, cc,
									ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
									"adding hash element %s", newkey);
						}
						else
						{
							ereply = valkeyCommand(context, "HSET %s %s %s",
												  fmstate->singleton_key, newkey,
												  nval);
							check_reply(ereply, context,
										ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
										"adding hash element %s", newkey);
						}
						freeReplyObject(ereply);
					}
					break;
				default:
					break;
			}
		}
	}							/* no key update */
	else if (newval)
	{
		if (!fmstate->singleton_key)
		{
			Assert(fmstate->table_type == PG_VALKEY_SCALAR_TABLE);
			if (iscluster)
				ereply = valkeyClusterCommand(cc, "SET %s %s", keyval, newval);
			else
				ereply = valkeyCommand(context, "SET %s %s", keyval, newval);
		}
		else
		{
			if (fmstate->table_type == PG_VALKEY_ZSET_TABLE)
				if (iscluster)
					ereply = valkeyClusterCommand(cc, "ZADD %s %s %s",
									  fmstate->singleton_key, "0", newval);
				else
					ereply = valkeyCommand(context, "ZADD %s %s %s",
									  fmstate->singleton_key, newval, keyval);
			else if (fmstate->table_type == PG_VALKEY_HASH_TABLE)
				if (iscluster)
					ereply = valkeyClusterCommand(cc, "HSET %s %s %s",
									  fmstate->singleton_key, keyval, newval);
				else
					ereply = valkeyCommand(context, "HSET %s %s %s",
									  fmstate->singleton_key, keyval, newval);
			else
				elog(ERROR, "impossible update");		/* should not happen */
		}

		if (iscluster)
			check_cluster_reply(ereply, cc,
					ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
					"setting key %s", keyval);
		else
			check_reply(ereply, context,
					ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
					"setting key %s", keyval);
		freeReplyObject(ereply);
	}

	if (array_vals)
	{

		Assert(!fmstate->singleton_key);

		if (iscluster)
		{
			ereply = valkeyClusterCommand(cc, "DEL %s ", newkey);
			check_cluster_reply(ereply, cc,
					ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
					"could not delete key %s", newkey);
		}
		else
		{
			ereply = valkeyCommand(context, "DEL %s ", newkey);
			check_reply(ereply, context,
					ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
					"could not delete key %s", newkey);
		}
		freeReplyObject(ereply);

		switch (fmstate->table_type)
		{
			case PG_VALKEY_SET_TABLE:
				{
					int			i;

					for (i = 0; i < nitems; i++)
					{
						if (iscluster)
						{
							ereply = valkeyClusterCommand(cc, "SADD %s %s",
											  newkey, array_vals[i]);
							check_cluster_reply(ereply, cc,
									ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
									"could not add element %s", array_vals[i]);
						}
						else
						{
							ereply = valkeyCommand(context, "SADD %s %s",
											  newkey, array_vals[i]);
							check_reply(ereply, context,
									ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
									"could not add element %s", array_vals[i]);
						}
						freeReplyObject(ereply);
					}
				}
				break;
			case PG_VALKEY_LIST_TABLE:
				{
					int			i;

					for (i = 0; i < nitems; i++)
					{
						ereply = valkeyCommand(context, "RPUSH %s %s",
											  newkey, array_vals[i]);
						check_reply(ereply, context,
									ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
									"could not add value %s", array_vals[i]);
						freeReplyObject(ereply);
					}
				}
				break;
			case PG_VALKEY_HASH_TABLE:
				{
					int			i;
					char	   *hk,
							   *hv;

					for (i = 0; i < nitems; i += 2)
					{
						hk = array_vals[i];
						hv = array_vals[i + 1];
						if (iscluster)
						{
							ereply = valkeyClusterCommand(cc, "HSET %s %s %s",
											  newkey, hk, hv);
							check_cluster_reply(ereply, cc,
									ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
									"could not add key %s", hk);
						}
						else
						{
							ereply = valkeyCommand(context, "HSET %s %s %s",
											  newkey, hk, hv);
							check_reply(ereply, context,
									ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
									"could not add key %s", hk);
						}
						freeReplyObject(ereply);
					}
				}
				break;
			case PG_VALKEY_ZSET_TABLE:
				{
					int			i;
					char		ibuff[100];
					char	   *zval;

					for (i = 0; i < nitems; i++)
					{
						zval = array_vals[i];
						sprintf(ibuff, "%d", i);

						/*
						 * score comes BEFORE value in ZADD, which seems
						 * slightly perverse
						 */
						if (iscluster)
						{
							ereply = valkeyClusterCommand(cc, "ZADD %s %s %s",
											  newkey, ibuff, zval);
							check_cluster_reply(ereply, cc,
									ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
									"could not add key %s", zval);
						}
						else
						{
							ereply = valkeyCommand(context, "ZADD %s %s %s",
											  newkey, ibuff, zval);
							check_reply(ereply, context,
									ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
									"could not add key %s", zval);
						}
						freeReplyObject(ereply);
					}
				}
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("update not supported for this type of table")
						 ));
		}

	}

	return slot;
}


static void
valkeyEndForeignModify(EState *estate,
					  ResultRelInfo *rinfo)
{
	ValkeyFdwModifyState *fmstate = (ValkeyFdwModifyState *) rinfo->ri_FdwState;

#ifdef DEBUG
	elog(NOTICE, "valkeyEndForeignModify");
#endif

	/* if fmstate is NULL, we are in EXPLAIN; nothing to do */
	if (fmstate)
	{

#ifdef DEBUG
		elog(NOTICE, "valkeyEndForeignModify clusterfree");
#endif
		if (fmstate->cc) {
			valkeyClusterFree(fmstate->cc);
		}
		else
		{
#ifdef DEBUG
			elog(NOTICE, "valkeyEndForeignModify contextfree");
#endif
			if (fmstate->context)
				valkeyFree(fmstate->context);
		}
	}

#ifdef DEBUG
	elog(NOTICE, "valkeyEndForeignModify done");
#endif
}
