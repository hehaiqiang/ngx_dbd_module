
/*
 * Copyright (C) Ngwsx
 */


#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_dbd.h>


#define DBNTWIN32
#define DBLIB_SKIP_PRAGMA_PACK


#include <sqlfront.h>
#include <sqldb.h>


#if 0
#include <sybfront.h>
#include <sybdb.h>
#include <syberror.h>

#include <ict_def.h>
#include <ict_db.h>
#endif


typedef struct {
    DBPROCESS       *db;
    LOGINREC        *login;

    u_char          *server;
    u_char          *db_name;
    u_char          *sql;

    RETCODE          err;
    u_char           errmsg[256];

    uint64_t         num_rows;
    uint64_t         num_fields;
    uint64_t         cur_col;
} ngx_dbd_freetds_ctx_t;


static ngx_dbd_t *ngx_dbd_freetds_create(ngx_pool_t *pool, ngx_log_t *log);
static void ngx_dbd_freetds_destroy(ngx_dbd_t *dbd);
static int ngx_dbd_freetds_set_options(ngx_dbd_t *dbd, int opts);
static int ngx_dbd_freetds_get_options(ngx_dbd_t *dbd);
static ssize_t ngx_dbd_freetds_escape(ngx_dbd_t *dbd, u_char *dst,
    size_t dst_len, u_char *src, size_t src_len);
static u_char *ngx_dbd_freetds_error(ngx_dbd_t *dbd);
static ngx_err_t ngx_dbd_freetds_error_code(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_freetds_set_handler(ngx_dbd_t *dbd,
    ngx_dbd_handler_pt handler, void *data);
static ngx_int_t ngx_dbd_freetds_set_tcp(ngx_dbd_t *dbd, u_char *host,
    in_port_t port);
static ngx_int_t ngx_dbd_freetds_set_uds(ngx_dbd_t *dbd, u_char *uds);
static ngx_int_t ngx_dbd_freetds_set_auth(ngx_dbd_t *dbd, u_char *user,
    u_char *passwd);
static ngx_int_t ngx_dbd_freetds_set_db(ngx_dbd_t *dbd, u_char *db);
static ngx_int_t ngx_dbd_freetds_connect(ngx_dbd_t *dbd);
static void ngx_dbd_freetds_close(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_freetds_set_sql(ngx_dbd_t *dbd, u_char *sql,
    size_t len);
static ngx_int_t ngx_dbd_freetds_query(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_freetds_result_buffer(ngx_dbd_t *dbd);
static uint64_t ngx_dbd_freetds_result_warning_count(ngx_dbd_t *dbd);
static uint64_t ngx_dbd_freetds_result_insert_id(ngx_dbd_t *dbd);
static uint64_t ngx_dbd_freetds_result_affected_rows(ngx_dbd_t *dbd);
static uint64_t ngx_dbd_freetds_result_column_count(ngx_dbd_t *dbd);
static uint64_t ngx_dbd_freetds_result_row_count(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_freetds_column_skip(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_freetds_column_buffer(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_freetds_column_read(ngx_dbd_t *dbd);
static u_char *ngx_dbd_freetds_column_catalog(ngx_dbd_t *dbd);
static u_char *ngx_dbd_freetds_column_db(ngx_dbd_t *dbd);
static u_char *ngx_dbd_freetds_column_table(ngx_dbd_t *dbd);
static u_char *ngx_dbd_freetds_column_orig_table(ngx_dbd_t *dbd);
static u_char *ngx_dbd_freetds_column_name(ngx_dbd_t *dbd);
static u_char *ngx_dbd_freetds_column_orig_name(ngx_dbd_t *dbd);
static int ngx_dbd_freetds_column_charset(ngx_dbd_t *dbd);
static size_t ngx_dbd_freetds_column_size(ngx_dbd_t *dbd);
static size_t ngx_dbd_freetds_column_max_size(ngx_dbd_t *dbd);
static int ngx_dbd_freetds_column_type(ngx_dbd_t *dbd);
static int ngx_dbd_freetds_column_flags(ngx_dbd_t *dbd);
static int ngx_dbd_freetds_column_decimals(ngx_dbd_t *dbd);
static void *ngx_dbd_freetds_column_default_value(ngx_dbd_t *dbd, size_t *len);
static ngx_int_t ngx_dbd_freetds_row_buffer(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_freetds_row_read(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_freetds_field_buffer(ngx_dbd_t *dbd, u_char **value,
    size_t *size);
static ngx_int_t ngx_dbd_freetds_field_read(ngx_dbd_t *dbd, u_char **value,
    off_t *offset, size_t *size, size_t *total);

static int ngx_dbd_freetds_err_handler(DBPROCESS *db, int severity, int dberr,
    int oserr, char *dberrstr, char *oserrstr);
static int ngx_dbd_freetds_msg_handler(DBPROCESS *db, DBINT msgno, int msgstate,
    int severity, char *msgtext, char *srvname, char *procname,
    DBUSMALLINT line);


static ngx_str_t  ngx_dbd_freetds_name = ngx_string("freetds");


ngx_dbd_driver_t  ngx_dbd_freetds_driver = {
    &ngx_dbd_freetds_name,
    ngx_dbd_freetds_create,
    ngx_dbd_freetds_destroy,
    ngx_dbd_freetds_set_options,
    ngx_dbd_freetds_get_options,
    ngx_dbd_freetds_escape,
    ngx_dbd_freetds_error,
    ngx_dbd_freetds_error_code,
    ngx_dbd_freetds_set_handler,
    ngx_dbd_freetds_set_tcp,
    ngx_dbd_freetds_set_uds,
    ngx_dbd_freetds_set_auth,
    ngx_dbd_freetds_set_db,
    ngx_dbd_freetds_connect,
    ngx_dbd_freetds_close,
    ngx_dbd_freetds_set_sql,
    ngx_dbd_freetds_query,
    ngx_dbd_freetds_result_buffer,
    ngx_dbd_freetds_result_warning_count,
    ngx_dbd_freetds_result_insert_id,
    ngx_dbd_freetds_result_affected_rows,
    ngx_dbd_freetds_result_column_count,
    ngx_dbd_freetds_result_row_count,
    ngx_dbd_freetds_column_skip,
    ngx_dbd_freetds_column_buffer,
    ngx_dbd_freetds_column_read,
    ngx_dbd_freetds_column_catalog,
    ngx_dbd_freetds_column_db,
    ngx_dbd_freetds_column_table,
    ngx_dbd_freetds_column_orig_table,
    ngx_dbd_freetds_column_name,
    ngx_dbd_freetds_column_orig_name,
    ngx_dbd_freetds_column_charset,
    ngx_dbd_freetds_column_size,
    ngx_dbd_freetds_column_max_size,
    ngx_dbd_freetds_column_type,
    ngx_dbd_freetds_column_flags,
    ngx_dbd_freetds_column_decimals,
    ngx_dbd_freetds_column_default_value,
    ngx_dbd_freetds_row_buffer,
    ngx_dbd_freetds_row_read,
    ngx_dbd_freetds_field_buffer,
    ngx_dbd_freetds_field_read
};


static ngx_dbd_t *
ngx_dbd_freetds_create(ngx_pool_t *pool, ngx_log_t *log)
{
    ngx_dbd_t              *dbd;
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, log, 0, "dbd freetds create");

    dbd = ngx_pcalloc(pool, sizeof(ngx_dbd_t));
    if (dbd == NULL) {
        return NULL;
    }

    ctx = ngx_pcalloc(pool, sizeof(ngx_dbd_freetds_ctx_t));
    if (ctx == NULL) {
        return NULL;
    }

    dbinit();

    /* dbsetversion */

    /* dbgetmaxprocs */

    /* dbsetmaxprocs(256); */

#if 0
    ngx_log_error(NGX_LOG_ALERT, log, 0, "version of db-lib:%s", dbversion());
#endif

    ctx->login = dblogin();
    if (ctx->login == NULL) {
        return NULL;
    }

    dbprocerrhandle(ctx->login, (DBERRHANDLE_PROC) ngx_dbd_freetds_err_handler);
    dbprocmsghandle(ctx->login, (DBMSGHANDLE_PROC) ngx_dbd_freetds_msg_handler);

    dbd->pool = pool;
    dbd->log = log;
    dbd->ctx = ctx;

    return dbd;
}


static void
ngx_dbd_freetds_destroy(ngx_dbd_t *dbd)
{
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd freetds destroy");

    ctx = dbd->ctx;

    dbfreelogin(ctx->login);

    /* TODO: dbwinexit(); */

    dbexit();
}


static int
ngx_dbd_freetds_set_options(ngx_dbd_t *dbd, int opts)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd freetds set options");

    dbd->opts = opts;

    return dbd->opts;
}


static int
ngx_dbd_freetds_get_options(ngx_dbd_t *dbd)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd freetds get options");

    return dbd->opts;
}


static ssize_t
ngx_dbd_freetds_escape(ngx_dbd_t *dbd, u_char *dst, size_t dst_len, u_char *src,
    size_t src_len)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd freetds escape");

    /* TODO: dbsafestr */

    ngx_copy(dst, src, src_len);

    return src_len;
}


static u_char *
ngx_dbd_freetds_error(ngx_dbd_t *dbd)
{
    u_char                 *errstr;
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd freetds error");

    ctx = dbd->ctx;

    switch (ctx->err) {
    /* TODO */
    default:
        errstr = (u_char *) "Unknown error";
    }

    ngx_sprintf(ctx->errmsg, "%d:%s", ctx->err, errstr);

    return ctx->errmsg;
}


static ngx_err_t
ngx_dbd_freetds_error_code(ngx_dbd_t *dbd)
{
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd freetds error code");

    ctx = dbd->ctx;

    return ctx->err;
}


static ngx_int_t
ngx_dbd_freetds_set_handler(ngx_dbd_t *dbd, ngx_dbd_handler_pt handler,
    void *data)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd freetds set handler");

    dbd->handler = handler;
    dbd->data = data;

    return NGX_OK;
}


static ngx_int_t
ngx_dbd_freetds_set_tcp(ngx_dbd_t *dbd, u_char *host, in_port_t port)
{
    ngx_str_t               str;
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd freetds set tcp");

    ctx = dbd->ctx;

    str.len = ngx_strlen(host) + 1;
    str.data = host;

    ctx->server = ngx_pstrdup(dbd->pool, &str);

    /* TODO: port */

    return NGX_OK;
}


static ngx_int_t
ngx_dbd_freetds_set_uds(ngx_dbd_t *dbd, u_char *uds)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd freetds set uds");

    return NGX_DECLINED;
}


static ngx_int_t
ngx_dbd_freetds_set_auth(ngx_dbd_t *dbd, u_char *user, u_char *passwd)
{
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd freetds set auth");

    ctx = dbd->ctx;

    DBSETLUSER(ctx->login, (LPCSTR) user);
    DBSETLPWD(ctx->login, (LPCSTR) passwd);

    return NGX_OK;
}


static ngx_int_t
ngx_dbd_freetds_set_db(ngx_dbd_t *dbd, u_char *db)
{
    ngx_str_t               str;
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd freetds set db");

    ctx = dbd->ctx;

    str.len = ngx_strlen(db) + 1;
    str.data = db;

    ctx->db_name = ngx_pstrdup(dbd->pool, &str);

    return NGX_OK;
}


static ngx_int_t
ngx_dbd_freetds_connect(ngx_dbd_t *dbd)
{
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd freetds connect");

    ctx = dbd->ctx;

#if 0
    DBSETLAPP;

    DBSETLCHARSET;

    DBSETLNATLANG;

    dbgetpacket;

    DBGETTIME;

    DBSETLVERSION;

    DBSETLTIME;

    DBSETLSECURE;

    DBSETLPACKET;
#endif

    /* DBSETLOGINTIME; */

    /* dbsetlogintime(60); */

    ctx->db = dbopen(ctx->login, (LPCSTR) ctx->server);
    if (ctx->db == NULL) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0, "dbopen() failed");
        return NGX_ERROR;
    }

    dbsetuserdata(ctx->db, dbd);

    if (ctx->db_name[0] != '\0') {
        if (dbuse(ctx->db, (LPCSTR) ctx->db_name) != SUCCEED) {
            ngx_log_error(NGX_LOG_ALERT, dbd->log, 0, "dbuse() failed");
            dbclose(ctx->db);
            return NGX_ERROR;
        }
    }

    /* dbspid(proc); */

    /* dbgettime(); */

    /* dbsettime(60); */

    /* dbservcharset */

    /* dbname */

    /* dbgetpacket */

    return NGX_OK;
}


static void
ngx_dbd_freetds_close(ngx_dbd_t *dbd)
{
    RETCODE                 rc;
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd freetds close");

    ctx = dbd->ctx;

    rc = dbcanquery(ctx->db);

    /* TODO: rc */

    dbclose(ctx->db);
}


static ngx_int_t
ngx_dbd_freetds_set_sql(ngx_dbd_t *dbd, u_char *sql, size_t len)
{
    ngx_str_t               str;
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd freetds set sql");

    ctx = dbd->ctx;

    str.len = len + 1;
    str.data = sql;

    ctx->sql = ngx_pstrdup(dbd->pool, &str);

    return NGX_OK;
}


static ngx_int_t
ngx_dbd_freetds_query(ngx_dbd_t *dbd)
{
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd freetds query");

    ctx = dbd->ctx;

#if 0
    if (dbrpcinit(ctx->db, ctx->sql, 0) != SUCCEED) {
        return NGX_ERROR;
    }

    /* TODO: dbreginit */

    /* dbrpcparam */

    if (dbrpcexec(ctx->db) != SUCCEED) {
        return NGX_ERROR;
    }

    if (dbrpcsend(ctx->db) != SUCCEED) {
        return NGX_ERROR;
    }

    /* dbregparam */
    /* dbregexec */

    ctx->err = dbfcmd(ctx->db, ctx->sql);
    if (ctx->err != SUCCEED) {
        return NGX_ERROR;
    }

    /* dbbind */
    /* dbhasretstat */
    /* dbretstatus */
    /* dbnumrets */
    /* dbretdata */
    /* dbrettype */
    /* dbretlen */
    /* dbretname */

    if (dbsqlok(ctx->db) != SUCCEED) {
        return NGX_ERROR;
    }
#endif

    ctx->err = dbcmd(ctx->db, (LPCSTR) ctx->sql);
    if (ctx->err != SUCCEED) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0, "dbcmd() failed");
        return NGX_ERROR;
    }

    ctx->err = dbsqlexec(ctx->db);
    if (ctx->err != SUCCEED) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0, "dbsqlexec() failed");
        return NGX_ERROR;
    }

    /* TODO: dbsetopt(DBBUFFER); */

    /* dbclropt(ctx->db, DBBUFFER, "100"); */

    /* dbisopt; */

#if 0
    ctx->err = dbsetopt(ctx->db, DBBUFFER, "100");
#endif

    ctx->err = dbresults(ctx->db);
    if (ctx->err != SUCCEED) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0, "dbresults() failed");
        return NGX_ERROR;
    }

    ctx->cur_col = 0;
    ctx->num_fields = (uint64_t) dbnumcols(ctx->db);

    /* dbnumcompute(ctx->db); */

    /* dbnumalts */

    return NGX_OK;
}


static ngx_int_t
ngx_dbd_freetds_result_buffer(ngx_dbd_t *dbd)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd freetds result buffer");

    return NGX_DECLINED;
}


static uint64_t
ngx_dbd_freetds_result_warning_count(ngx_dbd_t *dbd)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd freetds result warning count");

    return 0;
}


static uint64_t
ngx_dbd_freetds_result_insert_id(ngx_dbd_t *dbd)
{
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd freetds result insert id");

    ctx = dbd->ctx;

    return 0;
}


static uint64_t
ngx_dbd_freetds_result_affected_rows(ngx_dbd_t *dbd)
{
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd freetds result affected rows");

    ctx = dbd->ctx;

    return 0;
}


static uint64_t
ngx_dbd_freetds_result_column_count(ngx_dbd_t *dbd)
{
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd freetds result column count");

    ctx = dbd->ctx;

    return ctx->num_fields;
}


static uint64_t
ngx_dbd_freetds_result_row_count(ngx_dbd_t *dbd)
{
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd freetds result row count");

    ctx = dbd->ctx;

    return ctx->num_rows;
}


static ngx_int_t
ngx_dbd_freetds_column_skip(ngx_dbd_t *dbd)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd freetds column skip");

    return NGX_DECLINED;
}


static ngx_int_t
ngx_dbd_freetds_column_buffer(ngx_dbd_t *dbd)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd freetds column buffer");

    return NGX_DECLINED;
}


static ngx_int_t
ngx_dbd_freetds_column_read(ngx_dbd_t *dbd)
{
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd freetds column read");

    ctx = dbd->ctx;

    if (ctx->cur_col++ == ctx->num_fields) {
        return NGX_DONE;
    }

    return NGX_OK;
}


static u_char *
ngx_dbd_freetds_column_catalog(ngx_dbd_t *dbd)
{
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd freetds column catalog");

    ctx = dbd->ctx;

    return NULL;
}


static u_char *
ngx_dbd_freetds_column_db(ngx_dbd_t *dbd)
{
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd freetds column db");

    ctx = dbd->ctx;

    return NULL;
}


static u_char *
ngx_dbd_freetds_column_table(ngx_dbd_t *dbd)
{
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd freetds column table");

    ctx = dbd->ctx;

    return NULL;
}


static u_char *
ngx_dbd_freetds_column_orig_table(ngx_dbd_t *dbd)
{
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd freetds column orig table");

    ctx = dbd->ctx;

    return NULL;
}


static u_char *
ngx_dbd_freetds_column_name(ngx_dbd_t *dbd)
{
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd freetds column name");

    ctx = dbd->ctx;

    return (u_char *) dbcolname(ctx->db, (INT) ctx->cur_col);
}


static u_char *
ngx_dbd_freetds_column_orig_name(ngx_dbd_t *dbd)
{
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd freetds column orig name");

    ctx = dbd->ctx;

    return NULL;
}


static int
ngx_dbd_freetds_column_charset(ngx_dbd_t *dbd)
{
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd freetds column charset");

    ctx = dbd->ctx;

    return 0;
}


static size_t
ngx_dbd_freetds_column_size(ngx_dbd_t *dbd)
{
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd freetds column size");

    ctx = dbd->ctx;

    return 0;
}


static size_t
ngx_dbd_freetds_column_max_size(ngx_dbd_t *dbd)
{
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd freetds column max size");

    ctx = dbd->ctx;

    return 0;
}


static int
ngx_dbd_freetds_column_type(ngx_dbd_t *dbd)
{
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd freetds column type");

    ctx = dbd->ctx;

    return 0;
}


static int
ngx_dbd_freetds_column_flags(ngx_dbd_t *dbd)
{
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd freetds column flags");

    ctx = dbd->ctx;

    return 0;
}


static int
ngx_dbd_freetds_column_decimals(ngx_dbd_t *dbd)
{
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd freetds column decimals");

    ctx = dbd->ctx;

    return 0;
}


static void *
ngx_dbd_freetds_column_default_value(ngx_dbd_t *dbd, size_t *len)
{
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd freetds column default value");

    ctx = dbd->ctx;

    return NULL;
}


static ngx_int_t
ngx_dbd_freetds_row_buffer(ngx_dbd_t *dbd)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd freetds row buffer");

    return NGX_DECLINED;
}


static ngx_int_t
ngx_dbd_freetds_row_read(ngx_dbd_t *dbd)
{
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd freetds row read");

    ctx = dbd->ctx;

#if 0
    /* dbsetrow: set current row */

    /* dbclrbuf */

    r = dbfirstrow(dbd->proc);

    r = dblastrow(dbd->proc);

    r = dbcurrow(dbd->proc);

    dbd->err = dbgetrow(dbd->proc, row + 1);
#endif

    ctx->err = dbnextrow(ctx->db);

    switch (ctx->err) {
    case SUCCEED:
    case REG_ROW:
        ctx->cur_col = 0;
        return NGX_OK;
    case NO_MORE_ROWS:
        return NGX_DONE;
    case FAIL:
    case BUF_FULL:
    /* TODO: case computeid: */
    default:
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "dbnextrow() failed (err:%d)", ctx->err);
        return NGX_ERROR;
    }
}


static ngx_int_t
ngx_dbd_freetds_field_buffer(ngx_dbd_t *dbd, u_char **value, size_t *size)
{
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd freetds field buffer");

    ctx = dbd->ctx;

    if (ctx->cur_col++ == ctx->num_fields) {
        return NGX_DONE;
    }

    /* dbcollen(ctx->db, ctx->cur_col) */

    /* dbvarylen(ctx->db, ctx->cur_col) */

    /* dbcoltype(ctx->db, ctx->cur_col) */

    /* dbcolutype */

    /* dbcoltypeinfo */

    /* dbcolinfo */

    /* dbcolsource */

    /* dbtablecolinfo */

    /* dbwillconvert */
    /* dbconvert */

    *value = dbdata(ctx->db, (INT) ctx->cur_col);
    *size = dbdatlen(ctx->db, (INT) ctx->cur_col);

    return NGX_OK;
}


static ngx_int_t
ngx_dbd_freetds_field_read(ngx_dbd_t *dbd, u_char **value, off_t *offset,
    size_t *size, size_t *total)
{
    ngx_dbd_freetds_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd freetds field read");

    ctx = dbd->ctx;

    if (ctx->cur_col++ == ctx->num_fields) {
        return NGX_DONE;
    }

    /* dbcollen(ctx->db, ctx->cur_col) */

    /* dbvarylen(ctx->db, ctx->cur_col) */

    /* dbcoltype(ctx->db, ctx->cur_col) */

    /* dbcolutype */

    /* dbcoltypeinfo */

    /* dbcolinfo */

    /* dbcolsource */

    /* dbtablecolinfo */

    /* dbwillconvert */
    /* dbconvert */

    *value = dbdata(ctx->db, (INT) ctx->cur_col);
    *size = dbdatlen(ctx->db, (INT) ctx->cur_col);
    *offset = 0;
    *total = *size;

    return NGX_OK;
}


static int
ngx_dbd_freetds_err_handler(DBPROCESS *db, int severity, int dberr, int oserr,
    char *dberrstr, char *oserrstr)
{
    ngx_log_t  *log;
    ngx_dbd_t  *dbd;

    dbd = dbgetuserdata(db);
    if (dbd != NULL) {
        log = dbd->log;

    } else {
        log = ngx_cycle->log;
    }

    if (dberrstr == NULL) {
        dberrstr = "";
    }

    if (oserrstr == NULL) {
        oserrstr = "";
    }

    /* TODO */

    ngx_log_error(NGX_LOG_ERR, log, 0,
                  "severity: %d, dberr: %d, oserr: %d, "
                  "dberrstr: %s, oserrstr: %s",
                  severity, dberr, oserr, dberrstr, oserrstr);

    return 0;
}


static int
ngx_dbd_freetds_msg_handler(DBPROCESS *db, DBINT msgno, int msgstate,
    int severity, char *msgtext, char *srvname, char *procname,
    DBUSMALLINT line)
{
    ngx_log_t  *log;
    ngx_dbd_t  *dbd;

    dbd = dbgetuserdata(db);
    if (dbd != NULL) {
        log = dbd->log;

    } else {
        log = ngx_cycle->log;
    }

    if (msgtext == NULL) {
        msgtext = "";
    }

    if (srvname == NULL) {
        srvname = "";
    }

    if (procname == NULL) {
        procname = "";
    }

    /* TODO */

    ngx_log_debug7(NGX_LOG_DEBUG_CORE, log, 0,
                   "msgno: %d, msgstate: %d, severity: %d, "
                   "msgtext: %s, srvname: %s, procname: %s, line: %d",
                   msgno, msgstate, severity, msgtext, srvname, procname, line);

    return 0;
}
