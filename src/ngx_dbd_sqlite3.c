
/*
 * Copyright (C) Ngwsx
 */


#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_dbd.h>
#include <sqlite3.h>


extern int sqlite3_initialize(void);
extern int sqlite3_shutdown(void);


typedef struct {
    sqlite3         *db;
    sqlite3_stmt    *stmt;
    u_char          *db_file;
    u_char          *sql;
    size_t           sql_len;
    int              cur_col;
} ngx_dbd_sqlite3_ctx_t;


static ngx_dbd_t *ngx_dbd_sqlite3_create(ngx_pool_t *pool, ngx_log_t *log);
static void ngx_dbd_sqlite3_destroy(ngx_dbd_t *dbd);
static int ngx_dbd_sqlite3_set_options(ngx_dbd_t *dbd, int opts);
static int ngx_dbd_sqlite3_get_options(ngx_dbd_t *dbd);
static ssize_t ngx_dbd_sqlite3_escape(ngx_dbd_t *dbd, u_char *dst,
    size_t dst_len, u_char *src, size_t src_len);
static u_char *ngx_dbd_sqlite3_error(ngx_dbd_t *dbd);
static ngx_err_t ngx_dbd_sqlite3_error_code(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_sqlite3_set_handler(ngx_dbd_t *dbd,
    ngx_dbd_handler_pt handler, void *data);
static ngx_int_t ngx_dbd_sqlite3_set_tcp(ngx_dbd_t *dbd, u_char *host,
    in_port_t port);
static ngx_int_t ngx_dbd_sqlite3_set_uds(ngx_dbd_t *dbd, u_char *uds);
static ngx_int_t ngx_dbd_sqlite3_set_auth(ngx_dbd_t *dbd, u_char *user,
    u_char *passwd);
static ngx_int_t ngx_dbd_sqlite3_set_db(ngx_dbd_t *dbd, u_char *db);
static ngx_int_t ngx_dbd_sqlite3_connect(ngx_dbd_t *dbd);
static void ngx_dbd_sqlite3_close(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_sqlite3_set_sql(ngx_dbd_t *dbd, u_char *sql,
    size_t len);
static ngx_int_t ngx_dbd_sqlite3_query(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_sqlite3_result_buffer(ngx_dbd_t *dbd);
static uint64_t ngx_dbd_sqlite3_result_warning_count(ngx_dbd_t *dbd);
static uint64_t ngx_dbd_sqlite3_result_insert_id(ngx_dbd_t *dbd);
static uint64_t ngx_dbd_sqlite3_result_affected_rows(ngx_dbd_t *dbd);
static uint64_t ngx_dbd_sqlite3_result_column_count(ngx_dbd_t *dbd);
static uint64_t ngx_dbd_sqlite3_result_row_count(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_sqlite3_column_skip(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_sqlite3_column_buffer(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_sqlite3_column_read(ngx_dbd_t *dbd);
static u_char *ngx_dbd_sqlite3_column_catalog(ngx_dbd_t *dbd);
static u_char *ngx_dbd_sqlite3_column_db(ngx_dbd_t *dbd);
static u_char *ngx_dbd_sqlite3_column_table(ngx_dbd_t *dbd);
static u_char *ngx_dbd_sqlite3_column_orig_table(ngx_dbd_t *dbd);
static u_char *ngx_dbd_sqlite3_column_name(ngx_dbd_t *dbd);
static u_char *ngx_dbd_sqlite3_column_orig_name(ngx_dbd_t *dbd);
static int ngx_dbd_sqlite3_column_charset(ngx_dbd_t *dbd);
static size_t ngx_dbd_sqlite3_column_size(ngx_dbd_t *dbd);
static size_t ngx_dbd_sqlite3_column_max_size(ngx_dbd_t *dbd);
static int ngx_dbd_sqlite3_column_type(ngx_dbd_t *dbd);
static int ngx_dbd_sqlite3_column_flags(ngx_dbd_t *dbd);
static int ngx_dbd_sqlite3_column_decimals(ngx_dbd_t *dbd);
static void *ngx_dbd_sqlite3_column_default_value(ngx_dbd_t *dbd, size_t *len);
static ngx_int_t ngx_dbd_sqlite3_row_buffer(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_sqlite3_row_read(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_sqlite3_field_buffer(ngx_dbd_t *dbd, u_char **value,
    size_t *size);
static ngx_int_t ngx_dbd_sqlite3_field_read(ngx_dbd_t *dbd, u_char **value,
    off_t *offset, size_t *size, size_t *total);


static ngx_str_t  ngx_dbd_sqlite3_name = ngx_string("sqlite3");


ngx_dbd_driver_t  ngx_dbd_sqlite3_driver = {
    &ngx_dbd_sqlite3_name,
    ngx_dbd_sqlite3_create,
    ngx_dbd_sqlite3_destroy,
    ngx_dbd_sqlite3_set_options,
    ngx_dbd_sqlite3_get_options,
    ngx_dbd_sqlite3_escape,
    ngx_dbd_sqlite3_error,
    ngx_dbd_sqlite3_error_code,
    ngx_dbd_sqlite3_set_handler,
    ngx_dbd_sqlite3_set_tcp,
    ngx_dbd_sqlite3_set_uds,
    ngx_dbd_sqlite3_set_auth,
    ngx_dbd_sqlite3_set_db,
    ngx_dbd_sqlite3_connect,
    ngx_dbd_sqlite3_close,
    ngx_dbd_sqlite3_set_sql,
    ngx_dbd_sqlite3_query,
    ngx_dbd_sqlite3_result_buffer,
    ngx_dbd_sqlite3_result_warning_count,
    ngx_dbd_sqlite3_result_insert_id,
    ngx_dbd_sqlite3_result_affected_rows,
    ngx_dbd_sqlite3_result_column_count,
    ngx_dbd_sqlite3_result_row_count,
    ngx_dbd_sqlite3_column_skip,
    ngx_dbd_sqlite3_column_buffer,
    ngx_dbd_sqlite3_column_read,
    ngx_dbd_sqlite3_column_catalog,
    ngx_dbd_sqlite3_column_db,
    ngx_dbd_sqlite3_column_table,
    ngx_dbd_sqlite3_column_orig_table,
    ngx_dbd_sqlite3_column_name,
    ngx_dbd_sqlite3_column_orig_name,
    ngx_dbd_sqlite3_column_charset,
    ngx_dbd_sqlite3_column_size,
    ngx_dbd_sqlite3_column_max_size,
    ngx_dbd_sqlite3_column_type,
    ngx_dbd_sqlite3_column_flags,
    ngx_dbd_sqlite3_column_decimals,
    ngx_dbd_sqlite3_column_default_value,
    ngx_dbd_sqlite3_row_buffer,
    ngx_dbd_sqlite3_row_read,
    ngx_dbd_sqlite3_field_buffer,
    ngx_dbd_sqlite3_field_read
};


static ngx_dbd_t *
ngx_dbd_sqlite3_create(ngx_pool_t *pool, ngx_log_t *log)
{
    ngx_dbd_t              *dbd;
    ngx_dbd_sqlite3_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, log, 0, "dbd sqlite3 create");

    dbd = ngx_pcalloc(pool, sizeof(ngx_dbd_t));
    if (dbd == NULL) {
        return NULL;
    }

    ctx = ngx_pcalloc(pool, sizeof(ngx_dbd_sqlite3_ctx_t));
    if (ctx == NULL) {
        return NULL;
    }

    /* TODO */

#if (NGX_WIN32)
    {
    ngx_int_t  rc;

    rc = sqlite3_initialize();
    if (rc != SQLITE_OK) {
        ngx_log_error(NGX_LOG_ALERT, log, 0,
                      "sqlite3_initialize() failed (rc:%d)", rc);
        return NULL;
    }
    }

    ngx_log_debug3(NGX_LOG_DEBUG_MYSQL, log, 0,
                   "version:%s version_number:%d threadsafe:%d",
                   sqlite3_libversion(), sqlite3_libversion_number(),
                   sqlite3_threadsafe());
#endif

    dbd->pool = pool;
    dbd->log = log;
    dbd->ctx = ctx;

    return dbd;
}


static void
ngx_dbd_sqlite3_destroy(ngx_dbd_t *dbd)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd sqlite3 destroy");

    /* TODO */

#if (NGX_WIN32)
    {
    ngx_int_t  rc;

    rc = sqlite3_shutdown();
    if (rc != SQLITE_OK) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "sqlite3_shutdown() failed (rc:%d)", rc);
    }
    }
#endif
}


static int
ngx_dbd_sqlite3_set_options(ngx_dbd_t *dbd, int opts)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd sqlite3 set options");

    dbd->opts = opts;

    return dbd->opts;
}


static int
ngx_dbd_sqlite3_get_options(ngx_dbd_t *dbd)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd sqlite3 get options");

    return dbd->opts;
}


static ssize_t
ngx_dbd_sqlite3_escape(ngx_dbd_t *dbd, u_char *dst, size_t dst_len, u_char *src,
    size_t src_len)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd sqlite3 escape");

    /* TODO: sqlite3_snprintf */

    if (sqlite3_snprintf(dst_len, (char *) dst, "%s", (char *) src) == NULL) {
        return NGX_ERROR;
    }

    return ngx_strlen(dst);
}


static u_char *
ngx_dbd_sqlite3_error(ngx_dbd_t *dbd)
{
    ngx_dbd_sqlite3_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd sqlite3 error");

    ctx = dbd->ctx;

    return (u_char *) sqlite3_errmsg(ctx->db);
}


static ngx_err_t
ngx_dbd_sqlite3_error_code(ngx_dbd_t *dbd)
{
    ngx_dbd_sqlite3_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd sqlite3 error code");

    ctx = dbd->ctx;

    return sqlite3_errcode(ctx->db);
}


static ngx_int_t
ngx_dbd_sqlite3_set_handler(ngx_dbd_t *dbd, ngx_dbd_handler_pt handler,
    void *data)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd sqlite3 set handler");

    dbd->handler = handler;
    dbd->data = data;

    return NGX_OK;
}


static ngx_int_t
ngx_dbd_sqlite3_set_tcp(ngx_dbd_t *dbd, u_char *host, in_port_t port)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd sqlite3 set tcp");

    return NGX_DECLINED;
}


static ngx_int_t
ngx_dbd_sqlite3_set_uds(ngx_dbd_t *dbd, u_char *uds)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd sqlite3 set uds");

    return NGX_DECLINED;
}


static ngx_int_t
ngx_dbd_sqlite3_set_auth(ngx_dbd_t *dbd, u_char *user, u_char *passwd)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd sqlite3 set auth");

    return NGX_DECLINED;
}


static ngx_int_t
ngx_dbd_sqlite3_set_db(ngx_dbd_t *dbd, u_char *db)
{
    ngx_str_t               str;
    ngx_dbd_sqlite3_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd sqlite3 set db");

    ctx = dbd->ctx;

    str.len = ngx_strlen(db) + 1;
    str.data = db;

    ctx->db_file = ngx_pstrdup(dbd->pool, &str);

    return NGX_OK;
}


static ngx_int_t
ngx_dbd_sqlite3_connect(ngx_dbd_t *dbd)
{
    ngx_dbd_sqlite3_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd sqlite3 connect");

    ctx = dbd->ctx;

    if (sqlite3_open((const char *) ctx->db_file, &ctx->db) != SQLITE_OK) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "sqlite3_open() \"%s\" failed (%d: %s)",
                      ctx->db_file,
                      sqlite3_errcode(ctx->db), sqlite3_errmsg(ctx->db));

        sqlite3_close(ctx->db);
        ctx->db = NULL;

        return NGX_ERROR;
    }

    return NGX_OK;
}


static void
ngx_dbd_sqlite3_close(ngx_dbd_t *dbd)
{
    ngx_dbd_sqlite3_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd sqlite3 close");

    ctx = dbd->ctx;

    if (sqlite3_finalize(ctx->stmt) != SQLITE_OK) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "sqlite3_finalize() failed (%d: %s)",
                      sqlite3_errcode(ctx->db), sqlite3_errmsg(ctx->db));
    }

    if (sqlite3_close(ctx->db) != SQLITE_OK) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "sqlite3_close() failed (%d: %s)",
                      sqlite3_errcode(ctx->db), sqlite3_errmsg(ctx->db));
    }
}


static ngx_int_t
ngx_dbd_sqlite3_set_sql(ngx_dbd_t *dbd, u_char *sql, size_t len)
{
    ngx_str_t               str;
    ngx_dbd_sqlite3_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd sqlite3 set sql");

    ctx = dbd->ctx;

    str.len = len;
    str.data = sql;

    ctx->sql = ngx_pstrdup(dbd->pool, &str);
    ctx->sql_len = len;

    return NGX_OK;
}


static ngx_int_t
ngx_dbd_sqlite3_query(ngx_dbd_t *dbd)
{
    ngx_dbd_sqlite3_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd sqlite3 query");

    ctx = dbd->ctx;

    if (sqlite3_prepare_v2(ctx->db, (const char *) ctx->sql, ctx->sql_len,
                           &ctx->stmt, NULL)
        != SQLITE_OK)
    {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "sqlite3_prepare_v2() failed (%d: %s)",
                      sqlite3_errcode(ctx->db), sqlite3_errmsg(ctx->db));
        return NGX_ERROR;
    }

    ctx->cur_col = -1;

    return NGX_OK;
}


static ngx_int_t
ngx_dbd_sqlite3_result_buffer(ngx_dbd_t *dbd)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd sqlite3 result buffer");

    /* TODO: */

    return NGX_OK;
}


static uint64_t
ngx_dbd_sqlite3_result_warning_count(ngx_dbd_t *dbd)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd sqlite3 result warning count");

    return 0;
}


static uint64_t
ngx_dbd_sqlite3_result_insert_id(ngx_dbd_t *dbd)
{
    ngx_dbd_sqlite3_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd sqlite3 result insert id");

    ctx = dbd->ctx;

    return sqlite3_last_insert_rowid(ctx->db);
}


static uint64_t
ngx_dbd_sqlite3_result_affected_rows(ngx_dbd_t *dbd)
{
    ngx_dbd_sqlite3_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd sqlite3 result affected rows");

    ctx = dbd->ctx;

    /* sqlite3_total_changes() */

    return sqlite3_changes(ctx->db);
}


static uint64_t
ngx_dbd_sqlite3_result_column_count(ngx_dbd_t *dbd)
{
    ngx_dbd_sqlite3_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd sqlite3 result column count");

    ctx = dbd->ctx;

    /* sqlite3_data_count() */

    return sqlite3_column_count(ctx->stmt);
}


static uint64_t
ngx_dbd_sqlite3_result_row_count(ngx_dbd_t *dbd)
{
    ngx_dbd_sqlite3_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd sqlite3 result row count");

    ctx = dbd->ctx;

    return 0;
}


static ngx_int_t
ngx_dbd_sqlite3_column_skip(ngx_dbd_t *dbd)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd sqlite3 column skip");

    /* TODO: */

    return NGX_OK;
}


static ngx_int_t
ngx_dbd_sqlite3_column_buffer(ngx_dbd_t *dbd)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd sqlite3 column buffer");

    /* TODO: */

    return NGX_OK;
}


static ngx_int_t
ngx_dbd_sqlite3_column_read(ngx_dbd_t *dbd)
{
    ngx_dbd_sqlite3_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd sqlite3 column read");

    ctx = dbd->ctx;

    if (++ctx->cur_col == sqlite3_column_count(ctx->stmt)) {
        return NGX_DONE;
    }

    return NGX_OK;
}


static u_char *
ngx_dbd_sqlite3_column_catalog(ngx_dbd_t *dbd)
{
    ngx_dbd_sqlite3_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd sqlite3 column catalog");

    ctx = dbd->ctx;

    return NULL;
}


static u_char *
ngx_dbd_sqlite3_column_db(ngx_dbd_t *dbd)
{
#if 0
    ngx_dbd_sqlite3_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd sqlite3 column db");

    ctx = dbd->ctx;

    return (u_char *) sqlite3_column_database_name(ctx->stmt, ctx->cur_col);
#else
    return NULL;
#endif
}


static u_char *
ngx_dbd_sqlite3_column_table(ngx_dbd_t *dbd)
{
#if 0
    ngx_dbd_sqlite3_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd sqlite3 column table");

    ctx = dbd->ctx;

    return (u_char *) sqlite3_column_table_name(ctx->stmt, ctx->cur_col);
#else
    return NULL;
#endif
}


static u_char *
ngx_dbd_sqlite3_column_orig_table(ngx_dbd_t *dbd)
{
#if 0
    ngx_dbd_sqlite3_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd sqlite3 column orig table");

    ctx = dbd->ctx;

    return (u_char *) sqlite3_column_origin_name(ctx->stmt, ctx->cur_col);
#else
    return NULL;
#endif
}


static u_char *
ngx_dbd_sqlite3_column_name(ngx_dbd_t *dbd)
{
    ngx_dbd_sqlite3_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd sqlite3 column name");

    ctx = dbd->ctx;

    return (u_char *) sqlite3_column_name(ctx->stmt, ctx->cur_col);
}


static u_char *
ngx_dbd_sqlite3_column_orig_name(ngx_dbd_t *dbd)
{
    ngx_dbd_sqlite3_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd sqlite3 column orig name");

    ctx = dbd->ctx;

    return NULL;
}


static int
ngx_dbd_sqlite3_column_charset(ngx_dbd_t *dbd)
{
    ngx_dbd_sqlite3_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd sqlite3 column charset");

    ctx = dbd->ctx;

    return 0;
}


static size_t
ngx_dbd_sqlite3_column_size(ngx_dbd_t *dbd)
{
    ngx_dbd_sqlite3_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd sqlite3 column size");

    ctx = dbd->ctx;

    return 0;
}


static size_t
ngx_dbd_sqlite3_column_max_size(ngx_dbd_t *dbd)
{
    ngx_dbd_sqlite3_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd sqlite3 column max size");

    ctx = dbd->ctx;

    return 0;
}


static int
ngx_dbd_sqlite3_column_type(ngx_dbd_t *dbd)
{
    ngx_dbd_sqlite3_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd sqlite3 column type");

    ctx = dbd->ctx;

    /* sqlite3_column_decltype() */

    return sqlite3_column_type(ctx->stmt, ctx->cur_col);
}


static int
ngx_dbd_sqlite3_column_flags(ngx_dbd_t *dbd)
{
    ngx_dbd_sqlite3_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd sqlite3 column flags");

    ctx = dbd->ctx;

    return 0;
}


static int
ngx_dbd_sqlite3_column_decimals(ngx_dbd_t *dbd)
{
    ngx_dbd_sqlite3_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd sqlite3 column decimals");

    ctx = dbd->ctx;

    return 0;
}


static void *
ngx_dbd_sqlite3_column_default_value(ngx_dbd_t *dbd, size_t *len)
{
    ngx_dbd_sqlite3_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd sqlite3 column default value");

    ctx = dbd->ctx;

    return NULL;
}


static ngx_int_t
ngx_dbd_sqlite3_row_buffer(ngx_dbd_t *dbd)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd sqlite3 row buffer");

    /* TODO: */

    return NGX_OK;
}


static ngx_int_t
ngx_dbd_sqlite3_row_read(ngx_dbd_t *dbd)
{
    int                     rc;
    ngx_dbd_sqlite3_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd sqlite3 row read");

    ctx = dbd->ctx;

    rc = sqlite3_step(ctx->stmt);

    if (rc == SQLITE_ROW) {
        ctx->cur_col = -1;
        return NGX_OK;

    } else if (rc == SQLITE_DONE) {
        return NGX_DONE;

    } else if (rc == SQLITE_BUSY) {
        return NGX_AGAIN;
    }

    ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                  "sqlite3_step() rc:%d (%d: %s)",
                  rc, sqlite3_errcode(ctx->db), sqlite3_errmsg(ctx->db));

    return NGX_ERROR;
}


static ngx_int_t
ngx_dbd_sqlite3_field_buffer(ngx_dbd_t *dbd, u_char **value, size_t *size)
{
    ngx_dbd_sqlite3_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd sqlite3 field buffer");

    ctx = dbd->ctx;

    if (++ctx->cur_col == sqlite3_column_count(ctx->stmt)) {
        return NGX_DONE;
    }

    *value = (u_char *) sqlite3_column_text(ctx->stmt, ctx->cur_col);
    *size = sqlite3_column_bytes(ctx->stmt, ctx->cur_col);

    return NGX_OK;
}


static ngx_int_t
ngx_dbd_sqlite3_field_read(ngx_dbd_t *dbd, u_char **value, off_t *offset,
    size_t *size, size_t *total)
{
    ngx_dbd_sqlite3_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd sqlite3 field read");

    ctx = dbd->ctx;

    if (++ctx->cur_col == sqlite3_column_count(ctx->stmt)) {
        return NGX_DONE;
    }

    *value = (u_char *) sqlite3_column_text(ctx->stmt, ctx->cur_col);
    *size = sqlite3_column_bytes(ctx->stmt, ctx->cur_col);
    *offset = 0;
    *total = *size;

    return NGX_OK;
}
