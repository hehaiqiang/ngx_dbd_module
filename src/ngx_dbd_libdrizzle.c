
/*
 * Copyright (C) Ngwsx
 */


#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_event.h>
#include <ngx_event_connect.h>
#include <ngx_dbd.h>

#if (NGX_WIN32)
#include <libdrizzle/common.h>
#else
#include <libdrizzle/drizzle_client.h>
#endif


typedef struct {
    drizzle_st                dri;
    drizzle_con_st            con;
    drizzle_result_st         res;
    drizzle_column_st         col;
    ngx_peer_connection_t     pc;
    ngx_uint_t                connected;
    u_char                   *sql;
    size_t                    sql_len;
    ngx_err_t                 error_code;
    u_char                   *error;
} ngx_dbd_libdrizzle_ctx_t;


static void ngx_dbd_libdrizzle_read_event_handler(ngx_event_t *rev);
static void ngx_dbd_libdrizzle_write_event_handler(ngx_event_t *wev);

#if (NGX_WIN32)
static ngx_int_t ngx_dbd_libdrizzle_get_peer(ngx_peer_connection_t *pc,
    void *data);
#endif

static ngx_dbd_t *ngx_dbd_libdrizzle_create(ngx_pool_t *pool, ngx_log_t *log);
static void ngx_dbd_libdrizzle_destroy(ngx_dbd_t *dbd);
static int ngx_dbd_libdrizzle_set_options(ngx_dbd_t *dbd, int opts);
static int ngx_dbd_libdrizzle_get_options(ngx_dbd_t *dbd);
static ssize_t ngx_dbd_libdrizzle_escape(ngx_dbd_t *dbd, u_char *dst,
    size_t dst_len, u_char *src, size_t src_len);
static u_char *ngx_dbd_libdrizzle_error(ngx_dbd_t *dbd);
static ngx_err_t ngx_dbd_libdrizzle_error_code(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_libdrizzle_set_handler(ngx_dbd_t *dbd,
    ngx_dbd_handler_pt handler, void *data);
static ngx_int_t ngx_dbd_libdrizzle_set_tcp(ngx_dbd_t *dbd, u_char *host,
    in_port_t port);
static ngx_int_t ngx_dbd_libdrizzle_set_uds(ngx_dbd_t *dbd, u_char *uds);
static ngx_int_t ngx_dbd_libdrizzle_set_auth(ngx_dbd_t *dbd, u_char *user,
    u_char *passwd);
static ngx_int_t ngx_dbd_libdrizzle_set_db(ngx_dbd_t *dbd, u_char *db);
static ngx_int_t ngx_dbd_libdrizzle_connect(ngx_dbd_t *dbd);
static void ngx_dbd_libdrizzle_close(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_libdrizzle_set_sql(ngx_dbd_t *dbd, u_char *sql,
    size_t len);
static ngx_int_t ngx_dbd_libdrizzle_query(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_libdrizzle_result_buffer(ngx_dbd_t *dbd);
static uint64_t ngx_dbd_libdrizzle_result_warning_count(ngx_dbd_t *dbd);
static uint64_t ngx_dbd_libdrizzle_result_insert_id(ngx_dbd_t *dbd);
static uint64_t ngx_dbd_libdrizzle_result_affected_rows(ngx_dbd_t *dbd);
static uint64_t ngx_dbd_libdrizzle_result_column_count(ngx_dbd_t *dbd);
static uint64_t ngx_dbd_libdrizzle_result_row_count(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_libdrizzle_column_skip(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_libdrizzle_column_buffer(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_libdrizzle_column_read(ngx_dbd_t *dbd);
static u_char *ngx_dbd_libdrizzle_column_catalog(ngx_dbd_t *dbd);
static u_char *ngx_dbd_libdrizzle_column_db(ngx_dbd_t *dbd);
static u_char *ngx_dbd_libdrizzle_column_table(ngx_dbd_t *dbd);
static u_char *ngx_dbd_libdrizzle_column_orig_table(ngx_dbd_t *dbd);
static u_char *ngx_dbd_libdrizzle_column_name(ngx_dbd_t *dbd);
static u_char *ngx_dbd_libdrizzle_column_orig_name(ngx_dbd_t *dbd);
static int ngx_dbd_libdrizzle_column_charset(ngx_dbd_t *dbd);
static size_t ngx_dbd_libdrizzle_column_size(ngx_dbd_t *dbd);
static size_t ngx_dbd_libdrizzle_column_max_size(ngx_dbd_t *dbd);
static int ngx_dbd_libdrizzle_column_type(ngx_dbd_t *dbd);
static int ngx_dbd_libdrizzle_column_flags(ngx_dbd_t *dbd);
static int ngx_dbd_libdrizzle_column_decimals(ngx_dbd_t *dbd);
static void *ngx_dbd_libdrizzle_column_default_value(ngx_dbd_t *dbd,
    size_t *len);
static ngx_int_t ngx_dbd_libdrizzle_row_buffer(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_libdrizzle_row_read(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_libdrizzle_field_buffer(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_libdrizzle_field_read(ngx_dbd_t *dbd, u_char **value,
    off_t *offset, size_t *size, size_t *total);


static ngx_str_t  ngx_dbd_libdrizzle_name = ngx_string("libdrizzle");


ngx_dbd_driver_t  ngx_dbd_libdrizzle_driver = {
    &ngx_dbd_libdrizzle_name,
    ngx_dbd_libdrizzle_create,
    ngx_dbd_libdrizzle_destroy,
    ngx_dbd_libdrizzle_set_options,
    ngx_dbd_libdrizzle_get_options,
    ngx_dbd_libdrizzle_escape,
    ngx_dbd_libdrizzle_error,
    ngx_dbd_libdrizzle_error_code,
    ngx_dbd_libdrizzle_set_handler,
    ngx_dbd_libdrizzle_set_tcp,
    ngx_dbd_libdrizzle_set_uds,
    ngx_dbd_libdrizzle_set_auth,
    ngx_dbd_libdrizzle_set_db,
    ngx_dbd_libdrizzle_connect,
    ngx_dbd_libdrizzle_close,
    ngx_dbd_libdrizzle_set_sql,
    ngx_dbd_libdrizzle_query,
    ngx_dbd_libdrizzle_result_buffer,
    ngx_dbd_libdrizzle_result_warning_count,
    ngx_dbd_libdrizzle_result_insert_id,
    ngx_dbd_libdrizzle_result_affected_rows,
    ngx_dbd_libdrizzle_result_column_count,
    ngx_dbd_libdrizzle_result_row_count,
    ngx_dbd_libdrizzle_column_skip,
    ngx_dbd_libdrizzle_column_buffer,
    ngx_dbd_libdrizzle_column_read,
    ngx_dbd_libdrizzle_column_catalog,
    ngx_dbd_libdrizzle_column_db,
    ngx_dbd_libdrizzle_column_table,
    ngx_dbd_libdrizzle_column_orig_table,
    ngx_dbd_libdrizzle_column_name,
    ngx_dbd_libdrizzle_column_orig_name,
    ngx_dbd_libdrizzle_column_charset,
    ngx_dbd_libdrizzle_column_size,
    ngx_dbd_libdrizzle_column_max_size,
    ngx_dbd_libdrizzle_column_type,
    ngx_dbd_libdrizzle_column_flags,
    ngx_dbd_libdrizzle_column_decimals,
    ngx_dbd_libdrizzle_column_default_value,
    ngx_dbd_libdrizzle_row_buffer,
    ngx_dbd_libdrizzle_row_read,
    ngx_dbd_libdrizzle_field_buffer,
    ngx_dbd_libdrizzle_field_read
};


static ngx_dbd_t *
ngx_dbd_libdrizzle_create(ngx_pool_t *pool, ngx_log_t *log)
{
    ngx_dbd_t                 *dbd;
    ngx_peer_connection_t     *pc;
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, log, 0, "dbd libdrizzle create");

    dbd = ngx_pcalloc(pool, sizeof(ngx_dbd_t));
    if (dbd == NULL) {
        return NULL;
    }

    ctx = ngx_pcalloc(pool, sizeof(ngx_dbd_libdrizzle_ctx_t));
    if (ctx == NULL) {
        return NULL;
    }

    if (drizzle_create(&ctx->dri) == NULL) {
        ngx_log_error(NGX_LOG_ALERT, log, ngx_errno, "drizzle_create() failed");
        return NULL;
    }

#if 0
    drizzle_set_timeout(&ctx->dri, 0);
#endif

    if (drizzle_con_create(&ctx->dri, &ctx->con) == NULL) {
        ngx_log_error(NGX_LOG_ALERT, log, ngx_errno,
                      "drizzle_con_create() failed");
        drizzle_free(&ctx->dri);
        return NULL;
    }

    drizzle_con_set_context(&ctx->con, dbd);
    drizzle_con_add_options(&ctx->con, DRIZZLE_CON_MYSQL);

    pc = &ctx->pc;
    pc->log = log;
    pc->rcvbuf = DRIZZLE_DEFAULT_SOCKET_RECV_SIZE;
#if (NGX_WIN32)
    pc->get = ngx_dbd_libdrizzle_get_peer;
    pc->data = dbd;
#endif

    dbd->pool = pool;
    dbd->log = log;
    dbd->ctx = ctx;

    return dbd;
}


static void
ngx_dbd_libdrizzle_destroy(ngx_dbd_t *dbd)
{
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd libdrizzle destroy");

    ctx = dbd->ctx;

    drizzle_con_free(&ctx->con);
    drizzle_free(&ctx->dri);
}


static int
ngx_dbd_libdrizzle_set_options(ngx_dbd_t *dbd, int opts)
{
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle set options");

    ctx = dbd->ctx;

    if (opts & NGX_DBD_OPT_NON_BLOCKING) {
        drizzle_set_options(&ctx->dri, DRIZZLE_NON_BLOCKING);
    }

    dbd->opts = opts;

    return dbd->opts;
}


static int
ngx_dbd_libdrizzle_get_options(ngx_dbd_t *dbd)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle get options");

    return dbd->opts;
}


static ssize_t
ngx_dbd_libdrizzle_escape(ngx_dbd_t *dbd, u_char *dst, size_t dst_len,
    u_char *src, size_t src_len)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd libdrizzle escape");

    return drizzle_escape_string((char *) dst, (const char *) src, src_len);
}


static u_char *
ngx_dbd_libdrizzle_error(ngx_dbd_t *dbd)
{
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd libdrizzle error");

    ctx = dbd->ctx;

    return (u_char *) drizzle_error(&ctx->dri);
}


static ngx_err_t
ngx_dbd_libdrizzle_error_code(ngx_dbd_t *dbd)
{
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle error code");

    ctx = dbd->ctx;

    return drizzle_error_code(&ctx->dri);
}


static ngx_int_t
ngx_dbd_libdrizzle_set_handler(ngx_dbd_t *dbd, ngx_dbd_handler_pt handler,
    void *data)
{
    ngx_connection_t          *c;
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle set handler");

    dbd->handler = handler;
    dbd->data = data;

    if (ngx_event_flags & NGX_USE_IOCP_EVENT) {
        return NGX_OK;
    }

    ctx = dbd->ctx;
    c = ctx->pc.connection;

    if ((dbd->opts & NGX_DBD_OPT_NON_BLOCKING) == 0 || c == NULL) {
        return NGX_OK;
    }

    if (handler == NULL) {
        if (c->read->timer_set) {
            ngx_del_timer(c->read);
        }

        if (c->write->timer_set) {
            ngx_del_timer(c->write);
        }

        if (c->read->active) {
            ngx_del_event(c->read, NGX_READ_EVENT, NGX_DISABLE_EVENT);
        }

        if (c->write->active) {
            ngx_del_event(c->write, NGX_WRITE_EVENT, NGX_DISABLE_EVENT);
        }

        if (c->read->prev) {
            ngx_delete_posted_event(c->read);
        }

        if (c->write->prev) {
            ngx_delete_posted_event(c->write);
        }

    } else {
        if (!c->read->active) {
            ngx_add_event(c->read, NGX_READ_EVENT, NGX_LEVEL_EVENT);
        }

        if (!c->write->active) {
            ngx_add_event(c->write, NGX_WRITE_EVENT, NGX_LEVEL_EVENT);
        }
    }

    return NGX_OK;
}


static ngx_int_t
ngx_dbd_libdrizzle_set_tcp(ngx_dbd_t *dbd, u_char *host, in_port_t port)
{
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd libdrizzle set tcp");

    ctx = dbd->ctx;

    drizzle_con_set_tcp(&ctx->con, (const char *) host, port);

    return NGX_OK;
}


static ngx_int_t
ngx_dbd_libdrizzle_set_uds(ngx_dbd_t *dbd, u_char *uds)
{
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd libdrizzle set uds");

    ctx = dbd->ctx;

    drizzle_con_set_uds(&ctx->con, (const char *) uds);

    return NGX_OK;
}


static ngx_int_t
ngx_dbd_libdrizzle_set_auth(ngx_dbd_t *dbd, u_char *user, u_char *passwd)
{
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd libdrizzle set auth");

    ctx = dbd->ctx;

    drizzle_con_set_auth(&ctx->con, (const char *) user, (const char *) passwd);

    return NGX_OK;
}


static ngx_int_t
ngx_dbd_libdrizzle_set_db(ngx_dbd_t *dbd, u_char *db)
{
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd libdrizzle set db");

    ctx = dbd->ctx;

    drizzle_con_set_db(&ctx->con, (const char *) db);

    return NGX_OK;
}


static ngx_int_t
ngx_dbd_libdrizzle_connect(ngx_dbd_t *dbd)
{
    drizzle_return_t           rv;
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd libdrizzle connect");

    ctx = dbd->ctx;

    if (ctx->connected) {
        return NGX_OK;
    }

    rv = drizzle_con_connect(&ctx->con);

    if (rv != DRIZZLE_RETURN_OK && rv != DRIZZLE_RETURN_IO_WAIT) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "drizzle_con_connect() failed (%d: %s)",
                      drizzle_con_error_code(&ctx->con),
                      drizzle_con_error(&ctx->con));
        return NGX_ERROR;
    }

#if !(NGX_WIN32)

    {
    int                fd;
    ngx_int_t          event;
    ngx_event_t       *rev, *wev;
    ngx_connection_t  *c;

    c = ctx->pc.connection;

    if ((dbd->opts & NGX_DBD_OPT_NON_BLOCKING) && c == NULL) {
        fd = drizzle_con_fd(&ctx->con);
        if (fd == -1) {
            ngx_log_error(NGX_LOG_ALERT, dbd->log, ngx_errno,
                          "drizzle_con_fd() failed (%d: %s)",
                          drizzle_con_error_code(&ctx->con),
                          drizzle_con_error(&ctx->con));
            drizzle_con_close(&ctx->con);
            return NGX_ERROR;
        }

        c = ngx_get_connection(fd, dbd->log);
        if (c == NULL) {
            ngx_log_error(NGX_LOG_ALERT, dbd->log, ngx_errno,
                          "ngx_get_connection() failed");
            drizzle_con_close(&ctx->con);
            return NGX_ERROR;
        }

        rev = c->read;
        wev = c->write;

        rev->log = dbd->log;
        wev->log = dbd->log;

        rev->handler = ngx_dbd_libdrizzle_read_event_handler;
        wev->handler = ngx_dbd_libdrizzle_write_event_handler;

        /* TODO:
         * using ngx_add_conn() instead of ngx_add_event() for some event model.
         */

        if (ngx_event_flags & NGX_USE_CLEAR_EVENT) {
            event = NGX_CLEAR_EVENT;

        } else {
            event = NGX_LEVEL_EVENT;
        }

        if (ngx_add_event(rev, NGX_READ_EVENT, event) != NGX_OK) {
            ngx_free_connection(c);
            drizzle_con_close(&ctx->con);
            return NGX_ERROR;
        }

        if (ngx_add_event(wev, NGX_WRITE_EVENT, event) != NGX_OK) {
            ngx_free_connection(c);
            drizzle_con_close(&ctx->con);
            return NGX_ERROR;
        }

        c->number = ngx_atomic_fetch_add(ngx_connection_counter, 1);

        c->data = dbd;

        ctx->pc.connection = c;

    } else if (c != NULL) {
        rev = c->read;
        wev = c->write;

    } else {
        wev = NULL;
    }

    if (rv == DRIZZLE_RETURN_IO_WAIT) {
        /* TODO: set timeout */
        return NGX_AGAIN;
    }

    if (wev != NULL) {
        wev->ready = 1;
    }
    }

#else

    if (rv == DRIZZLE_RETURN_IO_WAIT) {
        /* TODO: set timeout */
        return NGX_AGAIN;
    }

#endif

    /* rv == DRIZZLE_RETURN_OK */

    ctx->connected = 1;

    return NGX_OK;
}


static void
ngx_dbd_libdrizzle_close(ngx_dbd_t *dbd)
{
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd libdrizzle close");

    ctx = dbd->ctx;

    if (ctx->pc.connection != NULL) {
        ngx_close_connection(ctx->pc.connection);
        ctx->pc.connection = NULL;
    }

    if (ctx->connected) {
#if 0
        drizzle_column_free(&ctx->col);
#else
        ctx->res.column_list = NULL;
#endif

        drizzle_result_free(&ctx->res);
        drizzle_con_close(&ctx->con);

        ctx->connected = 0;
    }
}


static ngx_int_t
ngx_dbd_libdrizzle_set_sql(ngx_dbd_t *dbd, u_char *sql, size_t len)
{
    ngx_str_t                  str;
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd libdrizzle set sql");

    ctx = dbd->ctx;

    /* TODO */

    drizzle_result_free(&ctx->res);

#if 0
    if (drizzle_result_create(&ctx->con, &ctx->res) == NULL) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, ngx_errno,
                      "drizzle_result_create() failed");
        return NGX_ERROR;
    }
#endif

    str.len = len;
    str.data = sql;

    ctx->sql = ngx_pstrdup(dbd->pool, &str);
    ctx->sql_len = len;

    return NGX_OK;
}


static ngx_int_t
ngx_dbd_libdrizzle_query(ngx_dbd_t *dbd)
{
    drizzle_return_t           rv;
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd libdrizzle query");

    ctx = dbd->ctx;
    rv = DRIZZLE_RETURN_OK;

    drizzle_query(&ctx->con, &ctx->res, (const char *) ctx->sql, ctx->sql_len,
                  &rv);

    if (rv == DRIZZLE_RETURN_IO_WAIT) {
        return NGX_AGAIN;
    }

    if (rv != DRIZZLE_RETURN_OK) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "drizzle_query() failed (%d: %s)",
                      drizzle_result_error_code(&ctx->res),
                      drizzle_result_error(&ctx->res));
        return NGX_ERROR;
    }

    /* rv == DRIZZLE_RETURN_OK */

#if 0
    if (drizzle_column_create(&ctx->res, &ctx->col) == NULL) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, ngx_errno,
                      "drizzle_column_create() failed");
        return NGX_ERROR;
    }
#endif

    return NGX_OK;
}


static ngx_int_t
ngx_dbd_libdrizzle_result_buffer(ngx_dbd_t *dbd)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle result buffer");

    /* TODO: */

    return NGX_DECLINED;
}


static uint64_t
ngx_dbd_libdrizzle_result_warning_count(ngx_dbd_t *dbd)
{
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle result warning count");

    ctx = dbd->ctx;

    return drizzle_result_warning_count(&ctx->res);
}


static uint64_t
ngx_dbd_libdrizzle_result_insert_id(ngx_dbd_t *dbd)
{
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle result insert id");

    ctx = dbd->ctx;

    return drizzle_result_insert_id(&ctx->res);
}


static uint64_t
ngx_dbd_libdrizzle_result_affected_rows(ngx_dbd_t *dbd)
{
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle result affected rows");

    ctx = dbd->ctx;

    return drizzle_result_affected_rows(&ctx->res);
}


static uint64_t
ngx_dbd_libdrizzle_result_column_count(ngx_dbd_t *dbd)
{
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle result column count");

    ctx = dbd->ctx;

    return drizzle_result_column_count(&ctx->res);
}


static uint64_t
ngx_dbd_libdrizzle_result_row_count(ngx_dbd_t *dbd)
{
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle result row count");

    ctx = dbd->ctx;

    return drizzle_result_row_count(&ctx->res);
}


static ngx_int_t
ngx_dbd_libdrizzle_column_skip(ngx_dbd_t *dbd)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle column skip");

    /* TODO: */

    return NGX_DECLINED;
}


static ngx_int_t
ngx_dbd_libdrizzle_column_buffer(ngx_dbd_t *dbd)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle column buffer");

    /* TODO: */

    return NGX_DECLINED;
}


static ngx_int_t
ngx_dbd_libdrizzle_column_read(ngx_dbd_t *dbd)
{
    drizzle_return_t           rv;
    drizzle_column_st         *column;
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle column read");

    ctx = dbd->ctx;
    rv = DRIZZLE_RETURN_OK;

    column = drizzle_column_read(&ctx->res, &ctx->col, &rv);

    if (rv == DRIZZLE_RETURN_IO_WAIT) {
        return NGX_AGAIN;
    }

    if (rv != DRIZZLE_RETURN_OK) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "drizzle_column_read() failed (%d: %s)",
                      drizzle_con_errno(&ctx->con),
                      drizzle_con_error(&ctx->con));
        return NGX_ERROR;
    }

    /* rv == DRIZZLE_RETURN_OK */

    if (column == NULL) {
        return NGX_DONE;
    }

    return NGX_OK;
}


static u_char *
ngx_dbd_libdrizzle_column_catalog(ngx_dbd_t *dbd)
{
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle column catalog");

    ctx = dbd->ctx;

    return (u_char *) drizzle_column_catalog(&ctx->col);
}


static u_char *
ngx_dbd_libdrizzle_column_db(ngx_dbd_t *dbd)
{
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle column db");

    ctx = dbd->ctx;

    return (u_char *) drizzle_column_db(&ctx->col);
}


static u_char *
ngx_dbd_libdrizzle_column_table(ngx_dbd_t *dbd)
{
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle column table");

    ctx = dbd->ctx;

    return (u_char *) drizzle_column_table(&ctx->col);
}


static u_char *
ngx_dbd_libdrizzle_column_orig_table(ngx_dbd_t *dbd)
{
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle column orig table");

    ctx = dbd->ctx;

    return (u_char *) drizzle_column_orig_table(&ctx->col);
}


static u_char *
ngx_dbd_libdrizzle_column_name(ngx_dbd_t *dbd)
{
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle column name");

    ctx = dbd->ctx;

    return (u_char *) drizzle_column_name(&ctx->col);
}


static u_char *
ngx_dbd_libdrizzle_column_orig_name(ngx_dbd_t *dbd)
{
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle column orig name");

    ctx = dbd->ctx;

    return (u_char *) drizzle_column_orig_name(&ctx->col);
}


static int
ngx_dbd_libdrizzle_column_charset(ngx_dbd_t *dbd)
{
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle column charset");

    ctx = dbd->ctx;

    return drizzle_column_charset(&ctx->col);
}


static size_t
ngx_dbd_libdrizzle_column_size(ngx_dbd_t *dbd)
{
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle column size");

    ctx = dbd->ctx;

    return drizzle_column_size(&ctx->col);
}


static size_t
ngx_dbd_libdrizzle_column_max_size(ngx_dbd_t *dbd)
{
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle column max size");

    ctx = dbd->ctx;

    return drizzle_column_max_size(&ctx->col);
}


static int
ngx_dbd_libdrizzle_column_type(ngx_dbd_t *dbd)
{
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle column type");

    ctx = dbd->ctx;

    return drizzle_column_type(&ctx->col);
}


static int
ngx_dbd_libdrizzle_column_flags(ngx_dbd_t *dbd)
{
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle column flags");

    ctx = dbd->ctx;

    return drizzle_column_flags(&ctx->col);
}


static int
ngx_dbd_libdrizzle_column_decimals(ngx_dbd_t *dbd)
{
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle column decimals");

    ctx = dbd->ctx;

    return drizzle_column_decimals(&ctx->col);
}


static void *
ngx_dbd_libdrizzle_column_default_value(ngx_dbd_t *dbd, size_t *len)
{
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle column default value");

    ctx = dbd->ctx;

    return (void *) drizzle_column_default_value(&ctx->col, len);
}


static ngx_int_t
ngx_dbd_libdrizzle_row_buffer(ngx_dbd_t *dbd)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle row buffer");

    /* TODO: */

    return NGX_DECLINED;
}


static ngx_int_t
ngx_dbd_libdrizzle_row_read(ngx_dbd_t *dbd)
{
    uint64_t                   row_num;
    drizzle_return_t           rv;
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd libdrizzle row read");

    ctx = dbd->ctx;
    rv = DRIZZLE_RETURN_OK;

    row_num = drizzle_row_read(&ctx->res, &rv);

    if (rv == DRIZZLE_RETURN_IO_WAIT) {
        return NGX_AGAIN;
    }

    if (rv != DRIZZLE_RETURN_OK) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "drizzle_row_read() failed (%d: %s)",
                      drizzle_con_errno(&ctx->con),
                      drizzle_con_error(&ctx->con));
        return NGX_ERROR;
    }

    /* rv == DRIZZLE_RETURN_OK */

    if (row_num == 0) {
        return NGX_DONE;
    }

    return NGX_OK;
}


static ngx_int_t
ngx_dbd_libdrizzle_field_buffer(ngx_dbd_t *dbd)
{
    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle field buffer");

    /* TODO: */

    return NGX_DECLINED;
}


static ngx_int_t
ngx_dbd_libdrizzle_field_read(ngx_dbd_t *dbd, u_char **value, off_t *offset,
    size_t *size, size_t *total)
{
    drizzle_field_t            field;
    drizzle_return_t           rv;
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle field read");

    ctx = dbd->ctx;
    rv = DRIZZLE_RETURN_OK;

    field = drizzle_field_read(&ctx->res, (size_t *) offset, size, total, &rv);

    if (rv == DRIZZLE_RETURN_IO_WAIT) {
        return NGX_AGAIN;
    }

    if (rv == DRIZZLE_RETURN_ROW_END) {
        return NGX_DONE;
    }

    if (rv != DRIZZLE_RETURN_OK) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "drizzle_field_read() failed (%d: %s)",
                      drizzle_con_errno(&ctx->con),
                      drizzle_con_error(&ctx->con));
        return NGX_ERROR;
    }

    /* rv == DRIZZLE_RETURN_OK */

    *value = (u_char *) field;

    return NGX_OK;
}


static void
ngx_dbd_libdrizzle_read_event_handler(ngx_event_t *rev)
{
    short                      revents;
    ngx_dbd_t                 *dbd;
    ngx_connection_t          *c;
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    c = rev->data;
    dbd = c->data;
    ctx = dbd->ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle read event handler");

    /* TODO: error handling */

    if (rev->error) {
        revents = POLLERR;

    } else {
        revents = POLLIN;
    }

    drizzle_con_set_revents(&ctx->con, revents);

    if (dbd->handler != NULL) {
        dbd->handler(dbd->data);
    }
}


static void
ngx_dbd_libdrizzle_write_event_handler(ngx_event_t *wev)
{
    short                      revents;
    ngx_dbd_t                 *dbd;
    ngx_connection_t          *c;
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    c = wev->data;
    dbd = c->data;
    ctx = dbd->ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "dbd libdrizzle write event handler");

    /* TODO: error handling */

    if (wev->error) {
        revents = POLLERR;

    } else {
        revents = POLLOUT;
    }

    drizzle_con_set_revents(&ctx->con, revents);

    if (dbd->handler != NULL) {
        dbd->handler(dbd->data);
    }
}


#if (NGX_WIN32)

static ngx_int_t
ngx_dbd_libdrizzle_get_peer(ngx_peer_connection_t *pc, void *data)
{
    ngx_dbd_t *dbd = data;

    size_t                     len;
    u_char                    *p;
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "dbd libdrizzle get peer");

    ctx = dbd->ctx;

    pc->sockaddr = ctx->con.addrinfo_next->ai_addr;
    pc->socklen = ctx->con.addrinfo_next->ai_addrlen;

    pc->name = ngx_palloc(dbd->pool, sizeof(ngx_str_t));
    if (pc->name == NULL) {
        return NGX_ERROR;
    }

    len = NGX_INET_ADDRSTRLEN + sizeof(":65535") - 1;

    p = ngx_pnalloc(dbd->pool, len);
    if (p == NULL) {
        return NGX_ERROR;
    }

    len = ngx_sock_ntop(ctx->con.addrinfo_next->ai_addr, p, len, 1);

    pc->name->len = len;
    pc->name->data = p;

    return NGX_OK;
}


drizzle_return_t
drizzle_state_connect(drizzle_con_st *con)
{
    ngx_int_t                  rc;
    ngx_err_t                  err;
    ngx_dbd_t                 *dbd;
    struct addrinfo           *ai;
    ngx_connection_t          *c;
    ngx_peer_connection_t     *pc;
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    dbd = drizzle_con_context(con);

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "drizzle state connect");

    ctx = dbd->ctx;
    pc = &ctx->pc;

    /* TODO: closing previous socket descriptor */

    if (con->fd != -1) {
        if (dbd->opts & NGX_DBD_OPT_NON_BLOCKING) {
#if 0
            ngx_free_connection(pc->connection);
#else
            ngx_close_connection(pc->connection);
#endif

            pc->connection = NULL;

        } else {
            ngx_close_socket(con->fd);
        }

        con->fd = (ngx_socket_t) -1;
    }

    ai = con->addrinfo_next;
    if (ai == NULL) {
        drizzle_set_error(con->drizzle, "drizzle_state_connect",
                          "could not connect");
        drizzle_state_reset(con);
        return DRIZZLE_RETURN_COULD_NOT_CONNECT;
    }

    if (dbd->opts & NGX_DBD_OPT_NON_BLOCKING) {

        rc = ngx_event_connect_peer(pc);

        if (rc == NGX_ERROR) {
            drizzle_set_error(con->drizzle, "drizzle_state_connect",
                              "ngx_event_connect_peer() failed");
            con->drizzle->last_errno = ngx_socket_errno;
            return DRIZZLE_RETURN_ERRNO;
        }

        /* TODO: NGX_BUSY and NGX_DECLINED */

        if (rc == NGX_BUSY || rc == NGX_DECLINED) {
            con->addrinfo_next = ai->ai_next;
            return DRIZZLE_RETURN_OK;
        }

        c = pc->connection;

        c->log_error = NGX_ERROR_INFO;
        c->data = dbd;

        c->read->handler = ngx_dbd_libdrizzle_read_event_handler;
        c->write->handler = ngx_dbd_libdrizzle_write_event_handler;

        con->fd = c->fd;

        /* TODO: setting socket options */

        if (rc == NGX_AGAIN) {
            drizzle_state_pop(con);
            drizzle_state_push(con, drizzle_state_connecting);
            return DRIZZLE_RETURN_OK;
        }

        /* rc == NGX_OK */

    } else {

        con->fd = ngx_socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);

        if (con->fd == -1) {
            drizzle_set_error(con->drizzle, "drizzle_state_connect",
                              ngx_socket_n " failed");
            con->drizzle->last_errno = ngx_socket_errno;
            return DRIZZLE_RETURN_ERRNO;
        }

        /* TODO: setting socket options */

        rc = connect(con->fd, ai->ai_addr, ai->ai_addrlen);

        if (rc == -1) {
            err = ngx_socket_errno;

            if (err == NGX_EINPROGRESS || err == NGX_EAGAIN) {
                drizzle_state_pop(con);
                drizzle_state_push(con, drizzle_state_connecting);
                return DRIZZLE_RETURN_OK;
            }

            if (err == NGX_ECONNREFUSED
                || err == NGX_ENETUNREACH
                || err == NGX_ETIMEDOUT)
            {
                con->addrinfo_next = ai->ai_next;
                return DRIZZLE_RETURN_OK;
            }

            drizzle_set_error(con->drizzle, "drizzle_state_connect",
                              "connect() failed");

            con->drizzle->last_errno = err;

            return DRIZZLE_RETURN_ERRNO;
        }

        /* rc == 0 */
    }

    con->addrinfo_next = NULL;

    drizzle_state_pop(con);

    return DRIZZLE_RETURN_OK;
}


drizzle_return_t
drizzle_state_read(drizzle_con_st *con)
{
    u_char                    *buf;
    size_t                     size;
    ssize_t                    n;
    ngx_err_t                  err;
    ngx_dbd_t                 *dbd;
    drizzle_return_t           rv;
    ngx_connection_t          *c;
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    dbd = drizzle_con_context(con);

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "drizzle state read");

    if (con->buffer_size == 0) {
        con->buffer_ptr = con->buffer;

    } else if (con->buffer_ptr - con->buffer > DRIZZLE_MAX_BUFFER_SIZE / 2) {
        /* TODO: memmove */
        memmove(con->buffer, con->buffer_ptr, con->buffer_size);

        con->buffer_ptr = con->buffer;
    }

    buf = con->buffer_ptr + con->buffer_size;
    size = con->buffer + DRIZZLE_MAX_BUFFER_SIZE - buf;

    if (con->drizzle->options & DRIZZLE_NON_BLOCKING) {
        ctx = dbd->ctx;
        c = ctx->pc.connection;

        n = c->recv(c, buf, size);

        if (n == 0) {
            drizzle_set_error(con->drizzle, "drizzle_state_read",
                              "lost connection to server (EOF)");
            return DRIZZLE_RETURN_LOST_CONNECTION;
        }

        if (n == NGX_ERROR) {
            drizzle_set_error(con->drizzle, "drizzle_state_read",
                              "recv() failed");
            con->drizzle->last_errno = errno;
            return DRIZZLE_RETURN_ERRNO;
        }

        if (n == NGX_AGAIN) {
            rv = drizzle_con_set_events(con, POLLIN);
            if (rv != DRIZZLE_RETURN_OK) {
                return rv;
            }

            return DRIZZLE_RETURN_IO_WAIT;
        }

        /* n > 0 */

    } else {

retry:

        n = recv(con->fd, (char *) buf, size, 0);

        if (n == 0) {
            drizzle_set_error(con->drizzle, "drizzle_state_read",
                              "lost connection to server (EOF)");
            return DRIZZLE_RETURN_LOST_CONNECTION;
        }

        if (n == -1) {
            err = ngx_socket_errno;

            if (err == NGX_EAGAIN) {
                rv = drizzle_con_set_events(con, POLLIN);
                if (rv != DRIZZLE_RETURN_OK) {
                    return rv;
                }

                rv = drizzle_con_wait(con->drizzle);
                if (rv != DRIZZLE_RETURN_OK) {
                    return rv;
                }

                goto retry;
            }

            if (err == NGX_ECONNREFUSED) {
                con->revents = 0;

                drizzle_state_pop(con);
                drizzle_state_push(con, drizzle_state_connect);

                con->addrinfo_next = con->addrinfo_next->ai_next;

                return DRIZZLE_RETURN_OK;
            }

            if (err == NGX_ECONNRESET) {
                drizzle_set_error(con->drizzle, "drizzle_state_read",
                                  "lost connection to server");
                return DRIZZLE_RETURN_LOST_CONNECTION;
            }

            drizzle_set_error(con->drizzle, "drizzle_state_read",
                              "recv() failed");
            con->drizzle->last_errno = err;
            return DRIZZLE_RETURN_ERRNO;
        }

        /* n > 0 */
    }

    con->buffer_size += n;

    drizzle_state_pop(con);

    return DRIZZLE_RETURN_OK;
}


drizzle_return_t
drizzle_state_write(drizzle_con_st *con)
{
    ssize_t                    n;
    ngx_err_t                  err;
    ngx_dbd_t                 *dbd;
    drizzle_return_t           rv;
    ngx_connection_t          *c;
    ngx_dbd_libdrizzle_ctx_t  *ctx;

    dbd = drizzle_con_context(con);

    ngx_log_debug0(NGX_LOG_DEBUG_MYSQL, dbd->log, 0, "drizzle state write");

    while (con->buffer_size > 0) {

        if (con->drizzle->options & DRIZZLE_NON_BLOCKING) {
            ctx = dbd->ctx;
            c = ctx->pc.connection;

            n = c->send(c, con->buffer_ptr, con->buffer_size);

            if (n == 0) {
                drizzle_set_error(con->drizzle, "drizzle_state_write",
                                  "lost connection to server (EOF)");
                return DRIZZLE_RETURN_LOST_CONNECTION;
            }

            if (n == NGX_ERROR) {
                drizzle_set_error(con->drizzle, "drizzle_state_write",
                                  "send() failed");
                con->drizzle->last_errno = errno;
                return DRIZZLE_RETURN_ERRNO;
            }

            if (n == NGX_AGAIN) {
                rv = drizzle_con_set_events(con, POLLOUT);
                if (rv != DRIZZLE_RETURN_OK) {
                    return rv;
                }

                return DRIZZLE_RETURN_IO_WAIT;
            }

            /* n > 0 */

        } else {

            n = send(con->fd, (const char *) con->buffer_ptr, con->buffer_size,
                     0);

            if (n == 0) {
                drizzle_set_error(con->drizzle, "drizzle_state_write",
                                  "lost connection to server (EOF)");
                return DRIZZLE_RETURN_LOST_CONNECTION;
            }

            if (n == -1) {
                err = ngx_socket_errno;

                if (err == NGX_EAGAIN) {
                    rv = drizzle_con_set_events(con, POLLOUT);
                    if (rv != DRIZZLE_RETURN_OK) {
                        return rv;
                    }

                    rv = drizzle_con_wait(con->drizzle);
                    if (rv != DRIZZLE_RETURN_OK) {
                        return rv;
                    }

                    continue;
                }

                if (err == NGX_ECONNRESET) {
                    drizzle_set_error(con->drizzle, "drizzle_state_read",
                                      "lost connection to server");
                    return DRIZZLE_RETURN_LOST_CONNECTION;
                }

                drizzle_set_error(con->drizzle, "drizzle_state_read",
                                  "recv() failed");
                con->drizzle->last_errno = err;
                return DRIZZLE_RETURN_ERRNO;
            }

            /* n > 0 */
        }

        con->buffer_ptr += n;
        con->buffer_size -= n;
    }

    con->buffer_ptr = con->buffer;

    drizzle_state_pop(con);

    return DRIZZLE_RETURN_OK;
}

#endif
