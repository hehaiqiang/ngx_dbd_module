
/*
 * Copyright (C) Ngwsx
 */


#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <ngx_dbd.h>


typedef struct {
    u_char                      *driver;
    u_char                      *host;
    ngx_uint_t                   port;
    u_char                      *db;
    u_char                      *user;
    u_char                      *passwd;
    ngx_http_complex_value_t     sql;
} ngx_http_dbd_loc_conf_t;


typedef struct {
    ngx_str_t                    sql;
    ngx_dbd_t                   *dbd;

    ngx_uint_t                   connected;
    uint64_t                     row_count;
    uint64_t                     col_count;

    ngx_buf_t                   *header;
    ngx_chain_t                 *body;
    ngx_chain_t                 *last;
} ngx_http_dbd_ctx_t;


static ngx_int_t ngx_http_dbd_init(ngx_http_request_t *r,
    ngx_http_dbd_ctx_t *ctx);

static void ngx_http_dbd_connect(void *data);
static void ngx_http_dbd_result(void *data);
static void ngx_http_dbd_column(void *data);
static void ngx_http_dbd_row(void *data);
static void ngx_http_dbd_field(void *data);

static ngx_int_t ngx_http_dbd_output(ngx_http_request_t *r,
    ngx_http_dbd_ctx_t *ctx, u_char *buf, size_t size);
static void ngx_http_dbd_error(ngx_http_request_t *r, ngx_http_dbd_ctx_t *ctx);
static void ngx_http_dbd_cleanup(void *data);
static void ngx_http_dbd_finalize(ngx_http_request_t *r,
    ngx_http_dbd_ctx_t *ctx);

static ngx_int_t ngx_http_dbd_module_init(ngx_cycle_t *cycle);
static void *ngx_http_dbd_create_loc_conf(ngx_conf_t *cf);
static char *ngx_http_dbd_merge_loc_conf(ngx_conf_t *cf, void *parent,
    void *child);
static char *ngx_http_dbd_server(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
static char *ngx_http_dbd_query(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);


static ngx_command_t  ngx_http_dbd_commands[] = {

    { ngx_string("dbd_server"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_1MORE,
      ngx_http_dbd_server,
      NGX_HTTP_LOC_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("dbd_query"),
      NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_http_dbd_query,
      NGX_HTTP_LOC_CONF_OFFSET,
      0,
      NULL },

      ngx_null_command
};


static ngx_http_module_t  ngx_http_dbd_module_ctx = {
    NULL,                                  /* preconfiguration */
    NULL,                                  /* postconfiguration */

    NULL,                                  /* create main configuration */
    NULL,                                  /* init main configuration */

    NULL,                                  /* create server configuration */
    NULL,                                  /* merge server configuration */

    ngx_http_dbd_create_loc_conf,          /* create location configuration */
    ngx_http_dbd_merge_loc_conf            /* merge location configuration */
};


ngx_module_t  ngx_http_dbd_module = {
    NGX_MODULE_V1,
    &ngx_http_dbd_module_ctx,              /* module context */
    ngx_http_dbd_commands,                 /* module directives */
    NGX_HTTP_MODULE,                       /* module type */
    NULL,                                  /* init master */
    ngx_http_dbd_module_init,              /* init module */
    NULL,                                  /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    NULL,                                  /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};


extern ngx_dbd_driver_t  ngx_dbd_db2_driver;
extern ngx_dbd_driver_t  ngx_dbd_freetds_driver;
extern ngx_dbd_driver_t  ngx_dbd_libdrizzle_driver;
extern ngx_dbd_driver_t  ngx_dbd_mysql_driver;
extern ngx_dbd_driver_t  ngx_dbd_oracle_driver;
extern ngx_dbd_driver_t  ngx_dbd_pgsql_driver;
extern ngx_dbd_driver_t  ngx_dbd_sqlite2_driver;
extern ngx_dbd_driver_t  ngx_dbd_sqlite3_driver;


static ngx_int_t
ngx_http_dbd_handler(ngx_http_request_t *r)
{
    ngx_int_t                 rc;
    ngx_http_cleanup_t       *cln;
    ngx_http_dbd_ctx_t       *ctx;
    ngx_http_dbd_loc_conf_t  *dlcf;

    rc = ngx_http_discard_request_body(r);
    if (rc != NGX_OK) {
        return rc;
    }

    rc = ngx_http_set_content_type(r);
    if (rc != NGX_OK) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    ctx = ngx_pcalloc(r->pool, sizeof(ngx_http_dbd_ctx_t));
    if (ctx == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    ngx_http_set_ctx(r, ctx, ngx_http_dbd_module);

    dlcf = ngx_http_get_module_loc_conf(r, ngx_http_dbd_module);

    if (ngx_http_complex_value(r, &dlcf->sql, &ctx->sql) != NGX_OK) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    if (ngx_http_dbd_init(r, ctx) != NGX_OK) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    cln = ngx_http_cleanup_add(r, 0);
    if (cln == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    cln->handler = ngx_http_dbd_cleanup;
    cln->data = r;

    r->main->count++;

    return NGX_DONE;
}


static ngx_int_t
ngx_http_dbd_init(ngx_http_request_t *r, ngx_http_dbd_ctx_t *ctx)
{
    ngx_http_dbd_loc_conf_t  *dlcf;

    ctx->header = ngx_create_temp_buf(r->pool, ngx_pagesize);
    if (ctx->header == NULL) {
        return NGX_ERROR;
    }

    dlcf = ngx_http_get_module_loc_conf(r, ngx_http_dbd_module);

    ctx->dbd = ngx_dbd_create(r->pool, r->connection->log, dlcf->driver);
    if (ctx->dbd == NULL) {
        return NGX_ERROR;
    }

    /* TODO: configurable blocking mode */

    ngx_dbd_set_options(ctx->dbd, NGX_DBD_OPT_NON_BLOCKING);
    ngx_dbd_set_tcp(ctx->dbd, dlcf->host, (in_port_t) dlcf->port);
    ngx_dbd_set_auth(ctx->dbd, dlcf->user, dlcf->passwd);
    ngx_dbd_set_db(ctx->dbd, dlcf->db);
    ngx_dbd_set_handler(ctx->dbd, ngx_http_dbd_connect, r);

    ngx_http_dbd_connect(r);

    return NGX_OK;
}


static void
ngx_http_dbd_connect(void *data)
{
    ngx_http_request_t *r = data;

    ngx_int_t            rc;
    ngx_http_dbd_ctx_t  *ctx;

    ctx = ngx_http_get_module_ctx(r, ngx_http_dbd_module);

    rc = ngx_dbd_connect(ctx->dbd);

    if (rc == NGX_AGAIN) {
        return;
    }

    if (rc == NGX_ERROR) {
        ngx_http_dbd_error(r, ctx);
        return;
    }

    /* rc == NGX_OK */

    ctx->connected = 1;

    /* TODO: escape sql */

    ngx_dbd_set_sql(ctx->dbd, ctx->sql.data, ctx->sql.len);
    ngx_dbd_set_handler(ctx->dbd, ngx_http_dbd_result, r);

    ngx_http_dbd_result(r);
}


static void
ngx_http_dbd_result(void *data)
{
    ngx_http_request_t *r = data;

    ngx_int_t            rc;
    ngx_http_dbd_ctx_t  *ctx;

    ctx = ngx_http_get_module_ctx(r, ngx_http_dbd_module);

    rc = ngx_dbd_query(ctx->dbd);

    if (rc == NGX_AGAIN) {
        return;
    }

    if (rc == NGX_ERROR) {
        ngx_http_dbd_error(r, ctx);
        return;
    }

    /* rc == NGX_OK */

    ngx_dbd_set_handler(ctx->dbd, ngx_http_dbd_column, r);

    ngx_http_dbd_column(r);
}


static void
ngx_http_dbd_column(void *data)
{
    ngx_http_request_t *r = data;

    u_char              *buf;
    size_t               size;
    ngx_int_t            rc;
    ngx_http_dbd_ctx_t  *ctx;

    ctx = ngx_http_get_module_ctx(r, ngx_http_dbd_module);

    for ( ;; ) {

        rc = ngx_dbd_column_read(ctx->dbd);

        if (rc == NGX_AGAIN) {
            return;
        }

        if (rc == NGX_ERROR) {
            ngx_http_dbd_error(r, ctx);
            return;
        }

        if (rc == NGX_DONE) {
            break;
        }

        /* rc == NGX_OK */

        ctx->col_count++;

        buf = ngx_dbd_column_name(ctx->dbd);
        size = ngx_strlen(buf);

        ngx_http_dbd_output(r, ctx, buf, size);
    }

    /* rc == NGX_DONE */

    ngx_dbd_set_handler(ctx->dbd, ngx_http_dbd_row, r);

    ngx_http_dbd_row(r);
}


static void
ngx_http_dbd_row(void *data)
{
    ngx_http_request_t *r = data;

    int                  temp;
    uint64_t             temp_uint64;
    ngx_int_t            rc;
    ngx_buf_t           *b;
    ngx_http_dbd_ctx_t  *ctx;

    ctx = ngx_http_get_module_ctx(r, ngx_http_dbd_module);

    for ( ;; ) {

        rc = ngx_dbd_row_read(ctx->dbd);

        if (rc == NGX_AGAIN) {
            return;
        }

        if (rc == NGX_ERROR) {
            ngx_http_dbd_error(r, ctx);
            return;
        }

        if (rc == NGX_DONE) {
            break;
        }

        /* rc == NGX_OK */

        ctx->row_count++;

        ngx_dbd_set_handler(ctx->dbd, ngx_http_dbd_field, r);

        ngx_http_dbd_field(r);

        return;
    }

    /* rc == NGX_DONE */

    /* header: errno, error, col_count, row_count, affected_rows, insert_id */

    b = ctx->header;

    temp = 0;

    b->last = ngx_copy(b->last, &temp, sizeof(int));
    b->last = ngx_copy(b->last, &temp, sizeof(int));
    b->last = ngx_copy(b->last, &ctx->col_count, sizeof(uint64_t));
    b->last = ngx_copy(b->last, &ctx->row_count, sizeof(uint64_t));

    temp_uint64 = ngx_dbd_result_affected_rows(ctx->dbd);
    b->last = ngx_copy(b->last, &temp_uint64, sizeof(uint64_t));

    temp_uint64 = ngx_dbd_result_insert_id(ctx->dbd);
    b->last = ngx_copy(b->last, &temp_uint64, sizeof(uint64_t));

    ngx_http_dbd_finalize(r, ctx);
}


static void
ngx_http_dbd_field(void *data)
{
    ngx_http_request_t *r = data;

    off_t                offset;
    size_t               size, total;
    u_char              *value;
    ngx_int_t            rc;
    ngx_http_dbd_ctx_t  *ctx;

    ctx = ngx_http_get_module_ctx(r, ngx_http_dbd_module);

    for ( ;; ) {

        rc = ngx_dbd_field_read(ctx->dbd, &value, &offset, &size, &total);

        if (rc == NGX_AGAIN) {
            return;
        }

        if (rc == NGX_ERROR) {
            ngx_http_dbd_error(r, ctx);
            return;
        }

        if (rc == NGX_DONE) {
            break;
        }

        /* rc == NGX_OK */

        /* TODO: value, offset, size, total */

        ngx_http_dbd_output(r, ctx, value, size);
    }

    ngx_dbd_set_handler(ctx->dbd, ngx_http_dbd_row, r);

    ngx_http_dbd_row(r);
}


static ngx_int_t
ngx_http_dbd_output(ngx_http_request_t *r, ngx_http_dbd_ctx_t *ctx,
    u_char *buf, size_t size)
{
    ngx_buf_t    *b;
    ngx_chain_t  *cl;

    if (ctx->last == NULL
        || (size_t) (ctx->last->buf->end - ctx->last->buf->last)
           < size + sizeof(int))
    {
        cl = ngx_alloc_chain_link(r->pool);
        if (cl == NULL) {
            return NGX_ERROR;
        }

        cl->buf = ngx_create_temp_buf(r->pool, ngx_pagesize);
        if (cl->buf == NULL) {
            return NGX_ERROR;
        }

        cl->next = ctx->last;

        ctx->last = cl;

        if (ctx->body == NULL) {
            ctx->body = cl;
        }
    }

    b = ctx->last->buf;

    b->last = ngx_copy(b->last, &size, sizeof(int));
    b->last = ngx_copy(b->last, buf, size);

    return NGX_OK;
}


static void
ngx_http_dbd_error(ngx_http_request_t *r, ngx_http_dbd_ctx_t *ctx)
{
    int         temp;
    u_char     *errstr;
    size_t      size;
    ngx_buf_t  *b;

    /* header: errno, error */

    b = ctx->header;

    temp = ngx_dbd_error_code(ctx->dbd);
    b->last = ngx_copy(b->last, &temp, sizeof(int));

    errstr = ngx_dbd_error(ctx->dbd);
    size = ngx_strlen(errstr);

    b->last = ngx_copy(b->last, &size, sizeof(int));
    b->last = ngx_copy(b->last, errstr, size);

    ngx_http_dbd_finalize(r, ctx);
}


static void
ngx_http_dbd_cleanup(void *data)
{
    ngx_http_request_t *r = data;

    ngx_http_dbd_ctx_t  *ctx;

    ctx = ngx_http_get_module_ctx(r, ngx_http_dbd_module);

    if (ctx->dbd) {
        if (ctx->connected) {
            ngx_dbd_close(ctx->dbd);
        }

        ngx_dbd_destroy(ctx->dbd);
    }
}


static void
ngx_http_dbd_finalize(ngx_http_request_t *r, ngx_http_dbd_ctx_t *ctx)
{
    size_t       size;
    ngx_int_t    rc;
    ngx_chain_t  out, *cl;

    ngx_str_set(&r->headers_out.content_type, "text/html");

    if (r->method == NGX_HTTP_HEAD) {
        r->headers_out.status = NGX_HTTP_OK;

        rc = ngx_http_send_header(r);

        if (rc == NGX_ERROR || rc > NGX_OK || r->header_only) {
            ngx_http_finalize_request(r, rc);
            return;
        }
    }

    out.buf = ctx->header;
    out.next = ctx->body;

    size = 0;

    for (cl = &out; cl != NULL; cl = cl->next) {
        size += cl->buf->last - cl->buf->pos;
    }

    r->headers_out.status = NGX_HTTP_OK;
    r->headers_out.content_length_n = size;

    rc = ngx_http_send_header(r);

    if (rc == NGX_ERROR || rc > NGX_OK || r->header_only) {
        ngx_http_finalize_request(r, rc);
        return;
    }

    if (ctx->last != NULL) {
        ctx->last->buf->last_buf = 1;

    } else {
        ctx->header->last_buf = 1;
    }

    ngx_http_output_filter(r, &out);

    ngx_http_finalize_request(r, rc);
}


static ngx_int_t
ngx_http_dbd_module_init(ngx_cycle_t *cycle)
{
#if (NGX_DBD_DB2)
    ngx_dbd_add_driver(&ngx_dbd_db2_driver);
#endif

#if (NGX_DBD_FREETDS)
    ngx_dbd_add_driver(&ngx_dbd_freetds_driver);
#endif

#if (NGX_DBD_LIBDRIZZLE)
    ngx_dbd_add_driver(&ngx_dbd_libdrizzle_driver);
#endif

#if (NGX_DBD_MYSQL)
    ngx_dbd_add_driver(&ngx_dbd_mysql_driver);
#endif

#if (NGX_DBD_ORACLE)
    ngx_dbd_add_driver(&ngx_dbd_oracle_driver);
#endif

#if (NGX_DBD_PGSQL)
    ngx_dbd_add_driver(&ngx_dbd_pgsql_driver);
#endif

#if (NGX_DBD_SQLITE2)
    ngx_dbd_add_driver(&ngx_dbd_sqlite2_driver);
#endif

#if (NGX_DBD_SQLITE3)
    ngx_dbd_add_driver(&ngx_dbd_sqlite3_driver);
#endif

    return NGX_OK;
}


static void *
ngx_http_dbd_create_loc_conf(ngx_conf_t *cf)
{
    ngx_http_dbd_loc_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_dbd_loc_conf_t));
    if (conf == NULL) {
        return NULL;
    }

    conf->driver = NGX_CONF_UNSET_PTR;
    conf->host = NGX_CONF_UNSET_PTR;
    conf->port = NGX_CONF_UNSET_UINT;
    conf->db = NGX_CONF_UNSET_PTR;
    conf->user = NGX_CONF_UNSET_PTR;
    conf->passwd = NGX_CONF_UNSET_PTR;

    return conf;
}


static char *
ngx_http_dbd_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child)
{
    ngx_http_dbd_loc_conf_t *prev = parent;
    ngx_http_dbd_loc_conf_t *conf = child;

    /* TODO */

    if (conf->sql.value.len) {
    }

    ngx_conf_merge_ptr_value(conf->driver, prev->driver, (u_char *) "sqlite3");
    ngx_conf_merge_ptr_value(conf->host, prev->host, (u_char *) "localhost");
    ngx_conf_merge_uint_value(conf->port, prev->port, 3306);
    ngx_conf_merge_ptr_value(conf->db, prev->db, (u_char *) "mysql");
    ngx_conf_merge_ptr_value(conf->user, prev->user, (u_char *) "root");
    ngx_conf_merge_ptr_value(conf->passwd, prev->passwd, (u_char *) "123456");

    return NGX_CONF_OK;
}


static char *
ngx_http_dbd_server(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_dbd_loc_conf_t *dlcf = conf;

    size_t       size;
    u_char      *last;
    ngx_str_t   *value;
    ngx_uint_t   i;

    value = cf->args->elts;

    for (i = 1; i < cf->args->nelts; i++) {

        if (ngx_strncmp(value[i].data, "host=", 5) == 0) {
            size = value[i].len - 5;
            dlcf->host = ngx_palloc(cf->pool, size + 1);
            if (dlcf->host == NULL) {
                return NGX_CONF_ERROR;
            }
            last = ngx_copy(dlcf->host, value[i].data + 5, size);
            *last = '\0';
            continue;
        }

        if (ngx_strncmp(value[i].data, "port=", 5) == 0) {
            dlcf->port = (in_port_t) ngx_atoi(value[i].data + 5,
                                              value[i].len - 5);
            if ((ngx_int_t) dlcf->port == NGX_ERROR) {
                return NGX_CONF_ERROR;
            }
            continue;
        }

        if (ngx_strncmp(value[i].data, "db=", 3) == 0) {
            size = value[i].len - 3;
            dlcf->db = ngx_palloc(cf->pool, size + 1);
            if (dlcf->db == NULL) {
                return NGX_CONF_ERROR;
            }
            last = ngx_copy(dlcf->db, value[i].data + 3, size);
            *last = '\0';
            continue;
        }

        if (ngx_strncmp(value[i].data, "user=", 5) == 0) {
            size = value[i].len - 5;
            dlcf->user = ngx_palloc(cf->pool, size + 1);
            if (dlcf->user == NULL) {
                return NGX_CONF_ERROR;
            }
            last = ngx_copy(dlcf->user, value[i].data + 5, size);
            *last = '\0';
            continue;
        }

        if (ngx_strncmp(value[i].data, "passwd=", 7) == 0) {
            size = value[i].len - 7;
            dlcf->passwd = ngx_palloc(cf->pool, size + 1);
            if (dlcf->passwd == NULL) {
                return NGX_CONF_ERROR;
            }
            last = ngx_copy(dlcf->passwd, value[i].data + 7, size);
            *last = '\0';
            continue;
        }

        if (ngx_strncmp(value[i].data, "driver=", 7) == 0) {
            size = value[i].len - 7;
            dlcf->driver = ngx_palloc(cf->pool, size + 1);
            if (dlcf->driver == NULL) {
                return NGX_CONF_ERROR;
            }
            last = ngx_copy(dlcf->driver, value[i].data + 7, size);
            *last = '\0';
            continue;
        }

        goto invalid;
    }

    return NGX_CONF_OK;

invalid:

    ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                       "invalid parameter \"%V\" in dbd_server", &value[i]);

    return NGX_CONF_ERROR;
}


static char *
ngx_http_dbd_query(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_dbd_loc_conf_t *dlcf = conf;

    ngx_str_t                         *value;
    ngx_http_core_loc_conf_t          *clcf;
    ngx_http_compile_complex_value_t   ccv;

    value = cf->args->elts;

    ngx_memzero(&ccv, sizeof(ngx_http_compile_complex_value_t));

    ccv.cf = cf;
    ccv.value = &value[1];
    ccv.complex_value = &dlcf->sql;

    if (ngx_http_compile_complex_value(&ccv) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    clcf->handler = ngx_http_dbd_handler;

    return NGX_CONF_OK;
}
