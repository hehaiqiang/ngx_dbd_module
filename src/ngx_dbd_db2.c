
/*
 * Copyright (C) Ngwsx
 */


#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_dbd.h>


#if (NGX_DBD_DB2)


#include <sqlcli1.h>


#define NGX_DBD_DB2_DEF_BUFSIZE  1024


static ngx_int_t ngx_dbd_db2_process_init(ngx_cycle_t *cycle);
static void ngx_dbd_db2_process_exit(ngx_cycle_t *cycle);

static ngx_dbd_t *ngx_dbd_db2_open(ngx_pool_t *pool, ngx_log_t *log,
    ngx_str_t *conn_str, u_char *errstr);
static ngx_int_t ngx_dbd_db2_close(ngx_dbd_t *dbd);
static void *ngx_dbd_db2_native_handle(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_db2_check_conn(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_db2_select_db(ngx_dbd_t *dbd, u_char *dbname);
static ngx_dbd_tran_t *ngx_dbd_db2_start_tran(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_db2_end_tran(ngx_dbd_tran_t *tran);
static ngx_uint_t ngx_dbd_db2_get_tran_mode(ngx_dbd_tran_t *tran);
static ngx_uint_t ngx_dbd_db2_set_tran_mode(ngx_dbd_tran_t *tran,
    ngx_uint_t mode);
static ngx_int_t ngx_dbd_db2_exec(ngx_dbd_t *dbd, ngx_str_t *sql,
    int *affected);
static ngx_dbd_res_t *ngx_dbd_db2_query(ngx_dbd_t *dbd, ngx_str_t *sql,
    ngx_uint_t random);
static ngx_dbd_prep_t *ngx_dbd_db2_prepare(ngx_dbd_t *dbd, ngx_str_t *sql,
    ngx_uint_t type);
static ngx_int_t ngx_dbd_db2_pexec(ngx_dbd_prep_t *prep, void *argv,
    ngx_uint_t argc, int *affected);
static ngx_dbd_res_t *ngx_dbd_db2_pquery(ngx_dbd_prep_t *prep, void *argv,
    ngx_uint_t argc, ngx_uint_t random);
static ngx_int_t ngx_dbd_db2_next_res(ngx_dbd_res_t *res);
static ngx_int_t ngx_dbd_db2_num_fields(ngx_dbd_res_t *res);
static ngx_int_t ngx_dbd_db2_num_rows(ngx_dbd_res_t *res);
static ngx_str_t *ngx_dbd_db2_field_name(ngx_dbd_res_t *res, int col);
static ngx_dbd_row_t *ngx_dbd_db2_fetch_row(ngx_dbd_res_t *res, int row);
static ngx_str_t *ngx_dbd_db2_fetch_field(ngx_dbd_row_t *row, int col);
static ngx_int_t ngx_dbd_db2_get_field(ngx_dbd_row_t *row, int col,
    ngx_dbd_data_type_e type, void *data);
static u_char *ngx_dbd_db2_escape(ngx_dbd_t *dbd, u_char *str);
static int ngx_dbd_db2_errno(ngx_dbd_t *dbd);
static u_char *ngx_dbd_db2_strerror(ngx_dbd_t *dbd);

static ngx_int_t ngx_dbd_db2_bind_params(ngx_dbd_prep_t *prep,
    void *argv, ngx_uint_t argc);
static ngx_int_t ngx_dbd_db2_bind_results(ngx_dbd_res_t *res);
static void ngx_dbd_db2_free_result(ngx_dbd_res_t *res);

static void ngx_dbd_db2_clear_error(ngx_dbd_t *dbd);
static void ngx_dbd_db2_log_error(ngx_dbd_t *dbd, SQLSMALLINT type,
    SQLHANDLE handle, u_char *func);


static ngx_str_t  ngx_dbd_db2_name = ngx_string("db2");


static ngx_dbd_driver_t  ngx_dbd_db2_driver = {
    &ngx_dbd_db2_name,
    ngx_dbd_db2_open,
    ngx_dbd_db2_close,
    ngx_dbd_db2_native_handle,
    ngx_dbd_db2_check_conn,
    ngx_dbd_db2_select_db,
    ngx_dbd_db2_start_tran,
    ngx_dbd_db2_end_tran,
    ngx_dbd_db2_get_tran_mode,
    ngx_dbd_db2_set_tran_mode,
    ngx_dbd_db2_exec,
    ngx_dbd_db2_query,
    ngx_dbd_db2_prepare,
    ngx_dbd_db2_pexec,
    ngx_dbd_db2_pquery,
    ngx_dbd_db2_next_res,
    ngx_dbd_db2_num_fields,
    ngx_dbd_db2_num_rows,
    ngx_dbd_db2_field_name,
    ngx_dbd_db2_fetch_row,
    ngx_dbd_db2_fetch_field,
    ngx_dbd_db2_get_field,
    ngx_dbd_db2_escape,
    ngx_dbd_db2_errno,
    ngx_dbd_db2_strerror
};


static SQLHANDLE  env;


enum col_states {
    COL_AVAIL,
    COL_PRESENT,
    COL_BOUND,
    COL_RETRIEVED,
    COL_UNAVAIL
};


struct ngx_dbd_s {
    SQLHANDLE             dbc;

    SQLRETURN             rc;
    SQLINTEGER            err;
    SQLCHAR               state[128];
    SQLCHAR               errmsg[256];

    ngx_pool_t           *pool;
    ngx_log_t            *log;

    ngx_array_t           trans;
    ngx_list_t            preps;

    ngx_dbd_res_t        *res;
};


struct ngx_dbd_tran_s {
    ngx_uint_t            mode;
    ngx_dbd_t            *dbd;
};


struct ngx_dbd_prep_s {
    SQLHANDLE             stmt;

    ngx_dbd_t            *dbd;
};


struct ngx_dbd_res_s {
    SQLHANDLE             stmt;
    SQLSMALLINT           ncol;
    SQLLEN                nrow;

    u_char              **names;
    SQLPOINTER           *values;
    SQLINTEGER           *sizes;
    SQLINTEGER           *lens;
    SQLSMALLINT          *types;
    SQLLEN               *inds;
    int                  *states;
    int                  *unsigneds;

    ngx_dbd_prep_t       *prep;

    ngx_uint_t            random;
    ngx_str_t             field_name;

    ngx_dbd_t            *dbd;
    ngx_dbd_row_t        *row;
};


struct ngx_dbd_row_s {
    ngx_str_t             field;
    ngx_dbd_res_t        *res;
};


static ngx_int_t
ngx_dbd_db2_process_init(ngx_cycle_t *cycle)
{
    SQLRETURN  rc;

    rc = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    if (rc != SQL_SUCCESS) {
        ngx_log_error(NGX_LOG_ALERT, cycle->log, 0,
                      "SQLAllocHandle(SQL_HANDLE_ENV) failed (%d)", rc);
        return NGX_ERROR;
    }

    /* SQLSetEnvAttr */

    return NGX_OK;
}


static void
ngx_dbd_db2_process_exit(ngx_cycle_t *cycle)
{
    SQLRETURN  rc;

    rc = SQLFreeHandle(SQL_HANDLE_ENV, env);
    if (rc != SQL_SUCCESS) {
        ngx_log_error(NGX_LOG_ALERT, cycle->log, 0,
                      "SQLFreeHandle(SQL_HANDLE_ENV) failed (%d)", rc);
    }
}


static ngx_dbd_t *
ngx_dbd_db2_open(ngx_pool_t *pool, ngx_log_t *log, ngx_str_t *conn_str,
    u_char *errstr)
{
    size_t         klen, vlen;
    u_char        *params, *last, *ptr, *key, *value;
    ngx_dbd_t     *dbd;
    ngx_uint_t     i;
    ngx_keyval_t   fields[] = {
        { ngx_string("server"), ngx_null_string },
        { ngx_string("user"), ngx_null_string },
        { ngx_string("passwd"), ngx_null_string },
        { ngx_null_string, ngx_null_string }
    };

    /* parse connection string */

    params = conn_str->data;
    last = params + conn_str->len;

    for (ptr = ngx_strchr(params, '='); ptr && ptr < last;
         ptr = ngx_strchr(params, '='))
    {
        /* don't dereference memory that may not belong to us */

        if (ptr == params) {
            ++ptr;
            continue;
        }

        /* key */

        for (key = ptr - 1; isspace(*key); key--);

        klen = 0;

        while (isspace(*key) == 0) {

            /* don't parse backwards off the start of the string */

            if (key == params) {
                ++klen;
                break;
            }

            --key;
            ++klen;
        }

        /* value */

        for (value = ptr + 1; isspace(*value); value++);

        vlen = strcspn(value, " \r\n\t;|,");

        for (i = 0; fields[i].key.data; i++) {
            if (fields[i].key.len == klen
                && ngx_strncmp(fields[i].key.data, key, klen) == 0)
            {
                fields[i].value.len = vlen;
                fields[i].value.data = value;
                break;
            }
        }

        params = value + vlen + 1;
    }

    dbd = ngx_pcalloc(pool, sizeof(ngx_dbd_t));
    if (dbd == NULL) {
        return NULL;
    }

    dbd->rc = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbd->dbc);
    if (dbd->rc != SQL_SUCCESS) {
        ngx_dbd_db2_log_error(dbd, SQL_HANDLE_ENV, env, "SQLAllocHandle");
        return NULL;
    }

    /* SQLSetConnectAttr; */

    dbd->rc = SQLConnect(dbd->dbc,
                         fields[0].value.data, (int) fields[0].value.len,
                         fields[1].value.data, (int) fields[1].value.len,
                         fields[2].value.data, (int) fields[2].value.len);
    if (dbd->rc != SQL_SUCCESS) {
        ngx_dbd_db2_log_error(dbd, SQL_HANDLE_DBC, dbd->dbc, "SQLConnect");
        SQLFreeHandle(SQL_HANDLE_DBC, dbd->dbc);
        return NULL;
    }

    if (ngx_array_init(&dbd->trans, pool, 4, sizeof(ngx_dbd_tran_t *))
        != NGX_OK)
    {
        goto failed;
    }

    if (ngx_list_init(&dbd->preps, pool, 4, sizeof(ngx_dbd_prep_t *))
        != NGX_OK)
    {
        goto failed;
    }

    dbd->pool = pool;
    dbd->log = log;

    return dbd;

failed:

    SQLDisconnect(dbd->dbc);

    SQLFreeHandle(SQL_HANDLE_DBC, dbd->dbc);

    return NULL;
}


static ngx_int_t
ngx_dbd_db2_close(ngx_dbd_t *dbd)
{
    SQLRETURN          rc;
    ngx_uint_t         i;
#if 0
    ngx_dbd_tran_t   **trans;
#endif
    ngx_dbd_prep_t   **preps;
    ngx_list_part_t   *part;

#if 0
    if (dbd->res) {
        ngx_dbd_db2_free_result(dbd->res);
    }
#endif

#if 0
    /* end transaction */

    if (dbd->trans.nelts) {
        trans = dbd->trans.elts;

        for (i = dbd->trans.nelts - 1; i >= 0; i--) {
            ngx_dbd_mysql_end_tran(trans[i]);
        }
    }
#endif

    /* close prepared statement */

    part = &dbd->preps.part;
    preps = part->elts;

    for (i = 0 ;; i++) {

        if (i >= part->nelts) {
            if (part->next == NULL) {
                break;
            }

            part = part->next;
            preps = part->elts;
            i = 0;
        }

        /* mysql_stmt_close(preps[i]->stmt); */
    }

    rc = SQLDisconnect(dbd->dbc);
    if (rc != SQL_SUCCESS) {
        ngx_dbd_db2_log_error(dbd, SQL_HANDLE_DBC, dbd->dbc, "SQLDisconnect");
        return NGX_ERROR;
    }

    rc = SQLFreeHandle(SQL_HANDLE_DBC, dbd->dbc);
    if (rc != SQL_SUCCESS) {
        ngx_dbd_db2_log_error(dbd, SQL_HANDLE_DBC, dbd->dbc, "SQLFreeHandle");
        return NGX_ERROR;
    }

    return NGX_OK;
}


static void *
ngx_dbd_db2_native_handle(ngx_dbd_t *dbd)
{
    return *((void **) &dbd->dbc);
}


static ngx_int_t
ngx_dbd_db2_check_conn(ngx_dbd_t *dbd)
{
    return NGX_DECLINED;
}


static ngx_int_t
ngx_dbd_db2_select_db(ngx_dbd_t *dbd, u_char *dbname)
{
    return NGX_DECLINED;
}


static ngx_dbd_tran_t *
ngx_dbd_db2_start_tran(ngx_dbd_t *dbd)
{
    /* TODO */
    return NULL;
}


static ngx_int_t
ngx_dbd_db2_end_tran(ngx_dbd_tran_t *tran)
{
    /* TODO */
    return NGX_DECLINED;
}


static ngx_uint_t
ngx_dbd_db2_get_tran_mode(ngx_dbd_tran_t *tran)
{
    if (tran == NULL) {
        return NGX_DBD_TRANS_MODE_COMMIT;
    }

    return tran->mode;

    return 0;
}


static ngx_uint_t
ngx_dbd_db2_set_tran_mode(ngx_dbd_tran_t *tran, ngx_uint_t mode)
{
    if (tran == NULL) {
        return NGX_DBD_TRANS_MODE_COMMIT;
    }

    tran->mode = (mode & NGX_DBD_TRANS_MODE_BITS);

    return tran->mode;

    return 0;
}


static ngx_int_t
ngx_dbd_db2_exec(ngx_dbd_t *dbd, ngx_str_t *sql, int *affected)
{
    SQLLEN     nrow;
    SQLHANDLE  stmt;

    if (dbd->res) {
        ngx_dbd_db2_free_result(dbd->res);
    }

    ngx_dbd_db2_clear_error(dbd);

    dbd->rc = SQLAllocHandle(SQL_HANDLE_STMT, dbd->dbc, &stmt);
    if (dbd->rc != SQL_SUCCESS) {
        ngx_dbd_db2_log_error(dbd, SQL_HANDLE_DBC, dbd->dbc, "SQLAllocHandle");
        return NGX_ERROR;
    }

    dbd->rc = SQLExecDirect(stmt, sql->data, (SQLSMALLINT) sql->len);
    if (dbd->rc != SQL_SUCCESS) {
        ngx_dbd_db2_log_error(dbd, SQL_HANDLE_STMT, stmt, "SQLExecDirect");
        goto failed;
    }

    dbd->rc = SQLRowCount(stmt, &nrow);
    if (dbd->rc != SQL_SUCCESS) {
        ngx_dbd_db2_log_error(dbd, SQL_HANDLE_STMT, stmt, "SQLRowCount");
        goto failed;
    }

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    if (affected) {
        *affected = nrow;
    }

    return NGX_OK;

failed:

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    return NGX_ERROR;
}


static ngx_dbd_res_t *
ngx_dbd_db2_query(ngx_dbd_t *dbd, ngx_str_t *sql, ngx_uint_t random)
{
    SQLHANDLE       stmt;
    SQLSMALLINT     ncol;
    ngx_dbd_res_t  *res;

    res = dbd->res;

    if (res) {
        ngx_dbd_db2_free_result(res);
    }

    ngx_dbd_db2_clear_error(dbd);

    dbd->rc = SQLAllocHandle(SQL_HANDLE_STMT, dbd->dbc, &stmt);
    if (dbd->rc != SQL_SUCCESS) {
        ngx_dbd_db2_log_error(dbd, SQL_HANDLE_DBC, dbd->dbc, "SQLAllocHandle");
        return NULL;
    }

    if (random) {
        dbd->rc = SQLSetStmtAttr(stmt, SQL_ATTR_CURSOR_SCROLLABLE,
                                 (SQLPOINTER) SQL_SCROLLABLE, 0);
        if (dbd->rc != SQL_SUCCESS) {
            ngx_dbd_db2_log_error(dbd, SQL_HANDLE_STMT, stmt, "SQLSetStmtAttr");
            goto failed;
        }
    }

    dbd->rc = SQLExecDirect(stmt, sql->data, (SQLINTEGER) sql->len);
    if (dbd->rc != SQL_SUCCESS) {
        ngx_dbd_db2_log_error(dbd, SQL_HANDLE_STMT, stmt, "SQLExecDirect");
        goto failed;
    }

    ncol = 0;

    dbd->rc = SQLNumResultCols(stmt, &ncol);
    if (dbd->rc != SQL_SUCCESS) {
        ngx_dbd_db2_log_error(dbd, SQL_HANDLE_STMT, stmt, "SQLNumResultCols");
        goto failed;
    }

    if (res == NULL) {
        res = ngx_pcalloc(dbd->pool, sizeof(ngx_dbd_res_t));
        if (res == NULL) {
            goto failed;
        }

        res->dbd = dbd;
        dbd->res = res;
    }

    res->stmt = stmt;
    res->ncol = ncol;
    res->nrow = -1;
    res->random = random;

    if (ncol > 0) {
        if (ngx_dbd_db2_bind_results(res) != NGX_OK) {
            res->stmt = 0;
            goto failed;
        }
    }

    return res;

failed:

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    return NULL;
}


static ngx_dbd_prep_t *
ngx_dbd_db2_prepare(ngx_dbd_t *dbd, ngx_str_t *sql, ngx_uint_t type)
{
    /* TODO */
    return NULL;
}


static ngx_int_t
ngx_dbd_db2_pexec(ngx_dbd_prep_t *prep, void *argv, ngx_uint_t argc,
    int *affected)
{
    /* TODO */
    return NGX_DECLINED;
}


static ngx_dbd_res_t *
ngx_dbd_db2_pquery(ngx_dbd_prep_t *prep, void *argv, ngx_uint_t argc,
    ngx_uint_t random)
{
    /* TODO */
    return NULL;
}


static ngx_int_t
ngx_dbd_db2_next_res(ngx_dbd_res_t *res)
{
    /* TODO */
    return NGX_DECLINED;
}


static ngx_int_t
ngx_dbd_db2_num_fields(ngx_dbd_res_t *res)
{
    return res->ncol;
}


static ngx_int_t
ngx_dbd_db2_num_rows(ngx_dbd_res_t *res)
{
    ngx_dbd_t  *dbd;

    if (res->nrow != -1) {
        return res->nrow;
    }

    dbd = res->dbd;

    dbd->rc = SQLRowCount(res->stmt, &res->nrow);
    if (dbd->rc != SQL_SUCCESS) {
        ngx_dbd_db2_log_error(dbd, SQL_HANDLE_STMT, res->stmt, "SQLRowCount");

        res->nrow = -1;
    }

    return res->nrow;
}


static ngx_str_t *
ngx_dbd_db2_field_name(ngx_dbd_res_t *res, int col)
{
    ngx_dbd_t    *dbd;
    SQLSMALLINT   buf_size, len, type, dec, nullable;
    SQLUINTEGER   size;

    dbd = res->dbd;

    if (col < 0 || col >= res->ncol) {
        return NULL;
    }

    if (res->names[col] == NULL) {

        /* TODO: buf_size */
        buf_size = 128;

        res->names[col] = ngx_palloc(dbd->pool, buf_size);
        if (res->names[col] == NULL) {
            return NULL;
        }

        dbd->rc = SQLDescribeCol(res->stmt, col + 1, res->names[col], buf_size,
                                 &len, &type, &size, &dec, &nullable);
        if (dbd->rc != SQL_SUCCESS) {
            ngx_dbd_db2_log_error(dbd, SQL_HANDLE_STMT, res->stmt,
                                  "SQLDescribeCol");
            return NULL;
        }
    }

    res->field_name.len = ngx_strlen(res->names[col]);
    res->field_name.data = res->names[col];

    return &res->field_name;
}


static ngx_dbd_row_t *
ngx_dbd_db2_fetch_row(ngx_dbd_res_t *res, int row)
{
    u_char         *func;
    ngx_dbd_t      *dbd;
    ngx_dbd_row_t  *dbd_row;

    dbd = res->dbd;
    dbd_row = res->row;

    if (dbd_row == NULL) {
        dbd_row = ngx_palloc(dbd->pool, sizeof(ngx_dbd_row_t));
        if (dbd_row == NULL) {
            return NULL;
        }

        dbd_row->field.len = 0;
        dbd_row->field.data = NULL;
        dbd_row->res = res;

        res->row = dbd_row;
    }

    if (res->random && row >= 0) {
        func = "SQLFetchScroll";

        dbd->rc = SQLFetchScroll(res->stmt, SQL_FETCH_ABSOLUTE, row);

    } else {
        func = "SQLFetch";

        dbd->rc = SQLFetch(res->stmt);
    }

    if (dbd->rc != SQL_SUCCESS) {
        ngx_dbd_db2_log_error(dbd, SQL_HANDLE_STMT, res->stmt, func);
        return NULL;
    }

    return dbd_row;
}


static ngx_str_t *
ngx_dbd_db2_fetch_field(ngx_dbd_row_t *row, int col)
{
    ngx_dbd_t      *dbd;
    ngx_dbd_res_t  *res;

    res = row->res;
    dbd = res->dbd;

    if (col < 0 || col >= res->ncol) {
        return NULL;
    }

    dbd->rc = SQLGetData(res->stmt, col + 1, SQL_C_CHAR, res->values[col],
                         res->sizes[col], &res->inds[col]);
    if (dbd->rc != SQL_SUCCESS) {
        ngx_dbd_db2_log_error(dbd, SQL_HANDLE_STMT, res->stmt, "SQLGetData");
        return NULL;
    }

    row->field.data = res->values[col];
    row->field.len = ngx_strlen(res->values[col]);

    return &row->field;
}


static ngx_int_t
ngx_dbd_db2_get_field(ngx_dbd_row_t *row, int col, ngx_dbd_data_type_e type,
    void *data)
{
    /* TODO */
    return NGX_DECLINED;
}


static u_char *
ngx_dbd_db2_escape(ngx_dbd_t *dbd, u_char *str)
{
    /* TODO */
    return NULL;
}


static int
ngx_dbd_db2_errno(ngx_dbd_t *dbd)
{
    return dbd->err;
}


static u_char *
ngx_dbd_db2_strerror(ngx_dbd_t *dbd)
{
    return dbd->errmsg;
}


static ngx_int_t
ngx_dbd_db2_bind_params(ngx_dbd_prep_t *prep, void *argv, ngx_uint_t argc)
{
    /* TODO */

    return NGX_OK;
}


static ngx_int_t
ngx_dbd_db2_bind_results(ngx_dbd_res_t *res)
{
    int          i;
    ngx_dbd_t   *dbd;
    SQLINTEGER   max_size;

    dbd = res->dbd;

    res->names = ngx_pcalloc(dbd->pool, sizeof(u_char *) * res->ncol);
    if (res->names == NULL) {
        return NGX_ERROR;
    }

    res->values = ngx_pcalloc(dbd->pool, sizeof(SQLPOINTER) * res->ncol);
    if (res->values == NULL) {
        return NGX_ERROR;
    }

    res->sizes = ngx_pcalloc(dbd->pool, sizeof(SQLINTEGER) * res->ncol);
    if (res->sizes == NULL) {
        return NGX_ERROR;
    }

    res->lens = ngx_pcalloc(dbd->pool, sizeof(SQLINTEGER) * res->ncol);
    if (res->lens == NULL) {
        return NGX_ERROR;
    }

    res->types = ngx_pcalloc(dbd->pool, sizeof(SQLSMALLINT) * res->ncol);
    if (res->types == NULL) {
        return NGX_ERROR;
    }

    res->inds = ngx_pcalloc(dbd->pool, sizeof(SQLLEN) * res->ncol);
    if (res->inds == NULL) {
        return NGX_ERROR;
    }

    res->states = ngx_pcalloc(dbd->pool, sizeof(int) * res->ncol);
    if (res->states == NULL) {
        return NGX_ERROR;
    }

    res->unsigneds = ngx_pcalloc(dbd->pool, sizeof(int) * res->ncol);
    if (res->unsigneds == NULL) {
        return NGX_ERROR;
    }

    for (i = 0; i < res->ncol; i++) {

        /* SQL_DESC_UNSIGNED */

        dbd->rc = SQLColAttribute(res->stmt, i + 1, SQL_DESC_UNSIGNED,
                                  NULL, 0, NULL, &res->unsigneds[i]);
        if (dbd->rc != SQL_SUCCESS) {
            ngx_dbd_db2_log_error(dbd, SQL_HANDLE_STMT, res->stmt,
                                  "SQLColAttribute");

            res->unsigneds[i] = SQL_TRUE;
        }

        /* SQL_DESC_TYPE or SQL_DESC_CONCISE_TYPE */

        dbd->rc = SQLColAttribute(res->stmt, i + 1, SQL_DESC_TYPE,
                                  NULL, 0, NULL, &res->types[i]);
        if (dbd->rc != SQL_SUCCESS) {
            ngx_dbd_db2_log_error(dbd, SQL_HANDLE_STMT, res->stmt,
                                  "SQLColAttribute");

            dbd->rc = SQLColAttribute(res->stmt, i + 1, SQL_DESC_CONCISE_TYPE,
                                      NULL, 0, NULL, &res->types[i]);
            if (dbd->rc != SQL_SUCCESS) {
                ngx_dbd_db2_log_error(dbd, SQL_HANDLE_STMT, res->stmt,
                                      "SQLColAttribute");
            }
        }

        if (dbd->rc != SQL_SUCCESS) {
            res->types[i] = SQL_C_CHAR;
        }

        switch (res->types[i]) {

        case SQL_INTEGER:
        case SQL_SMALLINT:
        case SQL_TINYINT:
        case SQL_BIGINT:
            res->types[i] += res->unsigneds[i] == SQL_TRUE
                             ? SQL_UNSIGNED_OFFSET : SQL_SIGNED_OFFSET;
            break;
        case SQL_LONGVARCHAR:
            res->types[i] = SQL_LONGVARCHAR;
            break;
        case SQL_LONGVARBINARY:
            res->types[i] = SQL_LONGVARBINARY;
            break;
        case SQL_FLOAT:
            res->types[i] = SQL_C_FLOAT;
            break;
        case SQL_DOUBLE:
            res->types[i] = SQL_C_DOUBLE;
            break;
        case SQL_TIMESTAMP:
        case SQL_DATE:
        case SQL_TIME:
        default:
            res->types[i] = SQL_C_CHAR;
        }

        /* SQL_DESC_DISPLAY_SIZE */

        dbd->rc = SQLColAttribute(res->stmt, i + 1, SQL_DESC_DISPLAY_SIZE,
                                  NULL, 0, NULL, &res->sizes[i]);
        if (dbd->rc != SQL_SUCCESS) {
            ngx_dbd_db2_log_error(dbd, SQL_HANDLE_STMT, res->stmt,
                                  "SQLColAttribute");
        }

        if (dbd->rc != SQL_SUCCESS || res->sizes[i] < 0) {
            res->sizes[i] = NGX_DBD_DB2_DEF_BUFSIZE;
        }

        res->sizes[i]++;

        /* SQL_DESC_OCTET_LENGTH */

        dbd->rc = SQLColAttribute(res->stmt, i + 1, SQL_DESC_OCTET_LENGTH,
                                  NULL, 0, NULL, &res->lens[i]);
        if (dbd->rc != SQL_SUCCESS) {
            ngx_dbd_db2_log_error(dbd, SQL_HANDLE_STMT, res->stmt,
                                  "SQLColAttribute");
            res->lens[i] = res->sizes[i];
        }

        max_size = res->sizes[i] > res->lens[i] ? res->sizes[i] : res->lens[i];

        if (res->types[i] == SQL_LONGVARCHAR
            || res->types[i] == SQL_LONGVARBINARY
            || res->types[i] == SQL_VARBINARY
            || res->types[i] == -98
            || res->types[i] == -99
            || max_size <= 0)
        {
            max_size = NGX_DBD_DB2_DEF_BUFSIZE;

            /* TODO: max_size */
#if 0
            if (IS_LOB(res->types[i] && max_size < xxx) {
                max_size = xxx;
            }
#endif

            res->values[i] = NULL;
            res->states[i] = COL_AVAIL;

        } else {

            res->values[i] = ngx_pcalloc(dbd->pool, max_size);
            if (res->values[i] == NULL) {
                return NGX_ERROR;
            }

#if 0
            if (dbd->options & SQL_GD_BOUND) {
                dbd->rc = SQLBindCol(res->stmt, i + 1, res->types[i],
                                     res->values[i], max_size, &res->inds[i]);
                if (dbd->rc != SQL_SUCCESS) {
                    ngx_dbd_db2_log_error(dbd, SQL_HANDLE_STMT, res->stmt,
                                          "SQLBindCol");
                    res->states[i] = COL_AVAIL;

                } else {
                    res->states[i] = COL_BOUND;
                }

            } else {
#endif
                res->states[i] = COL_AVAIL;
#if 0
            }
#endif
        }

        res->sizes[i] = max_size;
        res->lens[i] = 0;
    }

    return NGX_OK;
}


static void ngx_dbd_db2_free_result(ngx_dbd_res_t *res)
{
    ngx_dbd_t  *dbd;

    dbd = res->dbd;

    if (res->stmt == 0) {
        return;
    }

    dbd->rc = SQLCloseCursor(res->stmt);
    if (dbd->rc != SQL_SUCCESS) {
        ngx_dbd_db2_log_error(dbd, SQL_HANDLE_STMT, res->stmt,
                              "SQLCloseCursor");
    }

    dbd->rc = SQLFreeHandle(SQL_HANDLE_STMT, res->stmt);
    if (dbd->rc != SQL_SUCCESS) {
        ngx_dbd_db2_log_error(dbd, SQL_HANDLE_STMT, res->stmt, "SQLFreeHandle");
    }

    res->stmt = 0;
}


static void
ngx_dbd_db2_clear_error(ngx_dbd_t *dbd)
{
    dbd->err = 0;
    dbd->state[0] = '\0';
    dbd->errmsg[0] = '\0';
}


static void
ngx_dbd_db2_log_error(ngx_dbd_t *dbd, SQLSMALLINT type, SQLHANDLE handle,
    u_char *func)
{
    SQLSMALLINT  n;

    /* TODO */

    dbd->rc = SQLGetDiagRec(type, handle, 1, dbd->state, &dbd->err,
                            dbd->errmsg, sizeof(dbd->errmsg), &n);
    if (dbd->rc != SQL_SUCCESS) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "SQLGetDiagRec() failed (%d)", dbd->rc);
        return;
    }

    ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                  "%s() failed (%d, state: %s, msg: %s)",
                  func, dbd->err, dbd->state, dbd->errmsg);
}


#endif
