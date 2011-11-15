
/*
 * Copyright (C) Ngwsx
 */


#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_dbd.h>


#if (NGX_DBD_POSTGRESQL)


#include <libpq-fe.h>


static ngx_int_t ngx_dbd_postgresql_process_init(ngx_cycle_t *cycle);
static void ngx_dbd_postgresql_process_exit(ngx_cycle_t *cycle);

static ngx_dbd_t *ngx_dbd_postgresql_open(ngx_pool_t *pool, ngx_log_t *log,
    ngx_str_t *conn_str, u_char *errstr);
static ngx_int_t ngx_dbd_postgresql_close(ngx_dbd_t *dbd);
static void *ngx_dbd_postgresql_native_handle(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_postgresql_check_conn(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_postgresql_select_db(ngx_dbd_t *dbd, u_char *dbname);
static ngx_dbd_tran_t *ngx_dbd_postgresql_start_tran(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_postgresql_end_tran(ngx_dbd_tran_t *tran);
static ngx_uint_t ngx_dbd_postgresql_get_tran_mode(ngx_dbd_tran_t *tran);
static ngx_uint_t ngx_dbd_postgresql_set_tran_mode(ngx_dbd_tran_t *tran,
    ngx_uint_t mode);
static ngx_int_t ngx_dbd_postgresql_exec(ngx_dbd_t *dbd, ngx_str_t *sql,
    int *affected);
static ngx_dbd_res_t *ngx_dbd_postgresql_query(ngx_dbd_t *dbd, ngx_str_t *sql,
    ngx_uint_t random);
static ngx_dbd_prep_t *ngx_dbd_postgresql_prepare(ngx_dbd_t *dbd,
    ngx_str_t *sql, ngx_uint_t type);
static ngx_int_t ngx_dbd_postgresql_pexec(ngx_dbd_prep_t *prep, void *argv,
    ngx_uint_t argc, int *affected);
static ngx_dbd_res_t *ngx_dbd_postgresql_pquery(ngx_dbd_prep_t *prep,
    void *argv, ngx_uint_t argc, ngx_uint_t random);
static ngx_int_t ngx_dbd_postgresql_next_res(ngx_dbd_res_t *res);
static ngx_int_t ngx_dbd_postgresql_num_fields(ngx_dbd_res_t *res);
static ngx_int_t ngx_dbd_postgresql_num_rows(ngx_dbd_res_t *res);
static ngx_str_t *ngx_dbd_postgresql_field_name(ngx_dbd_res_t *res, int col);
static ngx_dbd_row_t *ngx_dbd_postgresql_fetch_row(ngx_dbd_res_t *res, int row);
static ngx_str_t *ngx_dbd_postgresql_fetch_field(ngx_dbd_row_t *row, int col);
static ngx_int_t ngx_dbd_postgresql_get_field(ngx_dbd_row_t *row, int col,
    ngx_dbd_data_type_e type, void *data);
static u_char *ngx_dbd_postgresql_escape(ngx_dbd_t *dbd, u_char *str);
static int ngx_dbd_postgresql_errno(ngx_dbd_t *dbd);
static u_char *ngx_dbd_postgresql_strerror(ngx_dbd_t *dbd);

static void ngx_dbd_postgresql_free_result(ngx_dbd_res_t *res);
static void ngx_dbd_postgresql_notice_receiver(void *arg, const PGresult *res);
static void ngx_dbd_postgresql_notice_processor(void *arg, const char *msg);


static ngx_str_t  ngx_dbd_postgresql_name = ngx_string("postgresql");


static ngx_dbd_driver_t  ngx_dbd_postgresql_driver = {
    &ngx_dbd_postgresql_name,
    ngx_dbd_postgresql_open,
    ngx_dbd_postgresql_close,
    ngx_dbd_postgresql_native_handle,
    ngx_dbd_postgresql_check_conn,
    ngx_dbd_postgresql_select_db,
    ngx_dbd_postgresql_start_tran,
    ngx_dbd_postgresql_end_tran,
    ngx_dbd_postgresql_get_tran_mode,
    ngx_dbd_postgresql_set_tran_mode,
    ngx_dbd_postgresql_exec,
    ngx_dbd_postgresql_query,
    ngx_dbd_postgresql_prepare,
    ngx_dbd_postgresql_pexec,
    ngx_dbd_postgresql_pquery,
    ngx_dbd_postgresql_next_res,
    ngx_dbd_postgresql_num_fields,
    ngx_dbd_postgresql_num_rows,
    ngx_dbd_postgresql_field_name,
    ngx_dbd_postgresql_fetch_row,
    ngx_dbd_postgresql_fetch_field,
    ngx_dbd_postgresql_get_field,
    ngx_dbd_postgresql_escape,
    ngx_dbd_postgresql_errno,
    ngx_dbd_postgresql_strerror
};


static ngx_dbd_module_t  ngx_dbd_postgresql_module_ctx = {
    &ngx_dbd_postgresql_driver,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
};


ngx_module_t  ngx_dbd_postgresql_module = {
    NGX_MODULE_V1,
    &ngx_dbd_postgresql_module_ctx,
    NULL,
    NGX_DBD_MODULE,
    NULL,
    NULL,
    ngx_dbd_postgresql_process_init,
    NULL,
    NULL,
    ngx_dbd_postgresql_process_exit,
    NULL,
    NGX_MODULE_V1_PADDING,
};


struct ngx_dbd_s {
    PGconn               *conn;

    ngx_str_t             escaped;

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
    ngx_dbd_t            *dbd;
};


struct ngx_dbd_res_s {
    PGresult             *res;

    ngx_uint_t            random;
    ngx_str_t             field_name;

    ngx_dbd_t            *dbd;
    ngx_dbd_row_t        *row;
};


struct ngx_dbd_row_s {
    int                   rownum;
    ngx_str_t             field;
    ngx_dbd_res_t        *res;
};


static ngx_int_t
ngx_dbd_postgresql_process_init(ngx_cycle_t *cycle)
{
    ngx_log_debug1(NGX_LOG_DEBUG_CORE, cycle->log, 0,
                   "PQisthreadsafe: %d", PQisthreadsafe());

    return NGX_OK;
}


static void
ngx_dbd_postgresql_process_exit(ngx_cycle_t *cycle)
{
    /* TODO */
}


static ngx_dbd_t *
ngx_dbd_postgresql_open(ngx_pool_t *pool, ngx_log_t *log, ngx_str_t *conn_str,
    u_char *errstr)
{
    u_char     *errmsg;
    PGconn     *conn;
    ngx_dbd_t  *dbd;

    conn = PQconnectdb(conn_str->data);
    if (conn == NULL) {
        return NULL;
    }

    if (PQstatus(conn) != CONNECTION_OK) {
        errmsg = PQerrorMessage(conn);

        ngx_log_error(NGX_LOG_ALERT, log, 0,
                      "PQconnectdb() failed (%s)", errmsg);

        if (errstr) {
            ngx_cpystrn(errstr, errmsg, ngx_strlen(errmsg) + 1);
        }

        PQfinish(conn);
        return NULL;
    }

    dbd = ngx_palloc(pool, sizeof(ngx_dbd_t));
    if (dbd == NULL) {
        PQfinish(conn);
        return NULL;
    }

    dbd->conn = conn;
    dbd->escaped.len = 0;
    dbd->escaped.data = NULL;
    dbd->pool = pool;
    dbd->log = log;
    dbd->res = NULL;

    /* TODO: dbd->trans, dbd->preps */

    PQsetNoticeReceiver(conn, ngx_dbd_postgresql_notice_receiver, dbd);
    PQsetNoticeProcessor(conn, ngx_dbd_postgresql_notice_processor, dbd);

    ngx_log_debug1(NGX_LOG_DEBUG_CORE, log, 0, "PQdb: %s", PQdb(conn));
    ngx_log_debug1(NGX_LOG_DEBUG_CORE, log, 0, "PQuser: %s", PQuser(conn));
    ngx_log_debug1(NGX_LOG_DEBUG_CORE, log, 0, "PQpass: %s", PQpass(conn));
    ngx_log_debug1(NGX_LOG_DEBUG_CORE, log, 0, "PQhost: %s", PQhost(conn));
    ngx_log_debug1(NGX_LOG_DEBUG_CORE, log, 0, "PQport: %s", PQport(conn));
    ngx_log_debug1(NGX_LOG_DEBUG_CORE, log, 0,
                   "PQoptions: %s", PQoptions(conn));

    return dbd;
}


static ngx_int_t
ngx_dbd_postgresql_close(ngx_dbd_t *dbd)
{
    /* TODO */

    PQfinish(dbd->conn);

    return NGX_OK;
}


static void *
ngx_dbd_postgresql_native_handle(ngx_dbd_t *dbd)
{
    return dbd->conn;
}


static ngx_int_t
ngx_dbd_postgresql_check_conn(ngx_dbd_t *dbd)
{
    if (PQstatus(dbd->conn) == CONNECTION_OK) {
        return NGX_OK;
    }

    PQreset(dbd->conn);

    if (PQstatus(dbd->conn) != CONNECTION_OK) {
        return NGX_ERROR;
    }

    return NGX_OK;
}


static ngx_int_t
ngx_dbd_postgresql_select_db(ngx_dbd_t *dbd, u_char *dbname)
{
    return NGX_DECLINED;
}


static ngx_dbd_tran_t *
ngx_dbd_postgresql_start_tran(ngx_dbd_t *dbd)
{
    /* TODO */
    return NULL;
}


static ngx_int_t
ngx_dbd_postgresql_end_tran(ngx_dbd_tran_t *tran)
{
    /* TODO */
    return NGX_DECLINED;
}


static ngx_uint_t
ngx_dbd_postgresql_get_tran_mode(ngx_dbd_tran_t *tran)
{
    if (tran == NULL) {
        return NGX_DBD_TRANS_MODE_COMMIT;
    }

    return tran->mode;

    return 0;
}


static ngx_uint_t
ngx_dbd_postgresql_set_tran_mode(ngx_dbd_tran_t *tran, ngx_uint_t mode)
{
    if (tran == NULL) {
        return NGX_DBD_TRANS_MODE_COMMIT;
    }

    tran->mode = (mode & NGX_DBD_TRANS_MODE_BITS);

    return tran->mode;

    return 0;
}


static ngx_int_t
ngx_dbd_postgresql_exec(ngx_dbd_t *dbd, ngx_str_t *sql, int *affected)
{
    u_char         *buf;
    PGresult       *res;
    ngx_dbd_res_t  *dbd_res;

    dbd_res = dbd->res;

    if (dbd_res) {
        ngx_dbd_postgresql_free_result(dbd_res);
    }

    if (affected) {
        *affected = 0;
    }

    res = PQexec(dbd->conn, sql->data);
    if (res == NULL) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "PQexec() failed (%s)", PQerrorMessage(dbd->conn));
        return NGX_ERROR;
    }

    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "PQresultStatus() != PGRES_COMMAND_OK (%s)",
                      PQresultErrorMessage(res));
        PQclear(res);
        return NGX_ERROR;
    }

    if (affected) {
        buf = PQcmdTuples(res);

        if (buf != NULL && buf != "") {
            *affected = (int) ngx_atoi(buf, ngx_strlen(buf));
        }
    }

    PQclear(res);

    return NGX_OK;
}


static ngx_dbd_res_t *
ngx_dbd_postgresql_query(ngx_dbd_t *dbd, ngx_str_t *sql, ngx_uint_t random)
{
    PGresult       *res;
    ngx_dbd_res_t  *dbd_res;

    dbd_res = dbd->res;

    if (dbd_res) {
        ngx_dbd_postgresql_free_result(dbd_res);
    }

    res = PQexec(dbd->conn, sql->data);
    if (res == NULL) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "PQexec() failed (%s)", PQerrorMessage(dbd->conn));
        return NULL;
    }

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "PQresultStatus() != PGRES_TUPLES_OK (%s)",
                      PQresultErrorMessage(res));
        PQclear(res);
        return NULL;
    }

    if (dbd_res == NULL) {
        dbd_res = ngx_palloc(dbd->pool, sizeof(ngx_dbd_res_t));
        if (dbd_res == NULL) {
            PQclear(res);
            return NULL;
        }

        dbd_res->dbd = dbd;
        dbd_res->row = NULL;

        dbd->res = dbd_res;
    }

    dbd_res->res = res;
    dbd_res->random = random;
    dbd_res->field_name.len = 0;
    dbd_res->field_name.data = NULL;

    return dbd_res;
}


static ngx_dbd_prep_t *
ngx_dbd_postgresql_prepare(ngx_dbd_t *dbd, ngx_str_t *sql, ngx_uint_t type)
{
    /* TODO */
    return NULL;
}


static ngx_int_t
ngx_dbd_postgresql_pexec(ngx_dbd_prep_t *prep, void *argv, ngx_uint_t argc,
    int *affected)
{
    /* TODO */
    return NGX_DECLINED;
}


static ngx_dbd_res_t *
ngx_dbd_postgresql_pquery(ngx_dbd_prep_t *prep, void *argv, ngx_uint_t argc,
    ngx_uint_t random)
{
    /* TODO */
    return NULL;
}


static ngx_int_t
ngx_dbd_postgresql_next_res(ngx_dbd_res_t *res)
{
    /* TODO */
    return NGX_DECLINED;
}


static ngx_int_t
ngx_dbd_postgresql_num_fields(ngx_dbd_res_t *res)
{
    return PQnfields(res->res);
}


static ngx_int_t
ngx_dbd_postgresql_num_rows(ngx_dbd_res_t *res)
{
    return PQntuples(res->res);
}


static ngx_str_t *
ngx_dbd_postgresql_field_name(ngx_dbd_res_t *res, int col)
{
    u_char  *name;

    if (col < 0 || col >= PQntuples(res->res)) {
        return NULL;
    }

    name = PQfname(res->res, col);

    res->field_name.data = name;
    res->field_name.len = ngx_strlen(name);

    return &res->field_name;
}


static ngx_dbd_row_t *
ngx_dbd_postgresql_fetch_row(ngx_dbd_res_t *res, int row)
{
    ngx_dbd_t      *dbd;
    ngx_dbd_row_t  *dbd_row;

    dbd = res->dbd;
    dbd_row = res->row;

    if (dbd_row == NULL) {
        dbd_row = ngx_palloc(dbd->pool, sizeof(ngx_dbd_row_t));
        if (dbd_row == NULL) {
            return NULL;
        }

        dbd_row->rownum = -1;
        dbd_row->field.len = 0;
        dbd_row->field.data = NULL;
        dbd_row->res = res;

        res->row = dbd_row;
    }

    if (res->random) {
        if (row < -1 || row >= PQntuples(res->res)) {
            return NULL;

        } else if (row == -1) {
            row = dbd_row->rownum + 1;
        }

        dbd_row->rownum = row;

    } else {
        dbd_row->rownum++;
    }

    if (dbd_row->rownum >= PQntuples(res->res)) {
        return NULL;
    }

    return dbd_row;
}


static ngx_str_t *
ngx_dbd_postgresql_fetch_field(ngx_dbd_row_t *row, int col)
{
    ngx_dbd_res_t  *res;

    res = row->res;

    if (col < 0 || col >= PQntuples(res->res)) {
        return NULL;
    }

    row->field.data = PQgetvalue(res->res, row->rownum, col);
    row->field.len = PQgetlength(res->res, row->rownum, col);

    return &row->field;
}


static ngx_int_t
ngx_dbd_postgresql_get_field(ngx_dbd_row_t *row, int col,
    ngx_dbd_data_type_e type, void *data)
{
    /* TODO */
    return NGX_DECLINED;
}


static u_char *
ngx_dbd_postgresql_escape(ngx_dbd_t *dbd, u_char *str)
{
    size_t  len, max, rlen;

    len = ngx_strlen(str);

    /* TODO: configurable max length of escape buffer */

    max = 1024;
    max = (max < len ? len : max) * 2 + 2;

    if (dbd->escaped.len < max) {
        dbd->escaped.data = ngx_palloc(dbd->pool, max);
        if (dbd->escaped.data == NULL) {
            return NULL;
        }

        dbd->escaped.len = max;
    }

    rlen = PQescapeString(dbd->escaped.data, str, len);

    return dbd->escaped.data;
}


static int
ngx_dbd_postgresql_errno(ngx_dbd_t *dbd)
{
    return PQstatus(dbd->conn);
}


static u_char *
ngx_dbd_postgresql_strerror(ngx_dbd_t *dbd)
{
    return PQerrorMessage(dbd->conn);
}


static void
ngx_dbd_postgresql_free_result(ngx_dbd_res_t *res)
{
    if (res->res == NULL) {
        return;
    }

    PQclear(res->res);

    res->res = NULL;
}


static void
ngx_dbd_postgresql_notice_receiver(void *arg, const PGresult *res)
{
    /* TODO */
}


static void
ngx_dbd_postgresql_notice_processor(void *arg, const char *msg)
{
    /* TODO */
}


#endif
