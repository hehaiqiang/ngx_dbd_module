
/*
 * Copyright (C) Ngwsx
 */


#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_dbd.h>


#if (NGX_DBD_SQLITE2)


#include <sqlite.h>


static ngx_int_t ngx_dbd_sqlite2_process_init(ngx_cycle_t *cycle);
static void ngx_dbd_sqlite2_process_done(ngx_cycle_t *cycle);

static ngx_dbd_t *ngx_dbd_sqlite2_open(ngx_pool_t *pool, ngx_log_t *log,
    ngx_str_t *conn_str, u_char *errstr);
static ngx_int_t ngx_dbd_sqlite2_close(ngx_dbd_t *dbd);
static void *ngx_dbd_sqlite2_native_handle(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_sqlite2_check_conn(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_sqlite2_select_db(ngx_dbd_t *dbd, u_char *dbname);
static ngx_dbd_tran_t *ngx_dbd_sqlite2_start_tran(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_sqlite2_end_tran(ngx_dbd_tran_t *tran);
static ngx_uint_t ngx_dbd_sqlite2_get_tran_mode(ngx_dbd_tran_t *tran);
static ngx_uint_t ngx_dbd_sqlite2_set_tran_mode(ngx_dbd_tran_t *tran,
    ngx_uint_t mode);
static ngx_int_t ngx_dbd_sqlite2_exec(ngx_dbd_t *dbd, ngx_str_t *sql,
    int *affected);
static ngx_dbd_res_t *ngx_dbd_sqlite2_query(ngx_dbd_t *dbd, ngx_str_t *sql,
    ngx_uint_t random);
static ngx_dbd_prep_t *ngx_dbd_sqlite2_prepare(ngx_dbd_t *dbd, ngx_str_t *sql,
    ngx_uint_t type);
static ngx_int_t ngx_dbd_sqlite2_pexec(ngx_dbd_prep_t *prep, void *argv,
    ngx_uint_t argc, int *affected);
static ngx_dbd_res_t *ngx_dbd_sqlite2_pquery(ngx_dbd_prep_t *prep, void *argv,
    ngx_uint_t argc, ngx_uint_t random);
static ngx_int_t ngx_dbd_sqlite2_next_res(ngx_dbd_res_t *res);
static ngx_int_t ngx_dbd_sqlite2_num_fields(ngx_dbd_res_t *res);
static ngx_int_t ngx_dbd_sqlite2_num_rows(ngx_dbd_res_t *res);
static ngx_str_t *ngx_dbd_sqlite2_field_name(ngx_dbd_res_t *res, int col);
static ngx_dbd_row_t *ngx_dbd_sqlite2_fetch_row(ngx_dbd_res_t *res, int row);
static ngx_str_t *ngx_dbd_sqlite2_fetch_field(ngx_dbd_row_t *row, int col);
static ngx_int_t ngx_dbd_sqlite2_get_field(ngx_dbd_row_t *row, int col,
    ngx_dbd_data_type_e type, void *data);
static u_char *ngx_dbd_sqlite2_escape(ngx_dbd_t *dbd, const u_char *str);
static int ngx_dbd_sqlite2_errno(ngx_dbd_t *dbd);
static u_char *ngx_dbd_sqlite2_strerror(ngx_dbd_t *dbd);

static void ngx_dbd_sqlite2_free_errmsg(ngx_dbd_t *dbd);
static void ngx_dbd_sqlite2_free_result(ngx_dbd_res_t *res);


static ngx_str_t  ngx_dbd_sqlite2_name = ngx_string("sqlite2");


static ngx_dbd_driver_t  ngx_dbd_sqlite2_driver = {
    &ngx_dbd_sqlite2_name,
    ngx_dbd_sqlite2_open,
    ngx_dbd_sqlite2_close,
    ngx_dbd_sqlite2_native_handle,
    ngx_dbd_sqlite2_check_conn,
    ngx_dbd_sqlite2_select_db,
    ngx_dbd_sqlite2_start_tran,
    ngx_dbd_sqlite2_end_tran,
    ngx_dbd_sqlite2_get_tran_mode,
    ngx_dbd_sqlite2_set_tran_mode,
    ngx_dbd_sqlite2_exec,
    ngx_dbd_sqlite2_query,
    ngx_dbd_sqlite2_prepare,
    ngx_dbd_sqlite2_pexec,
    ngx_dbd_sqlite2_pquery,
    ngx_dbd_sqlite2_next_res,
    ngx_dbd_sqlite2_num_fields,
    ngx_dbd_sqlite2_num_rows,
    ngx_dbd_sqlite2_field_name,
    ngx_dbd_sqlite2_fetch_row,
    ngx_dbd_sqlite2_fetch_field,
    ngx_dbd_sqlite2_get_field,
    ngx_dbd_sqlite2_escape,
    ngx_dbd_sqlite2_errno,
    ngx_dbd_sqlite2_strerror
};


static ngx_dbd_module_t  ngx_dbd_sqlite2_module_ctx = {
    &ngx_dbd_sqlite2_driver,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
};


ngx_module_t  ngx_dbd_sqlite2_module = {
    NGX_MODULE_V1,
    &ngx_dbd_sqlite2_module_ctx,           /* module context */
    NULL,                                  /* module directives */
    NGX_DBD_MODULE,                        /* module type */
    NULL,                                  /* init master */
    NULL,                                  /* init module */
    ngx_dbd_sqlite2_process_init,          /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    ngx_dbd_sqlite2_process_done,          /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};


struct ngx_dbd_s {
    sqlite               *db;

    int                   err;
    u_char               *errmsg;
    u_char               *escaped;

    ngx_dbd_res_t        *res;
    ngx_dbd_tran_t       *tran;

    ngx_log_t            *log;
    ngx_pool_t           *pool;
};


struct ngx_dbd_tran_s {
    ngx_uint_t            mode;
    ngx_dbd_t            *dbd;
};


struct ngx_dbd_res_s {
    sqlite_vm            *vm;
    u_char               *tail;

    int                   nrow;
    int                   ncol;
    u_char              **result;
    u_char              **names;

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
ngx_dbd_sqlite2_process_init(ngx_cycle_t *cycle)
{
    ngx_log_debug1(NGX_LOG_DEBUG_CORE, cycle->log, 0,
                   "sqlite_version: %s", sqlite_version);

    ngx_log_debug1(NGX_LOG_DEBUG_CORE, cycle->log, 0,
                   "sqlite_encoding: %s", sqlite_encoding);

    return NGX_OK;
}


static void
ngx_dbd_sqlite2_process_done(ngx_cycle_t *cycle)
{
}


static ngx_dbd_t *
ngx_dbd_sqlite2_open(ngx_pool_t *pool, ngx_log_t *log, ngx_str_t *conn_str,
    u_char *errstr)
{
    u_char     *errmsg;
    sqlite     *db;
    ngx_dbd_t  *dbd;

    db = sqlite_open(conn_str->data, 0, &errmsg);
    if (db == NULL) {
        ngx_log_error(NGX_LOG_ALERT, log, 0,
                      "sqlite_open(\"%V\") failed (%s)", conn_str, errmsg);

        if (errstr) {
            ngx_cpystrn(errstr, errmsg, ngx_strlen(errmsg) + 1);
        }

        sqlite_freemem(errmsg);
        return NULL;
    }

    dbd = ngx_pcalloc(pool, sizeof(ngx_dbd_t));
    if (dbd == NULL) {
        sqlite_close(db);
        return NULL;
    }

    /*
     * set by ngx_pcalloc:
     *
     * dbd->err     = 0;
     * dbd->errmsg  = NULL;
     * dbd->escaped = NULL;
     * dbd->res     = NULL;
     * dbd->tran    = NULL;
     */

    dbd->db = db;
    dbd->log = log;
    dbd->pool = pool;

    return dbd;
}


static ngx_int_t
ngx_dbd_sqlite2_close(ngx_dbd_t *dbd)
{
    if (dbd->res) {
        ngx_dbd_sqlite2_free_result(dbd->res);
    }

    if (dbd->tran) {
        if (ngx_dbd_sqlite2_end_tran(dbd->tran) != NGX_OK) {
            return NGX_ERROR;
        }
    }

    if (dbd->errmsg) {
        sqlite_freemem(dbd->errmsg);
    }

    if (dbd->escaped) {
        sqlite_freemem(dbd->escaped);
    }

    sqlite_close(dbd->db);

    return NGX_OK;
}


static void *
ngx_dbd_sqlite2_native_handle(ngx_dbd_t *dbd)
{
    return dbd->db;
}


static ngx_int_t
ngx_dbd_sqlite2_check_conn(ngx_dbd_t *dbd)
{
    u_char  *sql;

    if (dbd->res) {
        ngx_dbd_sqlite2_free_result(dbd->res);
    }

    if (dbd->errmsg) {
        ngx_dbd_sqlite2_free_errmsg(dbd);
    }

    sql = "select count(*) from sqlite_master;";

    dbd->err = sqlite_exec(dbd->db, sql, NULL, NULL, &dbd->errmsg);
    if (dbd->err != SQLITE_OK) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "sqlite_exec(\"%s\") failed (%d: %s)",
                      sql, dbd->err, dbd->errmsg);
        return NGX_ERROR;
    }

    return NGX_OK;
}


static ngx_int_t
ngx_dbd_sqlite2_select_db(ngx_dbd_t *dbd, u_char *dbname)
{
    return NGX_DECLINED;
}


static ngx_dbd_tran_t *
ngx_dbd_sqlite2_start_tran(ngx_dbd_t *dbd)
{
    u_char          *sql;
    ngx_dbd_tran_t  *tran;

    tran = dbd->tran;

    if (tran && tran->dbd) {
        return tran;
    }

    if (dbd->errmsg) {
        ngx_dbd_sqlite2_free_errmsg(dbd);
    }

    sql = "begin transaction;";

    dbd->err = sqlite_exec(dbd->db, sql, NULL, NULL, &dbd->errmsg);
    if (dbd->err != SQLITE_OK) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "sqlite_exec(\"%s\") failed (%d: %s)",
                      sql, dbd->err, dbd->errmsg);
        return NULL;
    }

    if (tran == NULL) {
        tran = ngx_palloc(dbd->pool, sizeof(ngx_dbd_tran_t));
        if (tran == NULL) {
            return NULL;
        }

        dbd->tran = tran;
    }

    tran->dbd = dbd;
    tran->mode = NGX_DBD_TRANS_MODE_COMMIT;

    return tran;
}


static ngx_int_t
ngx_dbd_sqlite2_end_tran(ngx_dbd_tran_t *tran)
{
    u_char     *sql;
    ngx_dbd_t  *dbd;

    dbd = tran->dbd;

    if (dbd == NULL) {
        return NGX_OK;
    }

    if (dbd->errmsg) {
        ngx_dbd_sqlite2_free_errmsg(dbd);
    }

    if (NGX_DBD_TRANS_DO_COMMIT(tran)) {
        sql = "commit transaction;";

    } else {
        sql = "rollback transaction;";
    }

    dbd->err = sqlite_exec(dbd->db, sql, NULL, NULL, &dbd->errmsg);
    if (dbd->err != SQLITE_OK) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "sqlite_exec(\"%s\") failed (%d: %s)",
                      sql, dbd->err, dbd->errmsg);
        return NGX_ERROR;
    }

    tran->dbd = NULL;

    return NGX_OK;
}


static ngx_uint_t
ngx_dbd_sqlite2_get_tran_mode(ngx_dbd_tran_t *tran)
{
    if (tran == NULL) {
        return NGX_DBD_TRANS_MODE_COMMIT;
    }

    return tran->mode;
}


static ngx_uint_t
ngx_dbd_sqlite2_set_tran_mode(ngx_dbd_tran_t *tran, ngx_uint_t mode)
{
    if (tran == NULL) {
        return NGX_DBD_TRANS_MODE_COMMIT;
    }

    tran->mode = (mode & NGX_DBD_TRANS_MODE_BITS);

    return tran->mode;
}


static ngx_int_t
ngx_dbd_sqlite2_exec(ngx_dbd_t *dbd, ngx_str_t *sql, int *affected)
{
    ngx_dbd_res_t  *res;

    res = dbd->res;

    if (res) {
        ngx_dbd_sqlite2_free_result(res);
    }

    if (dbd->errmsg) {
        ngx_dbd_sqlite2_free_errmsg(dbd);
    }

    if (affected) {
        *affected = 0;
    }

    dbd->err = sqlite_exec(dbd->db, sql->data, NULL, NULL, &dbd->errmsg);
    if (dbd->err != SQLITE_OK) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "sqlite_exec(\"%V\") failed (%d: %s)",
                      sql, dbd->err, dbd->errmsg);
        return NGX_ERROR;
    }

    if (affected) {
        *affected = sqlite_changes(dbd->db);
    }

    return NGX_OK;
}


static ngx_dbd_res_t *
ngx_dbd_sqlite2_query(ngx_dbd_t *dbd, ngx_str_t *sql, ngx_uint_t random)
{
    int             nrow, ncol;
    u_char         *tail, **result, **names, *func;
    sqlite_vm      *vm;
    ngx_dbd_res_t  *res;

    res = dbd->res;

    if (res) {
        ngx_dbd_sqlite2_free_result(res);
    }

    if (dbd->errmsg) {
        ngx_dbd_sqlite2_free_errmsg(dbd);
    }

    if (random) {
        func = "sqlite_get_table";

        dbd->err = sqlite_get_table(dbd->db, sql->data, &result, &nrow, &ncol,
                                    &dbd->errmsg);

    } else {
        func = "sqlite_compile";

        dbd->err = sqlite_compile(dbd->db, sql->data, &tail, &vm, &dbd->errmsg);
    }

    if (dbd->err != SQLITE_OK) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "%s(\"%V\") failed (%d: %s)",
                      func, sql, dbd->err, dbd->errmsg);
        return NULL;
    }

    if (random ) {
        tail = NULL;
        names = NULL;
        vm = NULL;

    } else {
        dbd->err = sqlite_step(vm, &ncol, &result, &names);

        if (dbd->err == SQLITE_ROW) {
            nrow = 1;

        } else if (dbd->err == SQLITE_DONE) {
            nrow = 0;

        } else {
            ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                          "sqlite_step() failed (%d)", dbd->err);
            dbd->err = sqlite_finalize(vm, &dbd->errmsg);
            return NULL;
        }
    }

    if (res == NULL) {
        res = ngx_palloc(dbd->pool, sizeof(ngx_dbd_res_t));
        if (res == NULL) {
            if (random) {
                sqlite_free_table(result);

            } else {
                sqlite_finalize(vm, NULL);
            }

            return NULL;
        }

        res->row = NULL;
        dbd->res = res;
    }

    res->vm = vm;
    res->tail = tail;
    res->nrow = nrow;
    res->ncol = ncol;
    res->result = result;
    res->names = names;
    res->random = random;
    res->field_name.len = 0;
    res->field_name.data = NULL;
    res->dbd = dbd;

    return res;
}


static ngx_dbd_prep_t *
ngx_dbd_sqlite2_prepare(ngx_dbd_t *dbd, ngx_str_t *sql, ngx_uint_t type)
{
    /* TODO */

    return NULL;
}


static ngx_int_t
ngx_dbd_sqlite2_pexec(ngx_dbd_prep_t *prep, void *argv, ngx_uint_t argc,
    int *affected)
{
    /* TODO */

    return NGX_DECLINED;
}


static ngx_dbd_res_t *
ngx_dbd_sqlite2_pquery(ngx_dbd_prep_t *prep, void *argv, ngx_uint_t argc,
    ngx_uint_t random)
{
    /* TODO */

    return NULL;
}


static ngx_int_t
ngx_dbd_sqlite2_next_res(ngx_dbd_res_t *res)
{
    /* TODO */

    return NGX_DECLINED;
}


static int
ngx_dbd_sqlite2_num_fields(ngx_dbd_res_t *res)
{
    ngx_dbd_t  *dbd;

    dbd = res->dbd;

    if (dbd->errmsg) {
        ngx_dbd_sqlite2_free_errmsg(dbd);
    }

    return res->ncol;
}


static int
ngx_dbd_sqlite2_num_rows(ngx_dbd_res_t *res)
{
    ngx_dbd_t  *dbd;

    dbd = res->dbd;

    if (dbd->errmsg) {
        ngx_dbd_sqlite2_free_errmsg(dbd);
    }

    return res->nrow;
}


static ngx_str_t *
ngx_dbd_sqlite2_field_name(ngx_dbd_res_t *res, int col)
{
    u_char     *name;
    ngx_dbd_t  *dbd;

    dbd = res->dbd;

    if (dbd->errmsg) {
        ngx_dbd_sqlite2_free_errmsg(dbd);
    }

    if (col < 0 || col >= res->ncol) {
        return NULL;
    }

    if (res->random) {
        name = res->result[col];

    } else {
        name = res->names[col];
    }

    res->field_name.data = name;
    res->field_name.len = ngx_strlen(name);

    return &res->field_name;
}


static ngx_dbd_row_t *
ngx_dbd_sqlite2_fetch_row(ngx_dbd_res_t *res, int row)
{
    u_char         **result;
    ngx_dbd_t       *dbd;
    ngx_dbd_row_t   *dbd_row;

    dbd = res->dbd;

    if (dbd->errmsg) {
        ngx_dbd_sqlite2_free_errmsg(dbd);
    }

    if (res->nrow == 0) {
        return NULL;
    }

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
        if (row < -1 || row >= res->nrow) {
            return NULL;

        } else if (row == -1) {
            row = dbd_row->rownum + 1;
        }

        if (row >= res->nrow) {
            return NULL;
        }

        dbd_row->rownum = row;

    } else {
        if (dbd_row->rownum != -1) {
            dbd->err = sqlite_step(res->vm, NULL, &result, NULL);

            if (dbd->err == SQLITE_DONE) {
                return NULL;

            } else if (dbd->err != SQLITE_ROW) {
                ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                              "sqlite_step() failed (%d)", dbd->err);
                dbd->err = sqlite_finalize(res->vm, &dbd->errmsg);
                return NULL;
            }

            res->nrow++;
            res->result = result;
        }

        dbd_row->rownum++;
    }

    return dbd_row;
}


static ngx_str_t *
ngx_dbd_sqlite2_fetch_field(ngx_dbd_row_t *row, int col)
{
    ngx_dbd_t      *dbd;
    ngx_dbd_res_t  *res;

    res = row->res;
    dbd = res->dbd;

    if (dbd->errmsg) {
        ngx_dbd_sqlite2_free_errmsg(dbd);
    }

    if (col < 0 || col >= res->ncol) {
        return NULL;
    }

    if (res->random) {
        col += (row->rownum + 1) * res->ncol;
    }

    row->field.data = res->result[col];
    row->field.len = ngx_strlen(res->result[col]);

    return &row->field;
}


static ngx_int_t
ngx_dbd_sqlite2_get_field(ngx_dbd_row_t *row, int col, ngx_dbd_data_type_e type,
    void *data)
{
    /* TODO */

    return NGX_DECLINED;
}


static u_char *
ngx_dbd_sqlite2_escape(ngx_dbd_t *dbd, const u_char *str)
{
    if (dbd->escaped) {
        sqlite_freemem(dbd->escaped);
    }

    dbd->escaped = sqlite_mprintf("%q", str);
    if (dbd->escaped == NULL) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "sqlite_mprintf(\"%s\") failed", str);
        return NULL;
    }

    return dbd->escaped;
}


static int
ngx_dbd_sqlite2_errno(ngx_dbd_t *dbd)
{
    return dbd->err;
}


static u_char *
ngx_dbd_sqlite2_strerror(ngx_dbd_t *dbd)
{
    return dbd->errmsg;
}


static void
ngx_dbd_sqlite2_free_errmsg(ngx_dbd_t *dbd)
{
    dbd->err = SQLITE_OK;

    if (dbd->errmsg) {
        sqlite_freemem(dbd->errmsg);
        dbd->errmsg = NULL;
    }
}


static void
ngx_dbd_sqlite2_free_result(ngx_dbd_res_t *res)
{
    u_char         *errmsg;
    ngx_dbd_t      *dbd;
    ngx_dbd_row_t  *row;

    dbd = res->dbd;
    row = res->row;

    if (dbd == NULL) {
        return;
    }

    if (res->random) {
        sqlite_free_table(res->result);

    } else {
        dbd->err = sqlite_finalize(res->vm, &errmsg);
        if (dbd->err != SQLITE_OK) {
            ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                          "sqlite_finalize() failed (%d: %s)",
                          dbd->err, errmsg);

            sqlite_freemem(errmsg);
        }
    }

    ngx_memzero(res, sizeof(ngx_dbd_res_t));

    if (row) {
        row->rownum = -1;
    }

    res->nrow = -1;
    res->row = row;
}


#endif