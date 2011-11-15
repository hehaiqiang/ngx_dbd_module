
/*
 * Copyright (C) Ngwsx
 */


#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_dbd.h>


#if (NGX_DBD_ORACLE)


#include <oci.h>


static ngx_int_t ngx_dbd_oracle_process_init(ngx_cycle_t *cycle);
static void ngx_dbd_oracle_process_exit(ngx_cycle_t *cycle);

static ngx_dbd_t *ngx_dbd_oracle_open(ngx_pool_t *pool, ngx_log_t *log,
    ngx_str_t *conn_str, u_char *errstr);
static ngx_int_t ngx_dbd_oracle_close(ngx_dbd_t *dbd);
static void *ngx_dbd_oracle_native_handle(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_oracle_check_conn(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_oracle_select_db(ngx_dbd_t *dbd, u_char *dbname);
static ngx_dbd_tran_t *ngx_dbd_oracle_start_tran(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_oracle_end_tran(ngx_dbd_tran_t *tran);
static ngx_uint_t ngx_dbd_oracle_get_tran_mode(ngx_dbd_tran_t *tran);
static ngx_uint_t ngx_dbd_oracle_set_tran_mode(ngx_dbd_tran_t *tran,
    ngx_uint_t mode);
static ngx_int_t ngx_dbd_oracle_exec(ngx_dbd_t *dbd, ngx_str_t *sql,
    int *affected);
static ngx_dbd_res_t *ngx_dbd_oracle_query(ngx_dbd_t *dbd, ngx_str_t *sql,
    ngx_uint_t random);
static ngx_dbd_prep_t *ngx_dbd_oracle_prepare(ngx_dbd_t *dbd, ngx_str_t *sql,
    ngx_uint_t type);
static ngx_int_t ngx_dbd_oracle_pexec(ngx_dbd_prep_t *prep, void *argv,
    ngx_uint_t argc, int *affected);
static ngx_dbd_res_t *ngx_dbd_oracle_pquery(ngx_dbd_prep_t *prep, void *argv,
    ngx_uint_t argc, ngx_uint_t random);
static ngx_int_t ngx_dbd_oracle_next_res(ngx_dbd_res_t *res);
static ngx_int_t ngx_dbd_oracle_num_fields(ngx_dbd_res_t *res);
static ngx_int_t ngx_dbd_oracle_num_rows(ngx_dbd_res_t *res);
static ngx_str_t *ngx_dbd_oracle_field_name(ngx_dbd_res_t *res, int col);
static ngx_dbd_row_t *ngx_dbd_oracle_fetch_row(ngx_dbd_res_t *res, int row);
static ngx_str_t *ngx_dbd_oracle_fetch_field(ngx_dbd_row_t *row, int col);
static ngx_int_t ngx_dbd_oracle_get_field(ngx_dbd_row_t *row, int col,
    ngx_dbd_data_type_e type, void *data);
static u_char *ngx_dbd_oracle_escape(ngx_dbd_t *dbd, u_char *str);
static int ngx_dbd_oracle_errno(ngx_dbd_t *dbd);
static u_char *ngx_dbd_oracle_strerror(ngx_dbd_t *dbd);

static ngx_int_t ngx_dbd_oracle_bind(ngx_dbd_prep_t *prep, void *argv,
    ngx_uint_t argc);
static void ngx_dbd_oracle_free_result(ngx_dbd_res_t *res);


static ngx_str_t  ngx_dbd_oracle_name = ngx_string("oracle");


static ngx_dbd_driver_t  ngx_dbd_oracle_driver = {
    &ngx_dbd_oracle_name,
    ngx_dbd_oracle_open,
    ngx_dbd_oracle_close,
    ngx_dbd_oracle_native_handle,
    ngx_dbd_oracle_check_conn,
    ngx_dbd_oracle_select_db,
    ngx_dbd_oracle_start_tran,
    ngx_dbd_oracle_end_tran,
    ngx_dbd_oracle_get_tran_mode,
    ngx_dbd_oracle_set_tran_mode,
    ngx_dbd_oracle_exec,
    ngx_dbd_oracle_query,
    ngx_dbd_oracle_prepare,
    ngx_dbd_oracle_pexec,
    ngx_dbd_oracle_pquery,
    ngx_dbd_oracle_next_res,
    ngx_dbd_oracle_num_fields,
    ngx_dbd_oracle_num_rows,
    ngx_dbd_oracle_field_name,
    ngx_dbd_oracle_fetch_row,
    ngx_dbd_oracle_fetch_field,
    ngx_dbd_oracle_get_field,
    ngx_dbd_oracle_escape,
    ngx_dbd_oracle_errno,
    ngx_dbd_oracle_strerror
};


static ngx_dbd_module_t  ngx_dbd_oracle_module_ctx = {
    &ngx_dbd_oracle_driver,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
};


ngx_module_t  ngx_dbd_oracle_module = {
    NGX_MODULE_V1,
    &ngx_dbd_oracle_module_ctx,
    NULL,
    NGX_DBD_MODULE,
    NULL,
    NULL,
    ngx_dbd_oracle_process_init,
    NULL,
    NULL,
    ngx_dbd_oracle_process_exit,
    NULL,
    NGX_MODULE_V1_PADDING,
};


static OCIEnv  *env;


typedef struct {
    int                   type;
    sb2                   ind;
    sb4                   len;
    void                 *value;
    OCIBind              *bind;
} ora_param_t;


typedef struct {
    int                   type;
    sb2                   ind;
    ub2                   len;
    size_t                size;
    ngx_str_t             name;
    void                 *value;
    OCIDefine            *def;
} ora_field_t;


struct ngx_dbd_s {
    OCIError             *error;
    OCIServer            *srv;
    OCISvcCtx            *svc_ctx;
    OCISession           *session;

    sword                 err;
    u_char                errbuf[256];

    ngx_uint_t            mode;

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
    OCIStmt              *stmt;
    ora_field_t          *fields;
    ora_param_t          *params;
    ngx_dbd_t            *dbd;
};


struct ngx_dbd_res_s {
    OCIStmt              *stmt;
    ngx_dbd_prep_t       *prep;

    ngx_uint_t            random;

    ngx_dbd_t            *dbd;
    ngx_dbd_row_t        *row;
};


struct ngx_dbd_row_s {
    ngx_str_t             field;
    ngx_dbd_res_t        *res;
};


static ngx_int_t
ngx_dbd_oracle_process_init(ngx_cycle_t *cycle)
{
    sword       rc;
    ngx_uint_t  mode;

#if 0
#ifdef OCI_NEW_LENGTH_SEMANTICS
    mode = OCI_DEFAULT; /* OCI_THREADED|OCI_NEW_LENGTH_SEMANTICS; */
#else
    mode = OCI_DEFAULT; /* OCI_THREADED; */
#endif
#endif

    mode = OCI_THREADED;

    rc = OCIInitialize(mode, NULL, NULL, NULL, NULL);
    if (rc == OCI_ERROR) {
        return NGX_ERROR;
    }

    rc = OCIEnvInit(&env, mode, NULL, NULL);
    if (rc == OCI_ERROR) {
        return NGX_ERROR;
    }

    return NGX_OK;
}


static void
ngx_dbd_oracle_process_exit(ngx_cycle_t *cycle)
{
    sword  rc;

    if (env) {
        rc = OCIHandleFree(env, OCI_HTYPE_ENV);
    }

    rc = OCITerminate(OCI_DEFAULT);

    /* TODO: rc */
}


static ngx_dbd_t *
ngx_dbd_oracle_open(ngx_pool_t *pool, ngx_log_t *log, ngx_str_t *conn_str,
    u_char *errstr)
{
    int            errcode;
    size_t         klen, vlen;
    u_char        *params, *last, *ptr, *key, *value;
    u_char         errbuf[512];
    ngx_dbd_t     *dbd;
    ngx_uint_t     i;
    ngx_keyval_t   fields[] = {
        { ngx_string("db"), ngx_null_string },
        { ngx_string("user"), ngx_null_string },
        { ngx_string("passwd"), ngx_null_string },
        { ngx_string("conn_mode"), ngx_null_string },
        { ngx_null_string, ngx_null_string }
    };

    dbd = ngx_pcalloc(pool, sizeof(ngx_dbd_t));
    if (dbd == NULL) {
        return NULL;
    }

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

    if (fields[3].value.len == sizeof("sysdba") - 1
        && ngx_strncmp(fields[3].value.data,
                       "sysdba", sizeof("sysdba") - 1) == 0)
    {
        dbd->mode = OCI_SYSDBA;

    } else if (fields[3].value.len == sizeof("sysoper") - 1
               && ngx_strncmp(fields[3].value.data,
                              "sysoper", sizeof("sysoper") - 1) == 0)
    {
        dbd->mode = OCI_SYSOPER;

    } else {
        dbd->mode = OCI_DEFAULT;
    }

    dbd->err = OCIHandleAlloc(env, &dbd->error, OCI_HTYPE_ERROR, 0, NULL);
    if (dbd->err == OCI_ERROR) {
        return NULL;
    }

    dbd->err = OCIHandleAlloc(env, &dbd->srv, OCI_HTYPE_SERVER, 0, NULL);
    if (dbd->err == OCI_ERROR) {
        goto failed;
    }

    dbd->err = OCIHandleAlloc(env, &dbd->svc_ctx, OCI_HTYPE_SVCCTX, 0, NULL);
    if (dbd->err == OCI_ERROR) {
        goto failed;
    }

    dbd->err = OCIHandleAlloc(env, &dbd->session, OCI_HTYPE_SESSION, 0, NULL);
    if (dbd->err == OCI_ERROR) {
        goto failed;
    }

    dbd->err = OCIServerAttach(dbd->srv, dbd->error,
                               fields[0].value.data, fields[0].value.len,
                               OCI_DEFAULT);
    if (dbd->err == OCI_ERROR) {
        goto failed;
    }

    dbd->err = OCIAttrSet(dbd->svc_ctx, OCI_HTYPE_SVCCTX, dbd->srv, 0,
                          OCI_ATTR_SERVER, dbd->error);
    if (dbd->err == OCI_ERROR) {
        goto failed;
    }

    dbd->err = OCIAttrSet(dbd->session, OCI_HTYPE_SESSION,
                          fields[1].value.data, fields[1].value.len,
                          OCI_ATTR_USERNAME, dbd->error);
    if (dbd->err == OCI_ERROR) {
        goto failed;
    }

    dbd->err = OCIAttrSet(dbd->session, OCI_HTYPE_SESSION,
                          fields[2].value.data, fields[2].value.len,
                          OCI_ATTR_PASSWORD, dbd->error);
    if (dbd->err == OCI_ERROR) {
        goto failed;
    }

    dbd->err = OCISessionBegin(dbd->svc_ctx, dbd->error, dbd->session,
                               OCI_CRED_RDBMS, dbd->mode);
    if (dbd->err == OCI_ERROR) {
        goto failed;
    }

    dbd->err = OCIAttrSet(dbd->svc_ctx, OCI_HTYPE_SVCCTX, dbd->session, 0,
                          OCI_ATTR_SESSION, dbd->error);
    if (dbd->err == OCI_ERROR) {
        goto failed;
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

    OCIErrorGet(dbd->error, 1, NULL, &errcode, errbuf, sizeof(errbuf),
                OCI_HTYPE_ERROR);

    /* TODO */

    if (errstr) {
        ngx_cpystrn(errstr, errbuf, sizeof(errbuf));
    }

    ngx_log_error(NGX_LOG_EMERG, log, 0,
                  "ngx_dbd_oracle_open() failed (%s)", errbuf);

    ngx_dbd_oracle_close(dbd);

    return NULL;
}


static ngx_int_t
ngx_dbd_oracle_close(ngx_dbd_t *dbd)
{
    ngx_uint_t         i;
    ngx_dbd_tran_t   **trans;
    ngx_dbd_prep_t   **preps;
    ngx_list_part_t   *part;

    if (dbd->res) {
        ngx_dbd_oracle_free_result(dbd->res);
    }

    /* end transaction */

    if (dbd->trans.nelts) {
        trans = dbd->trans.elts;

        for (i = dbd->trans.nelts - 1; i >= 0; i--) {
            ngx_dbd_oracle_end_tran(trans[i]);
        }
    }

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

        OCIHandleFree(preps[i]->stmt, OCI_HTYPE_STMT);
    }

    if (dbd->svc_ctx && dbd->error && dbd->session) {
        OCISessionEnd(dbd->svc_ctx, dbd->error, dbd->session, dbd->mode);
    }

    if (dbd->srv && dbd->error) {
        OCIServerDetach(dbd->srv, dbd->error, OCI_DEFAULT);
    }

    if (dbd->session) {
        OCIHandleFree(dbd->session, OCI_HTYPE_SESSION);
    }

    if (dbd->svc_ctx) {
        OCIHandleFree(dbd->svc_ctx, OCI_HTYPE_SVCCTX);
    }

    if (dbd->srv) {
        OCIHandleFree(dbd->srv, OCI_HTYPE_SERVER);
    }

    if (dbd->error) {
        OCIHandleFree(dbd->error, OCI_HTYPE_ERROR);
    }

    return NGX_OK;
}


static void *
ngx_dbd_oracle_native_handle(ngx_dbd_t *dbd)
{
    return dbd->svc_ctx;
}


static ngx_int_t
ngx_dbd_oracle_check_conn(ngx_dbd_t *dbd)
{
    return NGX_DECLINED;
}


static ngx_int_t
ngx_dbd_oracle_select_db(ngx_dbd_t *dbd, u_char *dbname)
{
    return NGX_DECLINED;
}


static ngx_dbd_tran_t *
ngx_dbd_oracle_start_tran(ngx_dbd_t *dbd)
{
    /* TODO */
    return NULL;
}


static ngx_int_t
ngx_dbd_oracle_end_tran(ngx_dbd_tran_t *tran)
{
    /* TODO */
    return NGX_DECLINED;
}


static ngx_uint_t
ngx_dbd_oracle_get_tran_mode(ngx_dbd_tran_t *tran)
{
    if (tran == NULL) {
        return NGX_DBD_TRANS_MODE_COMMIT;
    }

    return tran->mode;

    return 0;
}


static ngx_uint_t
ngx_dbd_oracle_set_tran_mode(ngx_dbd_tran_t *tran, ngx_uint_t mode)
{
    if (tran == NULL) {
        return NGX_DBD_TRANS_MODE_COMMIT;
    }

    tran->mode = (mode & NGX_DBD_TRANS_MODE_BITS);

    return tran->mode;

    return 0;
}


static ngx_int_t
ngx_dbd_oracle_exec(ngx_dbd_t *dbd, ngx_str_t *sql, int *affected)
{
    ngx_dbd_prep_t  *prep;

    prep = ngx_dbd_oracle_prepare(dbd, sql, NGX_DBD_CMD_TYPE_STMT);
    if (prep == NULL) {
        return NGX_ERROR;
    }

    /* TODO: prep */

    return ngx_dbd_oracle_pexec(prep, NULL, 0, affected);
}


static ngx_dbd_res_t *
ngx_dbd_oracle_query(ngx_dbd_t *dbd, ngx_str_t *sql, ngx_uint_t random)
{
    ngx_dbd_prep_t  *prep;

    prep = ngx_dbd_oracle_prepare(dbd, sql, NGX_DBD_CMD_TYPE_STMT);
    if (prep == NULL) {
        return NULL;
    }

    /* TODO: prep */

    return ngx_dbd_oracle_pquery(prep, NULL, 0, random);
}


static ngx_dbd_prep_t *
ngx_dbd_oracle_prepare(ngx_dbd_t *dbd, ngx_str_t *sql, ngx_uint_t type)
{
    int              ftype, n, i;
    ub2             *ptr;
    size_t           len;
    OCIStmt         *stmt;
    OCIParam        *param;
    ora_field_t     *fields;
    ngx_dbd_prep_t  *prep, **pprep;

    dbd->err = OCIHandleAlloc(env, &stmt, OCI_HTYPE_STMT, 0, NULL);
    if (dbd->err == OCI_ERROR) {
        return NULL;
    }

    dbd->err = OCIStmtPrepare(stmt, dbd->error, sql->data, sql->len,
                              OCI_NTV_SYNTAX, OCI_DEFAULT);
    if (dbd->err == OCI_ERROR) {
        goto failed;
    }

    prep = ngx_palloc(dbd->pool, sizeof(ngx_dbd_prep_t));
    if (prep == NULL) {
        goto failed;
    }

    pprep = ngx_list_push(&dbd->preps);
    if (prep == NULL) {
        goto failed;
    }

    *pprep = prep;

    prep->stmt = stmt;
    prep->fields = NULL;
    prep->params = NULL;
    prep->dbd = dbd;

    type = 0;

    dbd->err = OCIAttrGet(stmt, OCI_HTYPE_STMT, &type, sizeof(type),
                          OCI_ATTR_STMT_TYPE, dbd->error);
    if (dbd->err == OCI_ERROR) {
        goto failed;
    }

    if (type != OCI_STMT_SELECT) {
        goto done;
    }

    dbd->err = OCIStmtExecute(dbd->svc_ctx, stmt, dbd->error, 0, 0, NULL, NULL,
                              OCI_DESCRIBE_ONLY);
    if (dbd->err == OCI_ERROR) {
        goto failed;
    }

    n = 0;

    dbd->err = OCIAttrGet(stmt, OCI_HTYPE_STMT, &n, 0,
                          OCI_ATTR_PARAM_COUNT, dbd->error);
    if (dbd->err == OCI_ERROR) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "OCIAttrGet(OCI_ATTR_PARAM_COUNT) failed (%s)",
                      ngx_dbd_oracle_strerror(dbd));
        goto failed;
    }

    fields = ngx_pcalloc(dbd->pool, sizeof(ora_field_t) * n);
    if (fields == NULL) {
        goto failed;
    }

    for (i = 0; i < n; i++) {
        dbd->err = OCIParamGet(stmt, OCI_HTYPE_STMT, dbd->error, &param, i + 1);
        if (dbd->err == OCI_ERROR) {
            goto failed;
        }

        dbd->err = OCIAttrGet(param, OCI_DTYPE_PARAM, &fields[i].type, 0,
                              OCI_ATTR_DATA_TYPE, dbd->error);
        if (dbd->err == OCI_ERROR) {
            goto failed;
        }

        dbd->err = OCIAttrGet(param, OCI_DTYPE_PARAM, &fields[i].len, 0,
                              OCI_ATTR_DATA_SIZE, dbd->error);
        if (dbd->err == OCI_ERROR) {
            goto failed;
        }

        fields[i].size = fields[i].len;

        dbd->err = OCIAttrGet(param, OCI_DTYPE_PARAM,
                              &fields[i].name.data, &fields[i].name.len,
                              OCI_ATTR_NAME, dbd->error);
        if (dbd->err == OCI_ERROR) {
            goto failed;
        }

        switch (fields[i].type) {

        case SQLT_NUM:
            fields[i].size = 172;
            break;
        case SQLT_CHR:
        case SQLT_AFC:
            fields[i].size *= 4;
            fields[i].size++;
            break;
        case SQLT_DAT:
            fields[i].size = 76;
            break;
        case SQLT_BIN:
            fields[i].size *= 2;
            fields[i].size++;
            break;
        case SQLT_RID:
        case SQLT_RDD:
            fields[i].size = 21;
            break;
        case SQLT_TIMESTAMP:
        case SQLT_TIMESTAMP_TZ:
        case SQLT_INTERVAL_YM:
        case SQLT_INTERVAL_DS:
        case SQLT_TIMESTAMP_LTZ:
            fields[i].size = 76;
            break;
        case SQLT_LNG:
            fields[i].size = 4096 * 4 + 4;
            break;
        case SQLT_LBI:
            fields[i].size = 4096 * 2 + 4;
            break;
        case SQLT_BLOB:
        case SQLT_CLOB:
        default:
            /* TODO */
            break;
        }

        if (fields[i].type == SQLT_BLOB || fields[i].type == SQLT_CLOB) {
            dbd->err = OCIDescriptorAlloc(env, &fields[i].value,
                                          OCI_DTYPE_LOB, 0, NULL);
            if (dbd->err == OCI_ERROR) {
                goto failed;
            }

            len = -1;
            ftype = fields[i].type;
            ptr = &fields[i].len;

        } else {
            fields[i].value = ngx_palloc(dbd->pool, fields[i].size);
            if (fields[i].value == NULL) {
                goto failed;
            }

            len = fields[i].size;

            if (fields[i].type == SQLT_LNG) {
                ftype = SQLT_LVC;
                ptr = NULL;

            } else if (fields[i].type == SQLT_LBI) {
                ftype = SQLT_LVB;
                ptr = NULL;

            } else {
                ftype = SQLT_STR;
                ptr = &fields[i].len;
            }
        }

        dbd->err = OCIDefineByPos(stmt, &fields[i].def, dbd->error, i + 1,
                                  fields[i].value, len, ftype, &fields[i].ind,
                                  ptr, 0, OCI_DEFAULT);
        if (dbd->err == OCI_ERROR) {
            goto failed;
        }
    }

    prep->fields = fields;

done:

    return prep;

failed:

    OCIHandleFree(stmt, OCI_HTYPE_STMT);

    return NULL;
}


static ngx_int_t
ngx_dbd_oracle_pexec(ngx_dbd_prep_t *prep, void *argv, ngx_uint_t argc,
    int *affected)
{
    ngx_dbd_t  *dbd;

    dbd = prep->dbd;

    if (argc > 0) {
        if (ngx_dbd_oracle_bind(prep, argv, argc) != NGX_OK) {
            return NGX_ERROR;
        }
    }

    dbd->err = OCIStmtExecute(dbd->svc_ctx, prep->stmt, dbd->error, 1, 0,
                              NULL, NULL, OCI_DEFAULT);
    if (dbd->err == OCI_ERROR) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "OCIStmtExecute() failed (%s)",
                      ngx_dbd_oracle_strerror(dbd));
        return NGX_ERROR;
    }

    dbd->err = OCIAttrGet(prep->stmt, OCI_HTYPE_STMT, affected, 0,
                          OCI_ATTR_ROW_COUNT, dbd->error);
    if (dbd->err == OCI_ERROR) {
        return NGX_ERROR;
    }

    return NGX_OK;
}


static ngx_dbd_res_t *
ngx_dbd_oracle_pquery(ngx_dbd_prep_t *prep, void *argv, ngx_uint_t argc,
    ngx_uint_t random)
{
    ngx_dbd_t      *dbd;
    ngx_dbd_res_t  *res;

    dbd = prep->dbd;

    res = dbd->res;
    if (res) {
        ngx_dbd_oracle_free_result(res);
    }

    if (argc > 0) {
        if (ngx_dbd_oracle_bind(prep, argv, argc) != NGX_OK) {
            return NULL;
        }
    }

    dbd->err = OCIStmtExecute(dbd->svc_ctx, prep->stmt, dbd->error, 1, 0,
                              NULL, NULL, OCI_DEFAULT);
    if (dbd->err == OCI_ERROR) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "OCIStmtExecute() failed (%s)",
                      ngx_dbd_oracle_strerror(dbd));
        return NULL;
    }

    if (res == NULL) {
        res = ngx_palloc(dbd->pool, sizeof(ngx_dbd_res_t));
        if (res == NULL) {
            return NULL;
        }

        res->dbd = dbd;
        res->row = NULL;

        dbd->res = res;
    }

    res->stmt = prep->stmt;
    res->prep = prep;
    res->random = random;

    return res;
}


static ngx_int_t
ngx_dbd_oracle_next_res(ngx_dbd_res_t *res)
{
    /* TODO */
    return NGX_DECLINED;
}


static ngx_int_t
ngx_dbd_oracle_num_fields(ngx_dbd_res_t *res)
{
    ngx_int_t   n;
    ngx_dbd_t  *dbd;

    dbd = res->dbd;

    dbd->err = OCIAttrGet(res->stmt, OCI_HTYPE_STMT, &n, 0,
                          OCI_ATTR_PARAM_COUNT, dbd->error);
    if (dbd->err == OCI_ERROR) {
        return -1;
    }

    return n;
}


static ngx_int_t
ngx_dbd_oracle_num_rows(ngx_dbd_res_t *res)
{
    ngx_int_t   n;
    ngx_dbd_t  *dbd;

    if (res->random == 0) {
        return -1;
    }

    dbd = res->dbd;

    dbd->err = OCIAttrGet(res->stmt, OCI_HTYPE_STMT, &n, 0,
                          OCI_ATTR_ROW_COUNT, dbd->error);
    if (dbd->err == OCI_ERROR) {
        return -1;
    }

    return n;
}


static ngx_str_t *
ngx_dbd_oracle_field_name(ngx_dbd_res_t *res, int col)
{
    if (col < 0 || col > ngx_dbd_oracle_num_fields(res)) {
        return NULL;
    }

    return &res->prep->fields[col].name;
}


static ngx_dbd_row_t *
ngx_dbd_oracle_fetch_row(ngx_dbd_res_t *res, int row)
{
    int             fm, errcode;
    ngx_dbd_t      *dbd;
    ngx_dbd_row_t  *dbd_row;

    dbd = res->dbd;
    dbd_row = res->row;

    if (dbd_row == NULL) {
        dbd_row = ngx_palloc(dbd->pool, sizeof(ngx_dbd_row_t));
        if (dbd_row == NULL) {
            return NULL;
        }

        dbd_row->res = res;
        res->row = dbd_row;
    }

    if (res->random && row >= 0) {
        fm = OCI_FETCH_ABSOLUTE;

    } else {
        fm = OCI_FETCH_NEXT;
        row = 0;
    }

    dbd->err = OCIStmtFetch2(res->stmt, dbd->error, 1, fm, row, OCI_DEFAULT);
    if (dbd->err == OCI_ERROR) {
        dbd->err = OCIErrorGet(dbd->error, 1, NULL, &errcode, NULL, 0,
                               OCI_HTYPE_ERROR);
        if (dbd->err == OCI_ERROR) {
            return NULL;
        }

        if (errcode != 1002) {
            ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                          "OCIStmtFetch2() failed (%s)",
                          ngx_dbd_oracle_strerror(dbd));
        }

        return NULL;
    }

    return dbd_row;
}


static ngx_str_t *
ngx_dbd_oracle_fetch_field(ngx_dbd_row_t *row, int col)
{
    ngx_dbd_res_t  *res;

    res = row->res;

    if (col < 0 || col > ngx_dbd_oracle_num_fields(res)) {
        return NULL;
    }

    row->field.len = res->prep->fields[col].len;
    row->field.data = res->prep->fields[col].value;

    return &row->field;
}


static ngx_int_t
ngx_dbd_oracle_get_field(ngx_dbd_row_t *row, int col, ngx_dbd_data_type_e type,
    void *data)
{
    return NGX_DECLINED;
}


static u_char *
ngx_dbd_oracle_escape(ngx_dbd_t *dbd, u_char *str)
{
    return str;
}


static int
ngx_dbd_oracle_errno(ngx_dbd_t *dbd)
{
    int  errcode;

    dbd->err = OCIErrorGet(dbd->error, 1, NULL, &errcode, NULL, 0,
                           OCI_HTYPE_ERROR);
    if (dbd->err == OCI_ERROR) {
        return OCI_ERROR;
    }

    return errcode;
}


static u_char *
ngx_dbd_oracle_strerror(ngx_dbd_t *dbd)
{
    int  errcode;

    dbd->err = OCIErrorGet(dbd->error, 1, NULL, &errcode,
                           dbd->errbuf, sizeof(dbd->errbuf),
                           OCI_HTYPE_ERROR);
    if (dbd->err == OCI_ERROR) {
        return NULL;
    }

    return dbd->errbuf;
}


static ngx_int_t
ngx_dbd_oracle_bind(ngx_dbd_prep_t *prep, void *argv, ngx_uint_t argc)
{
    int               type;
    u_char           *data;
    size_t            len;
    ngx_uint_t        i;
    ngx_dbd_t        *dbd;
    ngx_dbd_param_t  *params;

    dbd = prep->dbd;
    params = argv;

    if (prep->params == NULL) {
        prep->params = ngx_pcalloc(dbd->pool, sizeof(ora_param_t) * argc);
        if (prep->params == NULL) {
            return NGX_ERROR;
        }
    }

    for (i = 0; i < argc; i++) {

        switch (params[i].type) {

        case NGX_DBD_DATA_TYPE_TINY:
        case NGX_DBD_DATA_TYPE_SHORT:
        case NGX_DBD_DATA_TYPE_INT:
        case NGX_DBD_DATA_TYPE_LONG:
        case NGX_DBD_DATA_TYPE_LONGLONG:
            type = SQLT_INT;
            break;
        case NGX_DBD_DATA_TYPE_UTINY:
        case NGX_DBD_DATA_TYPE_USHORT:
        case NGX_DBD_DATA_TYPE_UINT:
        case NGX_DBD_DATA_TYPE_ULONG:
        case NGX_DBD_DATA_TYPE_ULONGLONG:
            type = SQLT_UIN;
            break;
        case NGX_DBD_DATA_TYPE_FLOAT:
        case NGX_DBD_DATA_TYPE_DOUBLE:
            type = SQLT_FLT;
            break;
        case NGX_DBD_DATA_TYPE_STRING:
        case NGX_DBD_DATA_TYPE_TEXT:
        case NGX_DBD_DATA_TYPE_TIME:
        case NGX_DBD_DATA_TYPE_DATE:
        case NGX_DBD_DATA_TYPE_DATETIME:
        case NGX_DBD_DATA_TYPE_TIMESTAMP:
        case NGX_DBD_DATA_TYPE_ZTIMESTAMP:
            type = SQLT_STR;
            break;
        case NGX_DBD_DATA_TYPE_BLOB:
            type = SQLT_LBI;
            break;
        case NGX_DBD_DATA_TYPE_CLOB:
            type = SQLT_LNG;
            break;
        case NGX_DBD_DATA_TYPE_CURSOR:
            type = SQLT_CUR;
            break;
        case NGX_DBD_DATA_TYPE_NULL:
            type = SQLT_STR;
            break;
        default:
            /* TODO */
            return NGX_ERROR;
        }

        data = params[i].value;

        if (data == NULL) {
            len = 0;
            type = SQLT_STR;

        } else {
            len = params[i].size;
        }

#if 1
        data = ngx_pcalloc(dbd->pool, 256);
        len = 256;
#endif

#if 0
        if (params[i].direction == NGX_DBD_PARAM_DIRECTION_IN
            || params[i].type == NGX_DBD_DATA_TYPE_CURSOR)
        {
            /* OCIBindByPos */

        } else {
            /* OCIDefineByPos */
        }
#endif

        dbd->err = OCIBindByPos(prep->stmt, &prep->params[i].bind, dbd->error,
                                i + 1, data, len, type, &prep->params[i].ind,
                                NULL, NULL, 0, NULL, OCI_DEFAULT);
        if (dbd->err == OCI_ERROR) {
            ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                          "OCIBindByPos() failed (%s)",
                          ngx_dbd_oracle_strerror(dbd));
            return NGX_ERROR;
        }
    }

    return NGX_OK;
}


static void
ngx_dbd_oracle_free_result(ngx_dbd_res_t *res)
{
}


#endif
