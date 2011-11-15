
/*
 * Copyright (C) Ngwsx
 */


#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_dbd.h>


#if (NGX_DBD_MYSQL)


#include <mysql/mysql.h>


static ngx_int_t ngx_dbd_mysql_process_init(ngx_cycle_t *cycle);
static void ngx_dbd_mysql_process_exit(ngx_cycle_t *cycle);
static ngx_int_t ngx_dbd_mysql_thread_init(ngx_cycle_t *cycle);
static void ngx_dbd_mysql_thread_exit(ngx_cycle_t *cycle);

static ngx_dbd_t *ngx_dbd_mysql_open(ngx_pool_t *pool, ngx_log_t *log,
    ngx_str_t *conn_str, u_char *errstr);
static ngx_int_t ngx_dbd_mysql_close(ngx_dbd_t *dbd);
static void *ngx_dbd_mysql_native_handle(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_mysql_check_conn(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_mysql_select_db(ngx_dbd_t *dbd, u_char *dbname);
static ngx_dbd_tran_t *ngx_dbd_mysql_start_tran(ngx_dbd_t *dbd);
static ngx_int_t ngx_dbd_mysql_end_tran(ngx_dbd_tran_t *tran);
static ngx_uint_t ngx_dbd_mysql_get_tran_mode(ngx_dbd_tran_t *tran);
static ngx_uint_t ngx_dbd_mysql_set_tran_mode(ngx_dbd_tran_t *tran,
    ngx_uint_t mode);
static ngx_int_t ngx_dbd_mysql_exec(ngx_dbd_t *dbd, ngx_str_t *sql,
    int *affected);
static ngx_dbd_res_t *ngx_dbd_mysql_query(ngx_dbd_t *dbd, ngx_str_t *sql,
    ngx_uint_t random);
static ngx_dbd_prep_t *ngx_dbd_mysql_prepare(ngx_dbd_t *dbd, ngx_str_t *sql,
    ngx_uint_t type);
static ngx_int_t ngx_dbd_mysql_pexec(ngx_dbd_prep_t *prep, void *argv,
    ngx_uint_t argc, int *affected);
static ngx_dbd_res_t *ngx_dbd_mysql_pquery(ngx_dbd_prep_t *prep, void *argv,
    ngx_uint_t argc, ngx_uint_t random);
static ngx_int_t ngx_dbd_mysql_next_res(ngx_dbd_res_t *res);
static ngx_int_t ngx_dbd_mysql_num_fields(ngx_dbd_res_t *res);
static ngx_int_t ngx_dbd_mysql_num_rows(ngx_dbd_res_t *res);
static ngx_str_t *ngx_dbd_mysql_field_name(ngx_dbd_res_t *res, int col);
static ngx_dbd_row_t *ngx_dbd_mysql_fetch_row(ngx_dbd_res_t *res, int row);
static ngx_str_t *ngx_dbd_mysql_fetch_field(ngx_dbd_row_t *row, int col);
static ngx_int_t ngx_dbd_mysql_get_field(ngx_dbd_row_t *row, int col,
    ngx_dbd_data_type_e type, void *data);
static u_char *ngx_dbd_mysql_escape(ngx_dbd_t *dbd, u_char *str);
static int ngx_dbd_mysql_errno(ngx_dbd_t *dbd);
static u_char *ngx_dbd_mysql_strerror(ngx_dbd_t *dbd);

static ngx_int_t ngx_dbd_mysql_bind(ngx_dbd_prep_t *prep, void *argv,
    ngx_uint_t argc);
static void ngx_dbd_mysql_free_result(ngx_dbd_res_t *res);


static ngx_str_t  ngx_dbd_mysql_name = ngx_string("mysql");


static ngx_dbd_driver_t  ngx_dbd_mysql_driver = {
    &ngx_dbd_mysql_name,
    ngx_dbd_mysql_open,
    ngx_dbd_mysql_close,
    ngx_dbd_mysql_native_handle,
    ngx_dbd_mysql_check_conn,
    ngx_dbd_mysql_select_db,
    ngx_dbd_mysql_start_tran,
    ngx_dbd_mysql_end_tran,
    ngx_dbd_mysql_get_tran_mode,
    ngx_dbd_mysql_set_tran_mode,
    ngx_dbd_mysql_exec,
    ngx_dbd_mysql_query,
    ngx_dbd_mysql_prepare,
    ngx_dbd_mysql_pexec,
    ngx_dbd_mysql_pquery,
    ngx_dbd_mysql_next_res,
    ngx_dbd_mysql_num_fields,
    ngx_dbd_mysql_num_rows,
    ngx_dbd_mysql_field_name,
    ngx_dbd_mysql_fetch_row,
    ngx_dbd_mysql_fetch_field,
    ngx_dbd_mysql_get_field,
    ngx_dbd_mysql_escape,
    ngx_dbd_mysql_errno,
    ngx_dbd_mysql_strerror
};


static ngx_dbd_module_t  ngx_dbd_mysql_module_ctx = {
    &ngx_dbd_mysql_driver,                 /* driver */

    NULL,                                  /* create main configuration */
    NULL,                                  /* init main configuration */

    NULL,                                  /* create connection configuration */
    NULL,                                  /* merge connection configuration */

    NULL,                                  /* create command configuration */
    NULL,                                  /* merge command configuration */

    NULL,                                  /* create parameter configuration */
    NULL,                                  /* merge parameter configuration */
};


ngx_module_t  ngx_dbd_mysql_module = {
    NGX_MODULE_V1,
    &ngx_dbd_mysql_module_ctx,             /* module context */
    NULL,                                  /* module directives */
    NGX_DBD_MODULE,                        /* module type */
    NULL,                                  /* init master */
    NULL,                                  /* init module */
    ngx_dbd_mysql_process_init,            /* init process */
    ngx_dbd_mysql_thread_init,             /* init thread */
    ngx_dbd_mysql_thread_exit,             /* exit thread */
    ngx_dbd_mysql_process_exit,            /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};


/* default maximum field size 1MB */

#define NGX_DBD_MYSQL_FIELDSIZE  1048575


struct ngx_dbd_s {
    MYSQL                *mysql;

    ngx_log_t            *log;
    ngx_pool_t           *pool;

    ngx_str_t             escaped;
    ngx_uint_t            fldsz;

    ngx_array_t           trans;
    ngx_list_t            preps;

    ngx_dbd_res_t        *res;
};


struct ngx_dbd_tran_s {
    ngx_uint_t            mode;
    int                   errnum;
    ngx_dbd_t            *dbd;
};


struct ngx_dbd_prep_s {
    MYSQL_STMT           *stmt;
    MYSQL_BIND           *binds;

    ngx_dbd_t            *dbd;
};


struct ngx_dbd_res_s {
    MYSQL_RES            *res;
    MYSQL_RES            *metadata;
    MYSQL_STMT           *stmt;
    MYSQL_BIND           *binds;

    ngx_uint_t            random;
    ngx_str_t             field_name;

    ngx_dbd_t            *dbd;
    ngx_dbd_row_t        *row;
};


struct ngx_dbd_row_s {
    MYSQL_ROW             row;
    ngx_uint_t           *lengths;
    ngx_str_t             field;
    ngx_dbd_res_t        *res;
};


static ngx_int_t
ngx_dbd_mysql_process_init(ngx_cycle_t *cycle)
{
    if (mysql_library_init(0, NULL, NULL) != 0) {
        ngx_log_error(NGX_LOG_ALERT, cycle->log, 0,
                      "mysql_library_init() failed");
        return NGX_ERROR;
    }

    my_init();

    mysql_thread_init();

    ngx_log_debug1(NGX_LOG_DEBUG_MYSQL, cycle->log, 0,
                   "mysql_get_client_info: %s", mysql_get_client_info());

    ngx_log_debug1(NGX_LOG_DEBUG_MYSQL, cycle->log, 0,
                   "mysql_get_client_version: %ul", mysql_get_client_version());

    ngx_log_debug1(NGX_LOG_DEBUG_MYSQL, cycle->log, 0,
                   "mysql_thread_safe: %ud", mysql_thread_safe());

    return NGX_OK;
}


static void
ngx_dbd_mysql_process_exit(ngx_cycle_t *cycle)
{
    mysql_thread_end();

    mysql_library_end();
}


static ngx_int_t
ngx_dbd_mysql_thread_init(ngx_cycle_t *cycle)
{
    my_init();

    mysql_thread_init();

    return NGX_OK;
}


static void
ngx_dbd_mysql_thread_exit(ngx_cycle_t *cycle)
{
    mysql_thread_end();
}


static ngx_dbd_t *
ngx_dbd_mysql_open(ngx_pool_t *pool, ngx_log_t *log, ngx_str_t *conn_str,
    u_char *errstr)
{
    MYSQL            *mysql;
    size_t            klen, vlen;
    u_char           *params, *last, *ptr, *key, *value;
    u_char            host[64], user[64], passwd[64], db[64];
    u_char            group[64], sock[64];
    ngx_dbd_t        *dbd;
    ngx_uint_t        i, port, flags, fldsz;
#if (MYSQL_VERSION_ID >= 50013)
    ngx_uint_t        reconnect;
#endif
#if (NGX_DEBUG)
    MY_CHARSET_INFO   csi;
#endif
    ngx_keyval_t      fields[] = {
        { ngx_string("host"), ngx_null_string },
        { ngx_string("user"), ngx_null_string },
        { ngx_string("passwd"), ngx_null_string },
        { ngx_string("db"), ngx_null_string },
        { ngx_string("port"), ngx_null_string },
        { ngx_string("sock"), ngx_null_string },
        { ngx_string("flags"), ngx_null_string },
        { ngx_string("fldsz"), ngx_null_string },
        { ngx_string("group"), ngx_null_string },
#if (MYSQL_VERSION_ID >= 50013)
        { ngx_string("reconnect"), ngx_null_string },
#endif
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

    if (fields[0].value.data) {
        ngx_cpystrn(host, fields[0].value.data, fields[0].value.len + 1);

    } else {
        ngx_cpystrn(host, "127.0.0.1", sizeof("127.0.0.1"));
    }

    if (fields[1].value.data) {
        ngx_cpystrn(user, fields[1].value.data, fields[1].value.len + 1);

    } else {
        user[0] = '\0';
    }

    if (fields[2].value.data) {
        ngx_cpystrn(passwd, fields[2].value.data, fields[2].value.len + 1);

    } else {
        passwd[0] = '\0';
    }

    if (fields[3].value.data) {
        ngx_cpystrn(db, fields[3].value.data, fields[3].value.len + 1);

    } else {
        db[0] = '\0';
    }

    if (fields[4].value.data) {
        port = ngx_atoi(fields[4].value.data, fields[4].value.len);

    } else {
        port = 3306;
    }

    if (fields[5].value.data) {
        ngx_cpystrn(sock, fields[5].value.data, fields[5].value.len + 1);

    } else {
        sock[0] = '\0';
    }

    if (fields[6].value.len == sizeof("CLIENT_FOUND_ROWS") - 1
        && ngx_strncmp(fields[6].value.data, "CLIENT_FOUND_ROWS",
                       fields[6].value.len))
    {
        flags |= CLIENT_FOUND_ROWS;

    } else {
        flags = 0;
    }

    if (fields[7].value.data) {
        fldsz = ngx_atoi(fields[7].value.data, fields[7].value.len);

    } else {
        fldsz = NGX_DBD_MYSQL_FIELDSIZE;
    }

    if (fields[8].value.data) {
        ngx_cpystrn(group, fields[8].value.data, fields[8].value.len + 1);

    } else {
        group[0] = '\0';
    }

#if (MYSQL_VERSION_ID >= 50013)
    if (fields[9].value.data) {
        reconnect = ngx_atoi(fields[9].value.data, fields[9].value.len) ? 1 : 0;

    } else {
        reconnect = 1;
    }
#endif

    mysql = mysql_init(NULL);
    if (mysql == NULL) {
        ngx_log_error(NGX_LOG_ALERT, log, 0, "mysql_init() failed");
        return NULL;
    }

    if (group[0] != '\0') {
        if (mysql_options(mysql, MYSQL_READ_DEFAULT_GROUP, group) != 0) {
            ngx_log_error(NGX_LOG_ALERT, log, 0,
                          "mysql_options(MYSQL_READ_DEFAULT_GROUP, "
                          "\"%s\") failed (%d: %s)",
                          group, mysql_errno(mysql), mysql_error(mysql));
        }
    }

#if (MYSQL_VERSION_ID >= 50013)
    if (mysql_options(mysql, MYSQL_OPT_RECONNECT, (const char *) &reconnect)
        != 0)
    {
        ngx_log_error(NGX_LOG_ALERT, log, 0,
                      "mysql_options(MYSQL_OPT_RECONNECT, %ui) failed (%d: %s)",
                      reconnect, mysql_errno(mysql), mysql_error(mysql));
    }
#endif

    if (mysql_real_connect(mysql, host, user,
                           (passwd[0] != '\0' ? passwd : NULL),
                           (db[0] != '\0' ? db : NULL),
                           (unsigned int) port,
                           (sock[0] != '\0' ? sock : NULL),
                           (unsigned long) flags)
        == NULL)
    {
        ngx_log_error(NGX_LOG_ALERT, log, 0,
                      "mysql_real_connect(\"%V\") failed (%d: %s)",
                      conn_str, mysql_errno(mysql), mysql_error(mysql));

        if (errstr != NULL) {
            ngx_sprintf(errstr,
                        "mysql_real_connect(\"%V\") failed (%d: %s)",
                        conn_str, mysql_errno(mysql), mysql_error(mysql));
        }

        goto failed;
    }

#if (MYSQL_VERSION_ID >= 50013)
    if (mysql_options(mysql, MYSQL_OPT_RECONNECT, (const char *) &reconnect)
        != 0)
    {
        ngx_log_error(NGX_LOG_ALERT, log, 0,
                      "mysql_options(MYSQL_OPT_RECONNECT, %ui) failed (%d: %s)",
                      reconnect, mysql_errno(mysql), mysql_error(mysql));
    }
#endif

    ngx_log_debug1(NGX_LOG_DEBUG_MYSQL, log, 0,
                   "mysql_character_set_name: %s",
                   mysql_character_set_name(mysql));

#if 0
    if (mysql_set_character_set(mysql, "gb2312") != 0) {
        ngx_log_error(NGX_LOG_ALERT, log, 0,
                      "mysql_set_character_set(\"gb2312\") failed (%d: %s)",
                      mysql_errno(mysql), mysql_error(mysql));

    }
#endif

#if (NGX_DEBUG)
    mysql_get_character_set_info(mysql, &csi);

    ngx_log_debug1(NGX_LOG_DEBUG_MYSQL, log, 0,
                   "character set number: %ud", csi.number);
    ngx_log_debug1(NGX_LOG_DEBUG_MYSQL, log, 0,
                   "character set state: %ud", csi.state);
    ngx_log_debug1(NGX_LOG_DEBUG_MYSQL, log, 0,
                   "character set name: %s", csi.name);
    ngx_log_debug1(NGX_LOG_DEBUG_MYSQL, log, 0,
                   "collation name: %s", csi.csname);
    /*ngx_log_debug1(NGX_LOG_DEBUG_MYSQL, log, 0,
                   "comment: %s", csi.comment ? csi.comment : "NULL");*/
    ngx_log_debug1(NGX_LOG_DEBUG_MYSQL, log, 0,
                   "character set directory: %s", csi.dir ? csi.dir : "NULL");
    ngx_log_debug1(NGX_LOG_DEBUG_MYSQL, log, 0,
                   "multibyte strings min length: %ud", csi.mbminlen);
    ngx_log_debug1(NGX_LOG_DEBUG_MYSQL, log, 0,
                   "multibyte strings max length: %ud", csi.mbmaxlen);
#endif

    ngx_log_debug1(NGX_LOG_DEBUG_MYSQL, log, 0,
                   "mysql_get_host_info: %s", mysql_get_host_info(mysql));

    ngx_log_debug1(NGX_LOG_DEBUG_MYSQL, log, 0,
                   "mysql_get_proto_info: %ud", mysql_get_proto_info(mysql));

    ngx_log_debug1(NGX_LOG_DEBUG_MYSQL, log, 0,
                   "mysql_get_server_info: %s", mysql_get_server_info(mysql));

    ngx_log_debug1(NGX_LOG_DEBUG_MYSQL, log, 0,
                   "mysql_get_server_version: %ul",
                   mysql_get_server_version(mysql));

    ngx_log_debug1(NGX_LOG_DEBUG_MYSQL, log, 0,
                   "mysql_stat: %s", mysql_stat(mysql));

    dbd = ngx_palloc(pool, sizeof(ngx_dbd_t));
    if (dbd == NULL) {
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

    dbd->mysql = mysql;
    dbd->log = log;
    dbd->pool = pool;
    dbd->escaped.data = NULL;
    dbd->escaped.len = 0;
    dbd->fldsz = fldsz;
    dbd->res = NULL;

    return dbd;

failed:

    mysql_close(mysql);

    return NULL;
}


static ngx_int_t
ngx_dbd_mysql_close(ngx_dbd_t *dbd)
{
    ngx_uint_t         i;
#if 0
    ngx_dbd_tran_t   **trans;
#endif
    ngx_dbd_prep_t   **preps;
    ngx_list_part_t   *part;

    if (dbd->res) {
        ngx_dbd_mysql_free_result(dbd->res);
    }

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

        mysql_stmt_close(preps[i]->stmt);
    }

    /* close connection */

    mysql_close(dbd->mysql);

    return NGX_OK;
}


static void *
ngx_dbd_mysql_native_handle(ngx_dbd_t *dbd)
{
    return dbd->mysql;
}


static ngx_int_t
ngx_dbd_mysql_check_conn(ngx_dbd_t *dbd)
{
    if (mysql_ping(dbd->mysql) != 0) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "mysql_ping() failed (%d: %s)",
                      mysql_errno(dbd->mysql), mysql_error(dbd->mysql));
        return NGX_ERROR;
    }

    return NGX_OK;
}


static ngx_int_t
ngx_dbd_mysql_select_db(ngx_dbd_t *dbd, u_char *dbname)
{
    if (mysql_select_db(dbd->mysql, (const char *) dbname) != 0) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "mysql_select_db(\"%s\") failed (%d: %s)",
                      dbname, mysql_errno(dbd->mysql), mysql_error(dbd->mysql));
        return NGX_ERROR;
    }

    return NGX_OK;
}


static ngx_dbd_tran_t *
ngx_dbd_mysql_start_tran(ngx_dbd_t *dbd)
{
#if 0
    ngx_dbd_tran_t  *tran;

    tran = dbd->tran;
    if (tran && tran->dbd) {
        return tran;
    }

    if (dbd->res) {
        ngx_dbd_mysql_free_result(dbd->res);
    }

    /* disable auto-commit */

    if (mysql_autocommit(dbd->mysql, 0) != 0) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "mysql_autocommit(0) failed (%d: %s)",
                      mysql_errno(dbd->mysql), mysql_error(dbd->mysql));
        return NULL;
    }

    ngx_log_debug1(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "mysql_autocommit(%p, 0)", dbd->mysql);

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
#endif
    return NULL;
}


static ngx_int_t
ngx_dbd_mysql_end_tran(ngx_dbd_tran_t *tran)
{
#if 0
    int         rc;
    u_char     *name;
    ngx_dbd_t  *dbd;

    dbd = tran->dbd;
    if (dbd == NULL) {
        return NGX_OK;
    }

    if (dbd->res) {
        ngx_dbd_mysql_free_result(dbd->res);
    }

    /* commit or rollback transaction */

    if (NGX_DBD_TRANS_DO_COMMIT(tran)) {
        name = (u_char *) "mysql_commit";

        rc = mysql_commit(dbd->mysql);

    } else {
        name = (u_char *) "mysql_rollback";

        rc = mysql_rollback(dbd->mysql);
    }

    if (rc != 0) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0, "%s(%p) failed (%ud: %s)",
                      name, dbd->mysql,
                      mysql_errno(dbd->mysql), mysql_error(dbd->mysql));
        return NGX_ERROR;
    }

    ngx_log_debug2(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "%s(%p)", name, dbd->mysql);

    /* enable auto-commit */

    if (mysql_autocommit(dbd->mysql, 1) != 0) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "mysql_autocommit(%p, 1) failed (%ud: %s)",
                      dbd->mysql, mysql_errno(dbd->mysql),
                      mysql_error(dbd->mysql));
        return NGX_ERROR;
    }

    ngx_log_debug1(NGX_LOG_DEBUG_MYSQL, dbd->log, 0,
                   "mysql_autocommit(%p, 1)", dbd->mysql);

    tran->dbd = NULL;
#endif
    return NGX_OK;
}


static ngx_uint_t
ngx_dbd_mysql_get_tran_mode(ngx_dbd_tran_t *tran)
{
    if (tran == NULL) {
        return NGX_DBD_TRANS_MODE_COMMIT;
    }

    return tran->mode;
}


static ngx_uint_t
ngx_dbd_mysql_set_tran_mode(ngx_dbd_tran_t *tran, ngx_uint_t mode)
{
    if (tran == NULL) {
        return NGX_DBD_TRANS_MODE_COMMIT;
    }

    tran->mode = (mode & NGX_DBD_TRANS_MODE_BITS);

    return tran->mode;
}


static ngx_int_t
ngx_dbd_mysql_exec(ngx_dbd_t *dbd, ngx_str_t *sql, int *affected)
{
    int         rv, changes;
    ngx_int_t   rc;
    MYSQL_RES  *res;

    if (dbd->res) {
        ngx_dbd_mysql_free_result(dbd->res);
    }

    if (mysql_real_query(dbd->mysql, (const char *) sql->data,
                         (unsigned long) sql->len)
        != 0)
    {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "mysql_real_query(\"%V\") failed (%d: %s)",
                      sql, mysql_errno(dbd->mysql), mysql_error(dbd->mysql));
        return NGX_ERROR;
    }

    rc = NGX_OK;
    changes = 0;

    do {
        /* support execution of multi-statements */

        res = mysql_use_result(dbd->mysql);
        if (res) {
            while (mysql_fetch_row(res)) {}

            mysql_free_result(res);

        } else {
            changes += (int) mysql_affected_rows(dbd->mysql);
        }

        rv = mysql_next_result(dbd->mysql);
        if (rv > 0) {
            ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                          "mysql_next_result() failed (%d: %s)",
                          mysql_errno(dbd->mysql), mysql_error(dbd->mysql));
            rc = NGX_ERROR;
            break;

        } else if (rv == -1) {
            break;
        }

    } while (rv == 0);

    if (affected) {
        *affected = changes;
    }

    return rc;
}


static ngx_dbd_res_t *
ngx_dbd_mysql_query(ngx_dbd_t *dbd, ngx_str_t *sql, ngx_uint_t random)
{
    u_char         *func;
    MYSQL_RES      *myres;
    ngx_dbd_res_t  *res;

    res = dbd->res;
    if (res) {
        ngx_dbd_mysql_free_result(res);
    }

    if (mysql_real_query(dbd->mysql, (const char *) sql->data,
                         (unsigned long) sql->len)
        != 0)
    {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "mysql_real_query(\"%V\") failed (%d: %s)",
                      sql, mysql_errno(dbd->mysql), mysql_error(dbd->mysql));
        return NULL;
    }

    if (random) {
        func = (u_char *) "mysql_store_result";

        myres = mysql_store_result(dbd->mysql);

    } else {
        func = (u_char *) "mysql_use_result";

        myres = mysql_use_result(dbd->mysql);
    }

    if (myres == NULL) {
        if (mysql_field_count(dbd->mysql) == 0) {
            return NULL;
        }

        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0, "%s() failed (%d: %s)",
                      func, mysql_errno(dbd->mysql), mysql_error(dbd->mysql));
        return NULL;
    }

    if (res == NULL) {
        res = ngx_palloc(dbd->pool, sizeof(ngx_dbd_res_t));
        if (res == NULL) {
            if (random == 0) {
                while (mysql_fetch_row(myres)) {}
            }

            mysql_free_result(myres);

            return NULL;
        }

        res->dbd = dbd;
        res->row = NULL;
        dbd->res = res;
    }

    res->res = myres;
    res->metadata = NULL;
    res->stmt = NULL;
    res->binds = NULL;
    res->random = random;
    res->field_name.data = NULL;
    res->field_name.len = 0;

    return res;
}


static ngx_dbd_prep_t *
ngx_dbd_mysql_prepare(ngx_dbd_t *dbd, ngx_str_t *sql, ngx_uint_t type)
{
    MYSQL_STMT      *stmt;
    ngx_dbd_prep_t  *prep, **pprep;

    stmt = mysql_stmt_init(dbd->mysql);
    if (stmt == NULL) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "mysql_stmt_init() failed (%d: %s)",
                      mysql_errno(dbd->mysql), mysql_error(dbd->mysql));
        return NULL;
    }

    if (mysql_stmt_prepare(stmt, sql->data, (unsigned long) sql->len) != 0) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "mysql_stmt_prepare(\"%V\") failed (%d: %s)",
                      sql, mysql_stmt_errno(stmt), mysql_stmt_error(stmt));
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
    prep->binds = NULL;
    prep->dbd = dbd;

    return prep;

failed:

    mysql_stmt_close(stmt);

    return NULL;
}


static ngx_int_t
ngx_dbd_mysql_pexec(ngx_dbd_prep_t *prep, void *argv, ngx_uint_t argc,
    int *affected)
{
    ngx_dbd_t  *dbd;

    dbd = prep->dbd;

    if (argc > 0) {
        if (ngx_dbd_mysql_bind(prep, argv, argc) != NGX_OK) {
            return NGX_ERROR;
        }

        if (mysql_stmt_bind_param(prep->stmt, prep->binds) != 0) {
            ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                          "mysql_stmt_bind_param() failed (%d: %s)",
                          mysql_stmt_errno(prep->stmt),
                          mysql_stmt_error(prep->stmt));
            return NGX_ERROR;
        }
    }

    if (mysql_stmt_execute(prep->stmt) != 0) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "mysql_stmt_execute() failed (%d: %s)",
                      mysql_stmt_errno(prep->stmt),
                      mysql_stmt_error(prep->stmt));
        return NGX_ERROR;
    }

    if (affected) {
        *affected = (int) mysql_stmt_affected_rows(prep->stmt);
    }

    while (mysql_stmt_fetch(prep->stmt) == 0) {}

    mysql_stmt_free_result(prep->stmt);

    return NGX_OK;
}


static ngx_dbd_res_t *
ngx_dbd_mysql_pquery(ngx_dbd_prep_t *prep, void *argv, ngx_uint_t argc,
    ngx_uint_t random)
{
    size_t          size;
#if (MYSQL_VERSION_ID >= 50000)
    my_bool        *errors;
#endif
    my_bool        *is_nulls;
    ngx_dbd_t      *dbd;
    MYSQL_RES      *myres;
    MYSQL_BIND     *binds;
    ngx_uint_t      i, n, *lengths;
    ngx_dbd_res_t  *res;

    dbd = prep->dbd;
    res = dbd->res;

    if (res) {
        ngx_dbd_mysql_free_result(res);

        binds = res->binds;

    } else {
        binds = NULL;
    }

    if (argc > 0) {
        if (ngx_dbd_mysql_bind(prep, argv, argc) != NGX_OK) {
            return NULL;
        }

        if (mysql_stmt_bind_param(prep->stmt, prep->binds) != 0) {
            ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                          "mysql_stmt_bind_param() failed (%d: %s)",
                          mysql_stmt_errno(prep->stmt),
                          mysql_stmt_error(prep->stmt));
            return NULL;
        }
    }

    if (mysql_stmt_execute(prep->stmt) != 0) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "mysql_stmt_execute() failed (%d: %s)",
                      mysql_stmt_errno(prep->stmt),
                      mysql_stmt_error(prep->stmt));
        return NULL;
    }

    if (binds) {
        goto bind_result;
    }

    myres = mysql_stmt_result_metadata(prep->stmt);
    if (myres == NULL) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "mysql_stmt_result_metadata() failed (%d: %s)",
                      mysql_stmt_errno(prep->stmt),
                      mysql_stmt_error(prep->stmt));
        return NULL;
    }

    n = mysql_num_fields(myres);

    binds = ngx_pcalloc(dbd->pool, sizeof(MYSQL_BIND) * n);
    if (binds == NULL) {
        goto failed;
    }

    lengths = ngx_pcalloc(dbd->pool, sizeof(ngx_uint_t) * n);
    if (lengths == NULL) {
        goto failed;
    }

    is_nulls = ngx_pcalloc(dbd->pool, sizeof(my_bool) * n);
    if (is_nulls == NULL) {
        goto failed;
    }

#if (MYSQL_VERSION_ID >= 50000)
    errors = ngx_pcalloc(dbd->pool, sizeof(my_bool) * n);
    if (errors == NULL) {
        goto failed;
    }
#endif

    for (i = 0; i < n; i++) {
        size = myres->fields[i].length < dbd->fldsz
               ? myres->fields[i].length : dbd->fldsz;

        binds[i].buffer_type = myres->fields[i].type == MYSQL_TYPE_BLOB
                               ? MYSQL_TYPE_BLOB : MYSQL_TYPE_VAR_STRING;
        binds[i].buffer = ngx_pcalloc(dbd->pool, size);
        binds[i].buffer_length = (unsigned long) size;
        binds[i].length = (unsigned long *) &lengths[i];
        binds[i].is_null = &is_nulls[i];
#if (MYSQL_VERSION_ID >= 50000)
        binds[i].error = &errors[i];
#endif
    }

bind_result:

    if (mysql_stmt_bind_result(prep->stmt, binds) != 0) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "mysql_stmt_bind_result() failed (%d: %s)",
                      mysql_stmt_errno(prep->stmt),
                      mysql_stmt_error(prep->stmt));
        goto failed;
    }

    if (random) {
        if (mysql_stmt_store_result(prep->stmt) != 0) {
            ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                          "mysql_stmt_store_result() failed (%d: %s)",
                          mysql_stmt_errno(prep->stmt),
                          mysql_stmt_error(prep->stmt));
            goto failed;
        }
    }

    if (res == NULL) {
        res = ngx_palloc(dbd->pool, sizeof(ngx_dbd_res_t));
        if (res == NULL) {
            goto failed;
        }

        res->dbd = dbd;
        res->row = NULL;
        dbd->res = res;
    }

    res->res = NULL;
    res->metadata = myres;
    res->stmt = prep->stmt;
    res->binds = binds;
    res->random = random;
    res->field_name.data = NULL;
    res->field_name.len = 0;

    return res;

failed:

    mysql_free_result(myres);

    while (mysql_stmt_fetch(res->stmt)) {}

    mysql_stmt_free_result(res->stmt);

    return NULL;
}


static ngx_int_t
ngx_dbd_mysql_next_res(ngx_dbd_res_t *res)
{
    /* TODO */
    return NGX_DECLINED;
}


static ngx_int_t
ngx_dbd_mysql_num_fields(ngx_dbd_res_t *res)
{
    if (res->res) {
        return mysql_num_fields(res->res);

    } else if (res->stmt) {
        return mysql_stmt_field_count(res->stmt);
    }

    return -1;
}


static ngx_int_t
ngx_dbd_mysql_num_rows(ngx_dbd_res_t *res)
{
    if (res->random == 0) {
        return -1;
    }

    if (res->res) {
        return (int) mysql_num_rows(res->res);

    } else if (res->stmt) {
        return (int) mysql_stmt_num_rows(res->stmt);
    }

    return -1;
}


static ngx_str_t *
ngx_dbd_mysql_field_name(ngx_dbd_res_t *res, int col)
{
    ngx_dbd_t    *dbd;
    MYSQL_RES    *myres;
    MYSQL_FIELD  *field;

    dbd = res->dbd;

    if (col < 0 || col >= ngx_dbd_mysql_num_fields(res)) {
        return NULL;
    }

    if (res->res) {
        myres = res->res;

    } else if (res->stmt) {
        myres = res->metadata;

    } else {
        return NULL;
    }

    field = mysql_fetch_field_direct(myres, col);
    if (field == NULL) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "mysql_fetch_field_direct() failed (%d: %s)",
                      mysql_errno(dbd->mysql), mysql_error(dbd->mysql));
        return NULL;
    }

    res->field_name.data = field->name;
    res->field_name.len = field->name_length;

    return &res->field_name;
}


static ngx_dbd_row_t *
ngx_dbd_mysql_fetch_row(ngx_dbd_res_t *res, int row)
{
    MYSQL_ROW       myrow;
    ngx_dbd_row_t  *dbd_row;

    dbd_row = res->row;

    if (dbd_row == NULL) {
        dbd_row = ngx_palloc(res->dbd->pool, sizeof(ngx_dbd_row_t));
        if (dbd_row == NULL) {
            return NULL;
        }

        dbd_row->res = res;
        res->row = dbd_row;
    }

    if (res->res) {
        if (res->random && row >= 0) {
            mysql_data_seek(res->res, row);
        }

        myrow = mysql_fetch_row(res->res);
        if (myrow == NULL) {
            return NULL;
        }

        dbd_row->row = myrow;
        dbd_row->lengths = (ngx_uint_t *) mysql_fetch_lengths(res->res);

    } else if (res->stmt) {
        if (res->random && row >= 0) {
            mysql_stmt_data_seek(res->stmt, row);
        }

        if (mysql_stmt_fetch(res->stmt) != 0) {
            return NULL;
        }

        dbd_row->row = NULL;
        dbd_row->lengths = NULL;
    }

    return dbd_row;
}


static ngx_str_t *
ngx_dbd_mysql_fetch_field(ngx_dbd_row_t *row, int col)
{
    ngx_dbd_t      *dbd;
    MYSQL_BIND     *bind;
    ngx_dbd_res_t  *res;

    res = row->res;
    dbd = res->dbd;

    if (col < 0 || col >= ngx_dbd_mysql_num_fields(res)) {
        return NULL;
    }

    if (res->res) {
        row->field.data = (u_char *) row->row[col];
        row->field.len = row->lengths[col];

    } else if (res->stmt) {
        bind = &res->binds[col];

        if (mysql_stmt_fetch_column(res->stmt, bind, col, 0) != 0) {
            ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                          "mysql_stmt_fetch_column() failed (%d: %s)",
                          mysql_stmt_errno(res->stmt),
                          mysql_stmt_error(res->stmt));
            return NULL;
        }

        if (*bind->is_null) {
            row->field.data = NULL;
            row->field.len = 0;

        } else {
            row->field.data = bind->buffer;
            row->field.len = *bind->length;
        }

    } else {
        return NULL;
    }

    return &row->field;
}


static ngx_int_t
ngx_dbd_mysql_get_field(ngx_dbd_row_t *row, int col, ngx_dbd_data_type_e type,
    void *data)
{
    void           *buf;
    size_t          size;
    ngx_uint_t      is_null;
    MYSQL_BIND     *bind;
    ngx_dbd_res_t  *res;

    is_null = 0;
    res = row->res;

    if (res->res == NULL && res->stmt == NULL) {
        return NGX_ERROR;

    } else if (res->res) {
        if (col < 0 || col >= (int) mysql_num_fields(res->res)) {
            return NGX_ERROR;
        }

        buf = row->row[col];
        size = row->lengths[col];

        if (buf == NULL) {
            is_null = 1;
        }

    } else if (res->stmt) {
        if (col < 0 || col >= (int) mysql_stmt_field_count(res->stmt)) {
            return NGX_ERROR;
        }

        bind = &res->binds[col];

        if (mysql_stmt_fetch_column(res->stmt, bind, col, 0) != 0) {
            return NGX_ERROR;
        }

        buf = bind->buffer;
        size = *bind->length;

        if (*bind->is_null) {
            is_null = 1;
        }
    }

    switch (type) {
#if 0
    case NGX_DBD_DATA_TYPE_TINY:
        *(char*)data = atoi(bind->buffer);
        break;
    case NGX_DBD_DATA_TYPE_UTINY:
        *(unsigned char*)data = atoi(bind->buffer);
        break;
    case NGX_DBD_DATA_TYPE_SHORT:
        *(short*)data = atoi(bind->buffer);
        break;
    case NGX_DBD_DATA_TYPE_USHORT:
        *(unsigned short*)data = atoi(bind->buffer);
        break;
    case NGX_DBD_DATA_TYPE_INT:
        *(int*)data = atoi(bind->buffer);
        break;
    case NGX_DBD_DATA_TYPE_UINT:
        *(unsigned int*)data = atoi(bind->buffer);
        break;
    case NGX_DBD_DATA_TYPE_LONG:
        *(long*)data = atol(bind->buffer);
        break;
    case NGX_DBD_DATA_TYPE_ULONG:
        *(unsigned long*)data = atol(bind->buffer);
        break;
    case NGX_DBD_DATA_TYPE_LONGLONG:
        *(apr_int64_t*)data = apr_atoi64(bind->buffer);
        break;
    case NGX_DBD_DATA_TYPE_ULONGLONG:
        *(apr_uint64_t*)data = apr_atoi64(bind->buffer);
        break;
    case NGX_DBD_DATA_TYPE_FLOAT:
        *(float*)data = (float) atof(bind->buffer);
        break;
    case NGX_DBD_DATA_TYPE_DOUBLE:
        *(double*)data = atof(bind->buffer);
        break;
    case NGX_DBD_DATA_TYPE_STRING:
    case NGX_DBD_DATA_TYPE_TEXT:
    case NGX_DBD_DATA_TYPE_TIME:
    case NGX_DBD_DATA_TYPE_DATE:
    case NGX_DBD_DATA_TYPE_DATETIME:
    case NGX_DBD_DATA_TYPE_TIMESTAMP:
    case NGX_DBD_DATA_TYPE_ZTIMESTAMP:
        *((char*)bind->buffer+bind->buffer_length-1) = '\0';
        *(char**)data = bind->buffer;
        break;
    case NGX_DBD_DATA_TYPE_BLOB:
    case NGX_DBD_DATA_TYPE_CLOB:
        {
        apr_bucket *e;
        apr_bucket_brigade *b = (apr_bucket_brigade*)data;

        e = apr_bucket_lob_create(row, n, 0, len,
                                  row->res->pool, b->bucket_alloc);
        APR_BRIGADE_INSERT_TAIL(b, e);
        }
        break;
#endif
    case NGX_DBD_DATA_TYPE_NULL:
        *(void**)data = NULL;
        break;
    default:
        return NGX_ERROR;
    }

    return NGX_OK;
}


static u_char *
ngx_dbd_mysql_escape(ngx_dbd_t *dbd, u_char *str)
{
    size_t  len, max, rlen;

    len = ngx_strlen(str);

    /* TODO: configurable max length of escape buffer */

    max = 1024;
    max = (max < len ? len : max) * 2 + 1;

    if (dbd->escaped.len < max) {
        dbd->escaped.data = ngx_palloc(dbd->pool, max);
        if (dbd->escaped.data == NULL) {
            return NULL;
        }

        dbd->escaped.len = max;
    }

    rlen = mysql_real_escape_string(dbd->mysql, dbd->escaped.data,
                                    str, (unsigned long) len);
    if (rlen < len) {
        ngx_log_error(NGX_LOG_ALERT, dbd->log, 0,
                      "mysql_real_escape_string() failed (%d: %s)",
                      mysql_errno(dbd->mysql), mysql_error(dbd->mysql));
        return NULL;
    }

    return dbd->escaped.data;
}


static int
ngx_dbd_mysql_errno(ngx_dbd_t *dbd)
{
    return mysql_errno(dbd->mysql);
}


static u_char *
ngx_dbd_mysql_strerror(ngx_dbd_t *dbd)
{
    return (u_char *) mysql_error(dbd->mysql);
}


static ngx_int_t
ngx_dbd_mysql_bind(ngx_dbd_prep_t *prep, void *argv, ngx_uint_t argc)
{
    ngx_dbd_t        *dbd;
    ngx_uint_t        i;
    MYSQL_BIND       *binds;
    ngx_dbd_param_t  *params;

    dbd = prep->dbd;
    binds = prep->binds;
    params = argv;

    /* TODO: mysql_stmt_param_count() */

    if (binds == NULL) {
        binds = ngx_pcalloc(dbd->pool, sizeof(MYSQL_BIND) * argc);
        if (binds == NULL) {
            return NGX_ERROR;
        }

        prep->binds = binds;
    }

    for (i = 0; i < argc; i++) {

        binds[i].length = &binds[i].buffer_length;
        binds[i].is_null = NULL;
        binds[i].buffer = params[i].value;

        switch (params[i].type) {

        case NGX_DBD_DATA_TYPE_TINY:
            binds[i].buffer_type = MYSQL_TYPE_TINY;
            binds[i].is_unsigned = 0;
            break;

        case NGX_DBD_DATA_TYPE_UTINY:
            binds[i].buffer_type = MYSQL_TYPE_TINY;
            binds[i].is_unsigned = 1;
            break;

        case NGX_DBD_DATA_TYPE_SHORT:
            binds[i].buffer_type = MYSQL_TYPE_SHORT;
            binds[i].is_unsigned = 0;
            break;

        case NGX_DBD_DATA_TYPE_USHORT:
            binds[i].buffer_type = MYSQL_TYPE_SHORT;
            binds[i].is_unsigned = 1;
            break;

        case NGX_DBD_DATA_TYPE_INT:
        case NGX_DBD_DATA_TYPE_LONG:
            binds[i].buffer_type = MYSQL_TYPE_LONG;
            binds[i].is_unsigned = 0;
            break;

        case NGX_DBD_DATA_TYPE_UINT:
        case NGX_DBD_DATA_TYPE_ULONG:
            binds[i].buffer_type = MYSQL_TYPE_LONG;
            binds[i].is_unsigned = 1;
            break;

        case NGX_DBD_DATA_TYPE_LONGLONG:
            binds[i].buffer_type = MYSQL_TYPE_LONGLONG;
            binds[i].is_unsigned = 0;
            break;

        case NGX_DBD_DATA_TYPE_ULONGLONG:
            binds[i].buffer_type = MYSQL_TYPE_LONGLONG;
            binds[i].is_unsigned = 1;
            break;

        case NGX_DBD_DATA_TYPE_FLOAT:
            binds[i].buffer_type = MYSQL_TYPE_FLOAT;
            binds[i].is_unsigned = 0;
            break;

        case NGX_DBD_DATA_TYPE_DOUBLE:
            binds[i].buffer_type = MYSQL_TYPE_DOUBLE;
            binds[i].is_unsigned = 0;
            break;

        case NGX_DBD_DATA_TYPE_STRING:
        case NGX_DBD_DATA_TYPE_TEXT:
        case NGX_DBD_DATA_TYPE_TIME:
        case NGX_DBD_DATA_TYPE_DATE:
        case NGX_DBD_DATA_TYPE_DATETIME:
        case NGX_DBD_DATA_TYPE_TIMESTAMP:
        case NGX_DBD_DATA_TYPE_ZTIMESTAMP:
            binds[i].buffer_type = MYSQL_TYPE_VAR_STRING;
            binds[i].is_unsigned = 0;
            binds[i].buffer_length = (unsigned long) params[i].size;
            break;

        case NGX_DBD_DATA_TYPE_BLOB:
        case NGX_DBD_DATA_TYPE_CLOB:
            binds[i].buffer_type = MYSQL_TYPE_LONG_BLOB;
            binds[i].is_unsigned = 0;
            binds[i].buffer_length = (unsigned long) params[i].size;
            break;

        case NGX_DBD_DATA_TYPE_NULL:
            binds[i].buffer_type = MYSQL_TYPE_NULL;
            break;

        default:
            /* TODO */
            return NGX_ERROR;
        }
    }

    return NGX_OK;
}


static void
ngx_dbd_mysql_free_result(ngx_dbd_res_t *res)
{
    if (res->res) {
        if (res->random == 0) {
            while (mysql_fetch_row(res->res)) {}
        }

        mysql_free_result(res->res);

        res->res = NULL;

    } else if (res->stmt) {

        if (res->metadata) {
            mysql_free_result(res->metadata);

            res->metadata = NULL;
        }

        if (res->random == 0) {
            while (mysql_stmt_fetch(res->stmt)) {}
        }

        mysql_stmt_free_result(res->stmt);

        res->stmt = NULL;
    }
}


#endif
