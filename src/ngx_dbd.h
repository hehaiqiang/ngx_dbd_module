
/*
 * Copyright (C) Ngwsx
 */


#ifndef _NGX_DBD_H_INCLUDED_
#define _NGX_DBD_H_INCLUDED_


#include <ngx_config.h>
#include <ngx_core.h>


#define NGX_DBD_OPT_NON_BLOCKING  0x01


typedef struct ngx_dbd_driver_s  ngx_dbd_driver_t;

typedef void (*ngx_dbd_handler_pt)(void *data);


typedef struct {
    ngx_dbd_driver_t    *drv;
    ngx_pool_t          *pool;
    ngx_log_t           *log;
    void                *ctx;
    int                  opts;
    ngx_dbd_handler_pt   handler;
    void                *data;
} ngx_dbd_t;


struct ngx_dbd_driver_s {
    ngx_str_t   *name;

#if 0
    ngx_int_t  (*init)(ngx_cycle_t *cycle);
    void       (*done)(ngx_cycle_t *cycle);
#endif

    ngx_dbd_t *(*create)(ngx_pool_t *pool, ngx_log_t *log);
    void       (*destroy)(ngx_dbd_t *dbd);

    int        (*set_options)(ngx_dbd_t *dbd, int opts);
    int        (*get_options)(ngx_dbd_t *dbd);

    ssize_t    (*escape)(ngx_dbd_t *dbd, u_char *dst, size_t dst_len,
                         u_char *src, size_t src_len);

    u_char    *(*error)(ngx_dbd_t *dbd);
    ngx_err_t  (*error_code)(ngx_dbd_t *dbd);

    ngx_int_t  (*set_handler)(ngx_dbd_t *dbd, ngx_dbd_handler_pt handler,
                              void *data);

    ngx_int_t  (*set_tcp)(ngx_dbd_t *dbd, u_char *host, in_port_t port);
    ngx_int_t  (*set_uds)(ngx_dbd_t *dbd, u_char *uds);
    ngx_int_t  (*set_auth)(ngx_dbd_t *dbd, u_char *user, u_char *passwd);
    ngx_int_t  (*set_db)(ngx_dbd_t *dbd, u_char *db);

    ngx_int_t  (*connect)(ngx_dbd_t *dbd);
    void       (*close)(ngx_dbd_t *dbd);

    ngx_int_t  (*set_sql)(ngx_dbd_t *dbd, u_char *sql, size_t len);
    ngx_int_t  (*query)(ngx_dbd_t *dbd);

    ngx_int_t  (*result_buffer)(ngx_dbd_t *dbd);
    uint64_t   (*result_warning_count)(ngx_dbd_t *dbd);
    uint64_t   (*result_insert_id)(ngx_dbd_t *dbd);
    uint64_t   (*result_affected_rows)(ngx_dbd_t *dbd);
    uint64_t   (*result_column_count)(ngx_dbd_t *dbd);
    uint64_t   (*result_row_count)(ngx_dbd_t *dbd);

    ngx_int_t  (*column_skip)(ngx_dbd_t *dbd);
    ngx_int_t  (*column_buffer)(ngx_dbd_t *dbd);
    ngx_int_t  (*column_read)(ngx_dbd_t *dbd);

    u_char    *(*column_catalog)(ngx_dbd_t *dbd);
    u_char    *(*column_db)(ngx_dbd_t *dbd);
    u_char    *(*column_table)(ngx_dbd_t *dbd);
    u_char    *(*column_orig_table)(ngx_dbd_t *dbd);
    u_char    *(*column_name)(ngx_dbd_t *dbd);
    u_char    *(*column_orig_name)(ngx_dbd_t *dbd);
    int        (*column_charset)(ngx_dbd_t *dbd);
    size_t     (*column_size)(ngx_dbd_t *dbd);
    size_t     (*column_max_size)(ngx_dbd_t *dbd);
    int        (*column_type)(ngx_dbd_t *dbd);
    int        (*column_flags)(ngx_dbd_t *dbd);
    int        (*column_decimals)(ngx_dbd_t *dbd);
    void      *(*column_default_value)(ngx_dbd_t *dbd, size_t *len);

    ngx_int_t  (*row_buffer)(ngx_dbd_t *dbd);
    ngx_int_t  (*row_read)(ngx_dbd_t *dbd);

    ngx_int_t  (*field_buffer)(ngx_dbd_t *dbd, u_char **value, size_t *size);
    ngx_int_t  (*field_read)(ngx_dbd_t *dbd, u_char **value, off_t *offset,
                             size_t *size, size_t *total);
};


ngx_int_t ngx_dbd_add_driver(ngx_dbd_driver_t *drv);
ngx_dbd_t *ngx_dbd_create(ngx_pool_t *pool, ngx_log_t *log, u_char *name);


#define ngx_dbd_destroy(dbd)               (dbd)->drv->destroy(dbd)

#define ngx_dbd_set_options(dbd, opts)     (dbd)->drv->set_options(dbd, opts)
#define ngx_dbd_get_options(dbd)           (dbd)->drv->get_options(dbd)

#define ngx_dbd_escape(dbd, dst, dst_len, src, src_len)                        \
    (dbd)->drv->escape(dbd, dst, dst_len, src, src_len)

#define ngx_dbd_error(dbd)                 (dbd)->drv->error(dbd)
#define ngx_dbd_error_code(dbd)            (dbd)->drv->error_code(dbd)

#define ngx_dbd_set_handler(dbd, handler, data)                                \
    (dbd)->drv->set_handler(dbd, handler, data)

#define ngx_dbd_set_tcp(dbd, host, port)   (dbd)->drv->set_tcp(dbd, host, port)
#define ngx_dbd_set_uds(dbd, uds)          (dbd)->drv->set_uds(dbd, uds)
#define ngx_dbd_set_auth(dbd, user, passwd)                                    \
    (dbd)->drv->set_auth(dbd, user, passwd)
#define ngx_dbd_set_db(dbd, db)            (dbd)->drv->set_db(dbd, db)

#define ngx_dbd_connect(dbd)               (dbd)->drv->connect(dbd)
#define ngx_dbd_close(dbd)                 (dbd)->drv->close(dbd)

#define ngx_dbd_set_sql(dbd, sql, len)     (dbd)->drv->set_sql(dbd, sql, len)
#define ngx_dbd_query(dbd)                 (dbd)->drv->query(dbd)

#define ngx_dbd_result_buffer(dbd)         (dbd)->drv->result_buffer(dbd)
#define ngx_dbd_result_warning_count(dbd)  (dbd)->drv->result_warning_count(dbd)
#define ngx_dbd_result_insert_id(dbd)      (dbd)->drv->result_insert_id(dbd)
#define ngx_dbd_result_affected_rows(dbd)  (dbd)->drv->result_affected_rows(dbd)
#define ngx_dbd_result_column_count(dbd)   (dbd)->drv->result_column_count(dbd)
#define ngx_dbd_result_row_count(dbd)      (dbd)->drv->result_row_count(dbd)

#define ngx_dbd_column_skip(dbd)           (dbd)->drv->column_skip(dbd)
#define ngx_dbd_column_buffer(dbd)         (dbd)->drv->column_buffer(dbd)
#define ngx_dbd_column_read(dbd)           (dbd)->drv->column_read(dbd)

#define ngx_dbd_column_catalog(dbd)        (dbd)->drv->column_catalog(dbd)
#define ngx_dbd_column_db(dbd)             (dbd)->drv->column_db(dbd)
#define ngx_dbd_column_table(dbd)          (dbd)->drv->column_table(dbd)
#define ngx_dbd_column_orig_table(dbd)     (dbd)->drv->column_orig_table(dbd)
#define ngx_dbd_column_name(dbd)           (dbd)->drv->column_name(dbd)
#define ngx_dbd_column_orig_name(dbd)      (dbd)->drv->column_orig_name(dbd)
#define ngx_dbd_column_charset(dbd)        (dbd)->drv->column_charset(dbd)
#define ngx_dbd_column_size(dbd)           (dbd)->drv->column_size(dbd)
#define ngx_dbd_column_max_size(dbd)       (dbd)->drv->column_max_size(dbd)
#define ngx_dbd_column_type(dbd)           (dbd)->drv->column_type(dbd)
#define ngx_dbd_column_flags(dbd)          (dbd)->drv->column_flags(dbd)
#define ngx_dbd_column_decimals(dbd)       (dbd)->drv->column_decimals(dbd)
#define ngx_dbd_column_default_value(dbd, len)                                 \
    (dbd)->drv->column_default_value(dbd, len)

#define ngx_dbd_row_buffer(dbd)            (dbd)->drv->row_buffer(dbd)
#define ngx_dbd_row_read(dbd)              (dbd)->drv->row_read(dbd)

#define ngx_dbd_field_buffer(dbd, value, size)                                 \
    (dbd)->drv->field_buffer(dbd, value, size)
#define ngx_dbd_field_read(dbd, value, offset, size, total)                    \
    (dbd)->drv->field_read(dbd, value, offset, size, total)


#endif /* _NGX_DBD_H_INCLUDED_ */
