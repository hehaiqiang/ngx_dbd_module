
/*
 * Copyright (C) Ngwsx
 */


#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_dbd.h>


#define NGX_DBD_MAX_DRIVERS  64


static ngx_dbd_driver_t  *ngx_dbd_drivers[NGX_DBD_MAX_DRIVERS];
static ngx_uint_t         ngx_dbd_driver_n;


ngx_int_t
ngx_dbd_add_driver(ngx_dbd_driver_t *drv)
{
    if (ngx_dbd_driver_n >= NGX_DBD_MAX_DRIVERS) {
        return NGX_ERROR;
    }

    ngx_dbd_drivers[ngx_dbd_driver_n++] = drv;

    return NGX_OK;
}


ngx_dbd_t *
ngx_dbd_create(ngx_pool_t *pool, ngx_log_t *log, u_char *name)
{
    ngx_dbd_t         *dbd;
    ngx_uint_t         i;
    ngx_dbd_driver_t  *drv;

    for (i = 0; i < ngx_dbd_driver_n; i++) {
        drv = ngx_dbd_drivers[i];

        if (ngx_strncmp(name, drv->name->data, drv->name->len) == 0) {
            goto found;
        }
    }

    ngx_log_error(NGX_LOG_ALERT, log, 0, "dbd driver \"%s\" not found", name);

    return NULL;

found:

    dbd = drv->create(pool, log);
    if (dbd == NULL) {
        return NULL;
    }

    dbd->drv = drv;

    return dbd;
}
