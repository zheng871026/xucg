/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019-2020.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCG_COLLECTIVE_H_
#define UCG_COLLECTIVE_H_

#include <ucg/api/ucg_def.h>
#include <ucs/type/status.h>

BEGIN_C_DECLS

ucs_status_t ucg_collective_release_barrier(ucg_group_h group);

END_C_DECLS
#endif