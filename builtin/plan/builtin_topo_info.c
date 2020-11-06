/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include "../builtin.h"
#include "builtin_plan.h"
#include <math.h>
#include <ucs/debug/log.h>
#include <ucs/debug/assert.h>
#include <ucs/debug/memtrack.h>
#include <uct/api/uct_def.h>



/* find my own rank */
ucs_status_t ucg_builtin_find_myself(const ucg_group_params_t *group_params,
                                     ucg_group_member_index_t *myrank)
{
    /* find my own rank */
    unsigned member_idx;
    ucg_group_member_index_t init_myrank = (ucg_group_member_index_t) - 1;
    *myrank = init_myrank;
    for (member_idx = 0; member_idx < group_params->member_count; member_idx++) {
        ucs_assert(group_params->distance[member_idx] < UCG_GROUP_MEMBER_DISTANCE_LAST);
        if (group_params->distance[member_idx] == UCG_GROUP_MEMBER_DISTANCE_SELF) {
            *myrank = member_idx;
            break;
        }
    }

    if (*myrank == init_myrank) {
        ucs_error("No member with distance==UCP_GROUP_MEMBER_DISTANCE_SELF found");
        return UCS_ERR_INVALID_PARAM;
    }
    return UCS_OK;
}

static ucs_status_t ucg_builtin_check_topology_info(ucg_builtin_topology_info_params_t *topo_params)
{
    ucg_group_member_index_t init_member_idx = (ucg_group_member_index_t)-1;
    unsigned node_idx = 0;
    unsigned ppn_idx = 0;
    for (node_idx = 0; node_idx < topo_params->node_cnt; node_idx++) {
        ucs_info("node_index[%u] = %lu", node_idx, topo_params->subroot_array[node_idx]);
        if (topo_params->subroot_array[node_idx] == init_member_idx) {
            ucs_error("Invalid parameter: node #%u's subroot", node_idx);
            return UCS_ERR_INVALID_PARAM;
        }
    }

    for (ppn_idx = 0; ppn_idx < topo_params->ppn_cnt; ppn_idx++) {
        ucs_info("rank_same_node[%u] = %lu", ppn_idx, topo_params->rank_same_node[ppn_idx]);
        if (topo_params->rank_same_node[ppn_idx] == init_member_idx) {
            ucs_error("Invalid parameter: ppn index #%u in same node", ppn_idx);
            return UCS_ERR_INVALID_PARAM;
        }
    }

    return UCS_OK;
}

static ucs_status_t ucg_builtin_prepare_topology_info(const ucg_group_params_t *group_params,
                                                      ucg_builtin_topology_info_params_t *topo_params,
                                                      ucg_group_member_index_t myrank)
{
    ucg_group_member_index_t member_idx;
    unsigned node_idx = 0;
    unsigned ppn_idx = 0;
    ucg_group_member_index_t init_member_idx = (ucg_group_member_index_t)-1;

    /* allocate rank_same_node & subroot_array */
    size_t alloc_size = sizeof(ucg_group_member_index_t) * topo_params->ppn_cnt;
    topo_params->rank_same_node = (ucg_group_member_index_t*)UCG_ALLOC_CHECK(alloc_size, "rank same node");

    alloc_size = sizeof(ucg_group_member_index_t) * topo_params->node_cnt;
    topo_params->subroot_array  = (ucg_group_member_index_t*)UCG_ALLOC_CHECK(alloc_size, "subroot array");

    /* Initialization */
    for (node_idx = 0; node_idx < topo_params->node_cnt; node_idx++) {
        topo_params->subroot_array[node_idx] = init_member_idx;
    }

    for (ppn_idx = 0; ppn_idx < topo_params->ppn_cnt; ppn_idx++) {
        topo_params->rank_same_node[ppn_idx] = init_member_idx;
    }

    /* rank_same_node */
    for (member_idx = 0, ppn_idx = 0; member_idx < group_params->member_count; member_idx++) {
        if (group_params->node_index[member_idx] == group_params->node_index[myrank]) {
            topo_params->rank_same_node[ppn_idx++] = member_idx;
        }
    }

    /* subroot_array: Pick mininum rank number as subroot in same node */
    for (member_idx = 0; member_idx < group_params->member_count; member_idx++)  {
        node_idx = group_params->node_index[member_idx];
        if (member_idx < topo_params->subroot_array[node_idx]) {
            topo_params->subroot_array[node_idx] = member_idx;
        }
    }

    return UCS_OK;
}

ucs_status_t ucg_builtin_topology_info_create(ucg_builtin_topology_info_params_t *topo_params,
                                              const ucg_group_params_t *group_params,
                                              ucg_group_member_index_t root)
{
    ucs_status_t status;
    ucg_group_member_index_t member_idx;
    unsigned node_idx;
    unsigned ppn_idx = 0;
    ucg_group_member_index_t myrank = 0;
    /* initalization */
    topo_params->node_cnt = 0;
    topo_params->ppn_cnt = 0;

    status = ucg_builtin_find_myself(group_params, &myrank);
    if (status != UCS_OK) {
        return status;
    }

    /* obtain node count, ppn_cnt */
    for (member_idx = 0; member_idx < group_params->member_count; member_idx++) {
        if (topo_params->node_cnt < group_params->node_index[member_idx]) {
            topo_params->node_cnt = group_params->node_index[member_idx];
        }
        if (group_params->node_index[member_idx] == group_params->node_index[myrank]) {
            (topo_params->ppn_cnt)++;
        }
    }

    (topo_params->node_cnt)++;
    status = ucg_builtin_prepare_topology_info(group_params, topo_params, myrank);
    if (status != UCS_OK) {
        return status;
    }

    /* Special case: root is not minumum */
    /* root must be sub-root no matter its value */
    node_idx = group_params->node_index[root];
    topo_params->subroot_array[node_idx] = root;

    if (group_params->node_index[myrank] == group_params->node_index[root]) {
        /* find ppn_idx corresponding to root rank */
        for (ppn_idx = 0; ppn_idx < topo_params->ppn_cnt; ppn_idx++) {
            if (topo_params->rank_same_node[ppn_idx] == root) {
                break;
            }
        }
        /* Swap root and current same_rank_node[0] */
        ucg_group_member_index_t tmp_member_idx = topo_params->rank_same_node[0];
        topo_params->rank_same_node[0] = root;
        topo_params->rank_same_node[ppn_idx] = tmp_member_idx;
    }

    ucs_info("rank #%lu: node count = %u, ppn count = %u\n", myrank, topo_params->node_cnt, topo_params->ppn_cnt);
    /* Check */
    return ucg_builtin_check_topology_info(topo_params);
}

/* check ppn balance or not */
ucs_status_t ucg_builtin_check_ppn(const ucg_group_params_t *group_params,
                                   unsigned *unequal_ppn)
{
    ucg_group_member_index_t member_idx;
    volatile unsigned node_cnt = 0;
    unsigned node_idx;
    *unequal_ppn = 0;
    /* node count */
    for (member_idx = 0; member_idx < group_params->member_count; member_idx++) {
        node_idx = group_params->node_index[member_idx];
        if (node_cnt < node_idx) {
            node_cnt = node_idx;
        }
    }
    node_cnt++;

    /* ppn array: record ppn vaule in every single node */
    size_t alloc_size = sizeof(unsigned) * node_cnt;
    unsigned *ppn_array = (unsigned *)UCG_ALLOC_CHECK(alloc_size, "ppn array");
    memset(ppn_array, 0, alloc_size);
    for (member_idx = 0; member_idx < group_params->member_count; member_idx++) {
        node_idx = group_params->node_index[member_idx];
        ppn_array[node_idx]++;
    }

    /* check balance ppn or not */
    for (node_idx = 0; node_idx < (node_cnt - 1); node_idx++) {
        if (ppn_array[node_idx] != ppn_array[node_idx + 1]) {
            *unequal_ppn = 1;
            break;
        }
    }

    ucs_free(ppn_array);
    ppn_array = NULL;
    return UCS_OK;
}