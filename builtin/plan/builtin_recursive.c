/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include "../builtin.h"
#include "builtin_plan.h"
#include <string.h>
#include <ucs/debug/log.h>
#include <ucs/debug/memtrack.h>
#include <uct/api/uct_def.h>
#include <ucg/api/ucg_mpi.h>

#define MAX_PEERS 100
#define MAX_PHASES 16
#define NUM_TWO 2

static ucs_status_t ucg_builtin_recursive_non_pow_two_pre(ucg_builtin_planner_ctx_t *ctx,
                                                          uct_ep_h *next_ep,
                                                          ucg_builtin_plan_phase_t *phase,
                                                          ucg_group_member_index_t my_index,
                                                          ucg_group_member_index_t *member_list,
                                                          ucg_step_idx_ext_t step_idx,
                                                          unsigned extra_indexs,
                                                          unsigned factor,
                                                          ucg_builtin_plan_t *recursive)
{
    ucs_status_t status;
    if (my_index % NUM_TWO != 0) {       // add  pre- and after- processing steps;
        phase->method = UCG_PLAN_METHOD_REDUCE_TERMINAL;
        phase->ep_cnt = factor - 1;
        phase->step_index = step_idx;
#if ENABLE_DEBUG_DATA
        phase->indexes = UCG_ALLOC_CHECK((factor - 1) * sizeof(my_index), "recursive topology indexes");
#endif
        ucg_group_member_index_t peer_index = my_index - 1;
        phase->multi_eps = next_ep;
        phase->is_swap = 0;
        status = ucg_builtin_connect(ctx, member_list[peer_index], phase, UCG_BUILTIN_CONNECT_SINGLE_EP);
    } else { // only pre- and after- processing steps;
        phase->method = UCG_PLAN_METHOD_SEND_TERMINAL;
        phase->ep_cnt = factor - 1;
        phase->step_index = step_idx;
#if ENABLE_DEBUG_DATA
        phase->indexes = UCG_ALLOC_CHECK((factor - 1) * sizeof(my_index), "recursive topology indexes");
#endif
        ucg_group_member_index_t peer_index = my_index + 1;
        phase->multi_eps = next_ep;
        phase->is_swap = 0;
        status = ucg_builtin_connect(ctx, member_list[peer_index], phase, UCG_BUILTIN_CONNECT_SINGLE_EP);
    }

    return status;
}

static ucs_status_t ucg_builtin_recursive_non_pow_two_post(ucg_builtin_planner_ctx_t *ctx,
                                                           uct_ep_h *next_ep,
                                                           ucg_builtin_plan_phase_t *phase,
                                                           ucg_group_member_index_t my_index,
                                                           ucg_group_member_index_t *member_list,
                                                           ucg_step_idx_ext_t step_idx,
                                                           unsigned extra_indexs,
                                                           unsigned factor,
                                                           unsigned near_power_of_two_step,
                                                           ucg_builtin_plan_t *recursive)
{
    ucs_status_t status;
    if (my_index % NUM_TWO != 0) {       /* add pre- and after- processing steps */
        phase->method = UCG_PLAN_METHOD_SEND_TERMINAL;
        phase->ep_cnt = factor - 1;
        phase->step_index = step_idx;
#if ENABLE_DEBUG_DATA
        phase->indexes = UCG_ALLOC_CHECK((factor - 1) * sizeof(my_index), "recursive topology indexes");
#endif
        ucg_group_member_index_t peer_index = my_index - 1;
        phase->multi_eps = next_ep;
        phase->is_swap = 0;
        status = ucg_builtin_connect(ctx, member_list[peer_index], phase, UCG_BUILTIN_CONNECT_SINGLE_EP);
    } else { // only pre- and after- processing steps;
        phase->method = UCG_PLAN_METHOD_RECV_TERMINAL;
        phase->ep_cnt = factor - 1;
        phase->step_index = step_idx;
#if ENABLE_DEBUG_DATA
        phase->indexes = UCG_ALLOC_CHECK((factor - 1) * sizeof(my_index), "recursive topology indexes");
#endif
        ucg_group_member_index_t peer_index = my_index + 1;
        phase->multi_eps = next_ep;
        phase->is_swap = 0;
        status = ucg_builtin_connect(ctx, member_list[peer_index], phase, UCG_BUILTIN_CONNECT_SINGLE_EP);
    }
    return status;
}

static ucs_status_t ucg_builtin_check_swap(unsigned factor, ucg_step_idx_t step_idx,
                                           ucg_group_member_index_t my_index, ucg_builtin_plan_phase_t *phase)
{
    /* The condition which don't use peer_idx as considering communicator split and dup case */
    unsigned current_scale = 1;
    unsigned i;
    for (i = 0; i < step_idx + 1; i++) {
        current_scale *= factor;
    }
    if ((my_index % current_scale) < (current_scale / factor)) {
        phase->is_swap = 1;
    } else {
        phase->is_swap = 0;
    }

    return UCS_OK;
}

static ucs_status_t ucg_builtin_recursive_non_pow_two_inter(ucg_builtin_planner_ctx_t *ctx,
                                                            ucg_group_member_index_t new_my_index,
                                                            ucg_group_member_index_t *member_list,
                                                            unsigned step_size,
                                                            unsigned near_power_of_two_step,
                                                            unsigned factor,
                                                            unsigned extra_indexs,
                                                            unsigned check_swap,
                                                            ucg_step_idx_ext_t step_idx,
                                                            ucg_builtin_plan_phase_t **phase,
                                                            uct_ep_h **next_ep,
                                                            ucg_builtin_plan_t *recursive)
{
    ucs_status_t status = UCS_OK;
    ucg_step_idx_t idx;
    if (new_my_index != ((ucg_group_member_index_t)-1)) {
        for (idx = 0, step_size = 1; ((idx < near_power_of_two_step) && (status == UCS_OK));
            idx++, (*phase)++, step_size *= factor) {
            unsigned step_base = new_my_index - (new_my_index % (step_size * factor));
            (*phase)->method = UCG_PLAN_METHOD_REDUCE_RECURSIVE;
            (*phase)->ep_cnt = factor - 1;
            (*phase)->step_index = step_idx + idx; /* need further check */
#if ENABLE_DEBUG_DATA
            (*phase)->indexes = UCG_ALLOC_CHECK((factor - 1) * sizeof(new_my_index), "recursive topology indexes");
#endif

            if (check_swap) {
                (void)ucg_builtin_check_swap(factor, idx, new_my_index, (*phase));
            }
            /* In each step, there are one or more peers */
            unsigned step_peer_idx;
            for (step_peer_idx = 1; ((step_peer_idx < factor) && (status == UCS_OK)); step_peer_idx++) {
                ucg_group_member_index_t peer_index =
                    step_base + ((new_my_index - step_base + step_size * step_peer_idx) % (step_size * factor));
                if (peer_index < extra_indexs) {
                    peer_index = NUM_TWO * peer_index + 1;
                } else {
                    peer_index += extra_indexs;
                }
                ucs_info("%lu's peer #%u/%u (step #%u/%u): %lu ", new_my_index, step_peer_idx, factor - 1, idx + 1,
                    recursive->phs_cnt, peer_index);
                (*phase)->multi_eps = (*next_ep)++;
                status = ucg_builtin_connect(ctx, member_list[peer_index], (*phase),
                    (factor != NUM_TWO) ? (step_peer_idx - 1) : UCG_BUILTIN_CONNECT_SINGLE_EP);
            }
            recursive->phs_cnt++;
            recursive->step_cnt++;
        }
    }
    return status;
}

static ucs_status_t ucg_builtin_recursive_non_pow_two(ucg_builtin_planner_ctx_t *ctx,
                                                      ucg_group_member_index_t my_index,
                                                      ucg_group_member_index_t *member_list,
                                                      ucg_group_member_index_t member_cnt,
                                                      unsigned factor,
                                                      unsigned step_size,
                                                      unsigned step_cnt,
                                                      unsigned check_swap,
                                                      ucg_builtin_plan_t *recursive)
{
    ucg_builtin_plan_phase_t *phase = &recursive->phss[recursive->phs_cnt];
    ucg_group_member_index_t new_my_index = (ucg_group_member_index_t) - 1;
    unsigned extra_indexs;
    unsigned near_power_of_two_step = (step_size != member_cnt) ? (step_cnt - 1) : step_cnt;
    step_size >>= 1;
    extra_indexs = member_cnt - step_size;
    if (my_index < NUM_TWO * extra_indexs && my_index % NUM_TWO == 0) {
        step_cnt = NUM_TWO;  // only pre- and after- processing steps
    } else if (my_index < NUM_TWO * extra_indexs && my_index % NUM_TWO != 0) {
        step_cnt++;  // add  pre- and after- processing steps
        new_my_index = my_index / NUM_TWO;
    } else {
        step_cnt--;  // no   pre- and after- processing steps
        new_my_index = my_index - extra_indexs;
    }
    /*
     * for     power of two processes case:
     * - near_power_of_two = log2(proc_count)
     * for non power of two processes case:
     * - near_power_of_two = log2(nearest power of two number less than proc_count)
     */
    /*
       TO support non-commutative operation, like matrix multiplication, modified recursive doubling change a little
       - Even ranks less than 2 * extra_indexs only has pre- and after- processing steps;
       - Odd  ranks less than 2 * extra_indexs participate all processing steps;
       - ranks more than 2 * extra_indexs only pure recursive doubling.

       An example:    0    1    2    3    4    5
       pre-           0 -> 1    2 -> 3    4    5
       RD:                 1         3    4    5

                         1  <->  3    4  <->  5
                         1  <->  4    3  <->  5

       after-         0 <- 1    2 <- 3    4    5
    */
    ucs_status_t status;
    ucg_step_idx_ext_t step_idx = recursive->step_cnt;
    uct_ep_h *next_ep               = (uct_ep_h*)(&recursive->phss[MAX_PHASES]) + recursive->ep_cnt;
    if (my_index < (NUM_TWO * extra_indexs)) {
        /* pre - processing steps for non power of two processes case */
        status = ucg_builtin_recursive_non_pow_two_pre(ctx, next_ep, phase, my_index, member_list,
                                                       step_idx, extra_indexs, factor,
                                                       recursive);
        if (status != UCS_OK) {
            return status;
        }
        phase++;
        next_ep++;
        recursive->phs_cnt++;
        recursive->ep_cnt++;
    }
    ++step_idx;

    /* Calculate the peers for each step */
    status = ucg_builtin_recursive_non_pow_two_inter(ctx, new_my_index, member_list, step_size, near_power_of_two_step,
                                                     factor, extra_indexs, check_swap, step_idx, &phase, &next_ep,
                                                     recursive);
    if (status != UCS_OK) {
        return status;
    }
    step_idx += near_power_of_two_step;

    if (my_index < (NUM_TWO * extra_indexs)) {
        /* after - processing steps for non power of two processes case */
        status = ucg_builtin_recursive_non_pow_two_post(ctx, next_ep, phase, my_index, member_list,
                                                        step_idx, extra_indexs, factor, near_power_of_two_step,
                                                        recursive);
        if (status != UCS_OK) {
            return status;
        }
        phase++;
        next_ep++;
        recursive->phs_cnt++;
        recursive->ep_cnt++;
    }
    ++step_idx;

    return status;
}

static ucs_status_t ucg_builtin_recursive_pow_two(ucg_builtin_planner_ctx_t *ctx,
                                                  ucg_group_member_index_t my_index,
                                                  ucg_group_member_index_t *member_list,
                                                  ucg_group_member_index_t member_cnt,
                                                  unsigned factor,
                                                  unsigned step_cnt,
                                                  unsigned check_swap,
                                                  ucg_builtin_plan_t *recursive)
{
    ucg_builtin_plan_phase_t *phase = &recursive->phss[recursive->phs_cnt];
    uct_ep_h *next_ep               = (uct_ep_h*)(&recursive->phss[MAX_PHASES]) + recursive->ep_cnt;
    ucs_status_t status = UCS_OK;
    ucg_step_idx_t step_idx;
    unsigned step_size;
    for (step_idx = 0, step_size = 1; ((step_idx < step_cnt) && (status == UCS_OK));
        step_idx++, phase++, step_size *= factor) {
        unsigned step_base = my_index - (my_index % (step_size * factor));

        phase->method = UCG_PLAN_METHOD_REDUCE_RECURSIVE;
        phase->ep_cnt = factor - 1;
        phase->step_index = recursive->step_cnt + step_idx; /* plus 1 to be consistent with no-power-of two process */
#if ENABLE_DEBUG_DATA
            phase->indexes = UCG_ALLOC_CHECK((factor - 1) * sizeof(my_index), "recursive topology indexes");
#endif

        if (check_swap) {
            (void)ucg_builtin_check_swap(factor, step_idx, my_index, phase);
        }
        /* In each step, there are one or more peers */
        unsigned step_peer_idx;
        for (step_peer_idx = 1; ((step_peer_idx < factor) && (status == UCS_OK)); step_peer_idx++) {
            ucg_group_member_index_t peer_index =
                step_base + ((my_index - step_base + step_size * step_peer_idx) % (step_size * factor));
            ucs_info("%lu's peer #%u/%u (step #%u/%u): %lu ", my_index, step_peer_idx, factor - 1, step_idx + 1,
                recursive->phs_cnt, peer_index);
            phase->multi_eps = next_ep++;
            recursive->ep_cnt++;

            status = ucg_builtin_connect(ctx, member_list[peer_index], phase,
                (factor != NUM_TWO) ? (step_peer_idx - 1) : UCG_BUILTIN_CONNECT_SINGLE_EP);
        }
        /* update the count of phase and step */
        recursive->phs_cnt++;
        recursive->step_cnt++;
    }

    return status;
}

void ucg_builtin_recursive_log(ucg_builtin_plan_t *recursive)
{
    int i;
    for (i = 0; i < recursive->phs_cnt; i++) {
        ucs_debug("phs %d ep_cnt %d step_idx %d method %d", i, (int)recursive->phss[i].ep_cnt,
                  (int)recursive->phss[i].step_index, (int)recursive->phss[i].method);
    }
}

ucs_status_t ucg_builtin_recursive_connect(ucg_builtin_planner_ctx_t *ctx,
                                           ucg_group_member_index_t my_rank,
                                           ucg_group_member_index_t *member_list,
                                           ucg_group_member_index_t member_cnt,
                                           unsigned factor,
                                           unsigned check_swap,
                                           ucg_builtin_plan_t *recursive)
{
    ucg_group_member_index_t my_index = (ucg_group_member_index_t)-1;
    /* find the local my own index */
    ucg_group_member_index_t member_idx;
    for (member_idx = 0; member_idx < member_cnt; member_idx++) {
        if (member_list[member_idx] == my_rank) {
            my_index = member_idx;
            break;
        }
    }

    if (my_index == (ucg_group_member_index_t)-1) {
        ucs_error("No member with distance==UCP_GROUP_MEMBER_DISTANCE_SELF found");
        return UCS_ERR_INVALID_PARAM;
    }

    unsigned step_size = 1;
    ucg_step_idx_t step_cnt = 0;
    while (step_size < member_cnt) {
        step_size *= factor;
        step_cnt++;
    }
    ucs_debug("recursive factor: %u, step size: %u, my index: %" PRId64 "", factor, step_size, my_index);

    ucs_status_t status;
    if (step_size != member_cnt) {
        ucs_debug("not power of two, step index: %hhu", step_cnt);
        status = ucg_builtin_recursive_non_pow_two(ctx, my_index,
                                                   member_list, member_cnt, factor, step_size,
                                                   step_cnt, check_swap, recursive);
    } else {
        status = ucg_builtin_recursive_pow_two(ctx, my_index, member_list, member_cnt, factor,
                                               step_cnt, check_swap, recursive);
    }
    ucg_builtin_recursive_log(recursive);

    return status;
}

ucs_status_t ucg_builtin_recursive_compute_steps(ucg_group_member_index_t my_index_local, unsigned rank_count,
                                                 unsigned factor, unsigned *steps)
{
    unsigned step_size = 1;
    ucg_step_idx_t step_idx = 0;
    while (step_size < rank_count) {
            step_size *= factor;
            step_idx++;
        }

    unsigned extra_indexs;
    unsigned near_power_of_two_step;

    /*
     * for   power of two processes case:
     * - near_power_of_two = log2(proc_count)
     * for non power of two processes case:
     * - near_power_of_two = log2(nearest power of two number less than proc_count)
     */
    near_power_of_two_step = (step_size != rank_count) ? (step_idx - 1) : step_idx;

    /* non-power of two case */
    if (step_size != rank_count) {
        step_size >>= 1;
        extra_indexs = rank_count - step_size;
        if (my_index_local < (rank_count - NUM_TWO * extra_indexs)) {
            step_idx--; // no   pre- and after- processing steps;
        } else if (my_index_local < rank_count - extra_indexs) {
            step_idx++; // add  pre- and after- processing steps;
        } else {
            step_idx = NUM_TWO; // only pre- and after- processing steps;
        }
    }

    *steps = (step_size != rank_count) ? (near_power_of_two_step + NUM_TWO) : step_idx;

    return UCS_OK;
}

void ucg_builtin_recursive_init_member_list(ucg_group_member_index_t member_cnt, ucg_group_member_index_t *member_list)
{
    ucg_group_member_index_t i;
    for (i = 0; i < member_cnt; i++) {
        member_list[i] = i;
    }
}

ucs_status_t ucg_builtin_recursive_create(ucg_builtin_planner_ctx_t *ctx,
    enum ucg_builtin_plan_topology_type plan_topo_type, const ucg_builtin_config_t *config,
    const ucg_group_params_t *group_params, const ucg_collective_type_t *coll_type, ucg_builtin_plan_t **plan_p)
{
    /* Find my own index */
    ucg_group_member_index_t my_rank = 0;
    while ((my_rank < group_params->member_count) &&
           (group_params->distance[my_rank] !=
           UCG_GROUP_MEMBER_DISTANCE_SELF)) {
        my_rank++;
    }

    ucg_group_member_index_t member_cnt = group_params->member_count;
    ucg_group_member_index_t *member_list = UCG_ALLOC_CHECK(member_cnt * sizeof(ucg_group_member_index_t), "member list");
    ucg_builtin_recursive_init_member_list(member_cnt, member_list);

    unsigned factor = config->recursive.factor;
    ucg_step_idx_t step_cnt = 0;
    unsigned step_size = 1;
    while (step_size < member_cnt) {
        step_size *= factor;
        step_cnt++;
    }
    /* Allocate memory resources */
    size_t alloc_size = sizeof(ucg_builtin_plan_t) +
            (MAX_PHASES * sizeof(ucg_builtin_plan_phase_t)) + MAX_PEERS * sizeof(uct_ep_h);
    if (factor != NUM_TWO) {
        /* Allocate extra space for the map's multiple endpoints */
        alloc_size += step_cnt * (factor - 1) * sizeof(uct_ep_h);
    }
    ucg_builtin_plan_t *recursive = (ucg_builtin_plan_t*)ucs_malloc(alloc_size, "recursive topology");
    if (recursive == NULL) {
        ucs_free(member_list);
        member_list = NULL;
        return UCS_ERR_NO_MEMORY;
    }
    memset(recursive, 0, alloc_size);
    ucs_status_t status = ucg_builtin_recursive_connect(ctx, my_rank, member_list, member_cnt, factor, 1, recursive);
    if (status != UCS_OK) {
        goto out;
    }

    recursive->super.my_index = my_rank;
    recursive->feature |= UCG_ALGORITHM_SUPPORT_NON_COMMUTATIVE_OPS |
                          UCG_ALGORITHM_SUPPORT_LARGE_DATATYPE;
    *plan_p = recursive;
out:
    ucs_free(member_list);
    member_list = NULL;
    return status;
}