/*********************************************************************************
 * Modifications Copyright 2017-2019 eBay Inc.
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 *********************************************************************************/
#pragma once
#include <homestore/btree/btree.hpp>

namespace homestore {

/* This function does the heavy lifiting of co-ordinating inserts. It is a recursive function which walks
 * down the tree.
 *
 * NOTE: It expects the node it operates to be locked (either read or write) and also the node should not be
 * full.
 *
 * Input:
 * myNode      = Node it operates on
 * curLock     = Type of lock held for this node
 * req         = Req with information of current key/value to insert
 */
template < typename K, typename V >
template < typename ReqT >
btree_status_t Btree< K, V >::do_put(const BtreeNodePtr& my_node, locktype_t curlock, ReqT& req) {
    btree_status_t ret = btree_status_t::success;

    if (my_node->is_leaf()) {
        /* update the leaf node */
        BT_NODE_LOG_ASSERT_EQ(curlock, locktype_t::WRITE, my_node);
        ret = mutate_write_leaf_node(my_node, req);
        unlock_node(my_node, curlock);
        return ret;
    }

    auto unlock_lambda = [this](const BtreeNodePtr& node, locktype_t& cur_lock) {
        unlock_node(node, cur_lock);
        cur_lock = locktype_t::NONE;
    };

retry:
    uint32_t start_idx{0};
    uint32_t end_idx{0};
    uint32_t curr_idx;

    if constexpr (std::is_same_v< ReqT, BtreeRangePutRequest< K > >) {
        const auto matched = my_node->match_range(req.working_range(), start_idx, end_idx);
        if (!matched) {
            BT_NODE_LOG_ASSERT(false, my_node, "match_range returns 0 entries for interior node is not valid pattern");
            ret = btree_status_t::not_found;
            goto out;
        }
    } else if constexpr (std::is_same_v< ReqT, BtreeSinglePutRequest >) {
        auto const [found, idx] = my_node->find(req.key(), nullptr, true);
        ASSERT_IS_VALID_INTERIOR_CHILD_INDX(found, idx, my_node);
        end_idx = start_idx = idx;
    }

    BT_NODE_DBG_ASSERT((curlock == locktype_t::READ || curlock == locktype_t::WRITE), my_node, "unexpected locktype {}",
                       curlock);

    if (req.route_tracing) { append_route_trace(req, my_node, btree_event_t::READ, start_idx, end_idx); }

    curr_idx = start_idx;
    while (curr_idx <= end_idx) { // iterate all matched childrens
        locktype_t child_cur_lock = locktype_t::NONE;

        // Get the childPtr for given key.
        BtreeLinkInfo child_info;
        BtreeNodePtr child_node;
        ret = get_child_and_lock_node(my_node, curr_idx, child_info, child_node, locktype_t::READ, locktype_t::WRITE,
                                      req.m_op_context);
        if (ret != btree_status_t::success) {
            if (ret == btree_status_t::not_found) {
                // Either the node was updated or mynode is freed. Just proceed again from top.
                /* XXX: Is this case really possible as we always take the parent lock and never
                 * release it.
                 */
                ret = btree_status_t::retry;
            }
            goto out;
        }

        // Directly get write lock for leaf, since its an insert.
        child_cur_lock = (child_node->is_leaf()) ? locktype_t::WRITE : locktype_t::READ;
        if (is_split_needed(child_node, req)) {
            ret = upgrade_node_locks(my_node, child_node, curlock, child_cur_lock, req.m_op_context);
            if (ret != btree_status_t::success) {
                BT_NODE_LOG(DEBUG, my_node, "Upgrade of node lock failed, retrying from root");
                goto out;
            }

            K split_key;
            BT_NODE_LOG(TRACE, my_node, "Split node needed");
            ret = split_node(my_node, child_node, curr_idx, &split_key, req.m_op_context);
            unlock_lambda(child_node, child_cur_lock);
            if (ret != btree_status_t::success) { goto out; }

            if (req.route_tracing) { append_route_trace(req, child_node, btree_event_t::SPLIT); }
            COUNTER_INCREMENT(m_metrics, btree_split_count, 1);
            goto retry; // After split, retry search and walk down.
        }

        // Get subrange if it is a range update
        if constexpr (std::is_same_v< ReqT, BtreeRangePutRequest< K > >) {
            if (child_node->is_leaf()) {
                // We get the trimmed range only for leaf because this is where we will be inserting keys. In
                // interior nodes, keys are always propogated from the lower nodes.
                if (curr_idx < my_node->total_entries()) {
                    K child_end_key = my_node->get_nth_key< K >(curr_idx, true);
                    if (child_end_key.compare(req.working_range().end_key()) < 0) {
                        req.trim_working_range(std::move(child_end_key), true /* inclusive child key */);
                    }
                }

                BT_NODE_LOG(DEBUG, my_node, "Subrange:idx=[{}-{}],c={},working={}", start_idx, end_idx, curr_idx,
                            req.working_range().to_string());
            }
        }

#ifndef NDEBUG
        K ckey, pkey;
        if (curr_idx != my_node->total_entries()) { // not edge
            pkey = my_node->get_nth_key< K >(curr_idx, true);
            if (child_node->total_entries() != 0) {
                ckey = child_node->get_last_key< K >();
                // For extent-based keys, extent mutations (split/merge) can change
                // the child's last key to be smaller than the separator set at split
                // time. The separator remains a valid upper bound for routing.
                BT_NODE_DBG_ASSERT_LE(ckey.compare(pkey), 0, my_node);
            }
            // BT_NODE_DBG_ASSERT_EQ((is_range_put_req(req) || k.compare(pkey) <= 0), true, child_node);
        }
        if (curr_idx > 0) { // not first child
            pkey = my_node->get_nth_key< K >(curr_idx - 1, true);
            if (child_node->total_entries() != 0) {
                ckey = child_node->get_first_key< K >();
                BT_NODE_DBG_ASSERT_GE(ckey.compare(pkey), 0, child_node);
            }
            // BT_NODE_DBG_ASSERT_EQ((is_range_put_req(req) || k.compare(pkey) >= 0), true, my_node);
        }
#endif
        if (curr_idx == end_idx) {
            // If we have reached the last index, unlock before traversing down, because we no longer need
            // this lock. Holding this lock will impact performance unncessarily.
            unlock_lambda(my_node, curlock);
        }

        ret = do_put(child_node, child_cur_lock, req);
        if (ret != btree_status_t::success) { goto out; }

        ++curr_idx;
    }
out:
    if (curlock != locktype_t::NONE) { unlock_lambda(my_node, curlock); }
    return ret;
    // Warning: Do not access childNode or myNode beyond this point, since it would
    // have been unlocked by the recursive function and it could also been deleted.
}

template < typename K, typename V >
btree_status_t Btree< K, V >::mutate_extents_in_leaf(const BtreeNodePtr& my_node, BtreeRangePutRequest< K >& rpreq) {
    // Guard: only compile body for extent key/value types (K has lba_start/end_lba/nlba, V has blkid).
    // For non-extent types (e.g. test keys), this function is never called but may be instantiated.
    if constexpr (requires(K k, V v) { k.lba_start(); k.end_lba(); k.nlba(); v.blkid(); }) {
        auto const working = rpreq.working_range();
        auto const new_start_key = s_cast< K const& >(working.start_key());
        auto const new_end_key = s_cast< K const& >(working.end_key());
        // When start_inclusive is false, the working range start key is the separator from the previous
        // child — we already wrote up to its end_lba, so we must begin at end_lba + 1 to avoid overlap.
        auto const new_start = working.is_start_inclusive() ? new_start_key.lba_start()
                                                           : new_start_key.end_lba() + 1;
        auto const new_end = new_end_key.end_lba(); // Use end_lba() since trimmed keys may have nlba > 1
        auto const* new_val = s_cast< V const* >(rpreq.m_newval);
        // The input range start is the original write start — used for blkid offset calculation
        auto const input_start_key = s_cast< K const& >(rpreq.input_range().start_key());
        auto const range_start = input_start_key.lba_start();

        DEBUG_ASSERT_LE(new_start, new_end,
                        "adjusted start {} past end {} — separator end_lba should not exceed write range",
                        new_start, new_end);

        // 1. Find overlapping entries using the adjusted LBA range.
        // When start_inclusive was false (cross-child continuation), the working range start key
        // covers the separator LBA, not our actual write start. Use adjusted keys for match_range
        // so we correctly find all entries overlapping with [new_start, new_end].
        BtreeKeyRange< K > search_range{K{new_start, 1}, true, K{new_end, 1}, true};
        uint32_t start_idx{0}, end_idx{0};
        bool has_match = my_node->template match_range< K >(search_range, start_idx, end_idx);

        // 2. Build replacement list on the stack (max 3: left trim + new extent + right trim).
        std::array< std::pair< K, V >, 3 > replacements;
        uint32_t num_replacements{0};
        uint32_t overlapping_count = has_match ? (end_idx - start_idx + 1) : 0;

        // Read first/last overlapping entries once — reused for trim and filter callback.
        K first_key{}, last_key{};
        V first_val{}, last_val{};
        if (has_match) {
            first_key = my_node->get_nth_key< K >(start_idx, false);
            my_node->get_nth_value(start_idx, &first_val, false);
            if (start_idx != end_idx) {
                last_key = my_node->get_nth_key< K >(end_idx, false);
                my_node->get_nth_value(end_idx, &last_val, false);
            } else {
                last_key = first_key;
                last_val = first_val;
            }

            // a. Trim first entry — if it starts before our range, keep left portion
            if (first_key.lba_start() < new_start) {
                auto left_nlba = static_cast< uint32_t >(new_start - first_key.lba_start());
                BlkId left_blkid(first_val.blkid().blk_num(), static_cast< homestore::blk_count_t >(left_nlba),
                                 first_val.blkid().chunk_num());
                replacements[num_replacements++] = {K{first_key.lba_start(), left_nlba}, V{left_blkid}};
            }

            // b. New extent — the write range
            auto new_nlba = static_cast< uint32_t >(new_end - new_start + 1);
            auto new_offset = static_cast< uint32_t >(new_start - range_start);
            BlkId new_blkid(static_cast< homestore::blk_num_t >(new_val->blkid().blk_num() + new_offset),
                            static_cast< homestore::blk_count_t >(new_nlba), new_val->blkid().chunk_num());
            replacements[num_replacements++] = {K{new_start, new_nlba}, V{new_blkid}};

            // c. Trim last entry — if it extends past our range, keep right portion
            if (last_key.end_lba() > new_end) {
                auto right_start = new_end + 1;
                auto right_nlba = static_cast< uint32_t >(last_key.end_lba() - new_end);
                auto right_offset = static_cast< uint32_t >(right_start - last_key.lba_start());
                BlkId right_blkid(static_cast< homestore::blk_num_t >(last_val.blkid().blk_num() + right_offset),
                                  static_cast< homestore::blk_count_t >(right_nlba), last_val.blkid().chunk_num());
                replacements[num_replacements++] = {K{right_start, right_nlba}, V{right_blkid}};
            }
        } else {
            // No overlapping entries — just insert the new extent
            auto new_nlba = static_cast< uint32_t >(new_end - new_start + 1);
            auto new_offset = static_cast< uint32_t >(new_start - range_start);
            BlkId new_blkid(static_cast< homestore::blk_num_t >(new_val->blkid().blk_num() + new_offset),
                            static_cast< homestore::blk_count_t >(new_nlba), new_val->blkid().chunk_num());
            replacements[num_replacements++] = {K{new_start, new_nlba}, V{new_blkid}};
        }

        // 3. Check space: we need room for net new entries.
        int32_t net_new = static_cast< int32_t >(num_replacements) - static_cast< int32_t >(overlapping_count);
        if (net_new > 0) {
            uint32_t entry_size = K::get_fixed_size() + V::get_fixed_size();
            if (my_node->available_size() < static_cast< uint32_t >(net_new) * entry_size) {
                rpreq.shift_working_range(K{new_start, 1}, true);
                return btree_status_t::has_more;
            }
        }

        // 4. Filter callback — notify volume layer of overwritten blkids for freeing.
        // Reuses first_key/first_val/last_key/last_val already read above; only re-reads middle entries.
        if (has_match && rpreq.m_filter_cb) {
            for (uint32_t i = start_idx; i <= end_idx; ++i) {
                K ekey;
                V eval;
                if (i == start_idx) { ekey = first_key; eval = first_val; }
                else if (i == end_idx) { ekey = last_key; eval = last_val; }
                else { ekey = my_node->get_nth_key< K >(i, false); my_node->get_nth_value(i, &eval, false); }
                auto overlap_start = std::max(new_start, ekey.lba_start());
                auto overlap_end = std::min(new_end, ekey.end_lba());
                if (overlap_start <= overlap_end) {
                    auto overlap_nlba = static_cast< uint32_t >(overlap_end - overlap_start + 1);
                    auto old_offset = static_cast< uint32_t >(overlap_start - ekey.lba_start());
                    BlkId old_blkid(static_cast< homestore::blk_num_t >(eval.blkid().blk_num() + old_offset),
                                    static_cast< homestore::blk_count_t >(overlap_nlba),
                                    eval.blkid().chunk_num());
                    rpreq.m_filter_cb(K{overlap_start, overlap_nlba}, V{old_blkid}, *new_val);
                }
            }
        }

        // 5. Remove old entries, insert replacements at known position (no binary search).
        if (overlapping_count > 0) {
            COUNTER_DECREMENT(m_metrics, btree_obj_count, overlapping_count);
            my_node->remove(start_idx, end_idx);
        }

        for (uint32_t i = 0; i < num_replacements; ++i) {
            my_node->insert(start_idx + i, replacements[i].first, replacements[i].second);
            COUNTER_INCREMENT(m_metrics, btree_obj_count, 1);
        }

        // 6. Validate node: no overlapping entries, sorted, valid blkids
#ifndef NDEBUG
        {
            auto const total = my_node->total_entries();
            for (uint32_t i = 0; i < total; ++i) {
                K ki = my_node->get_nth_key< K >(i, false);
                V vi;
                my_node->get_nth_value(i, &vi, false);
                DEBUG_ASSERT_GT(ki.nlba(), 0, "entry {} has zero nlba: lba_start={}", i, ki.lba_start());
                DEBUG_ASSERT_GT(vi.blkid().blk_count(), 0, "entry {} has zero blk_count: lba_start={}", i,
                                ki.lba_start());
                DEBUG_ASSERT_EQ(ki.nlba(), vi.blkid().blk_count(),
                                "entry {} nlba/blk_count mismatch: lba=[{},{}] blk_num={} blk_count={}", i,
                                ki.lba_start(), ki.end_lba(), vi.blkid().blk_num(), vi.blkid().blk_count());
                if (i + 1 < total) {
                    K kn = my_node->get_nth_key< K >(i + 1, false);
                    DEBUG_ASSERT_LE(ki.end_lba() + 1, kn.lba_start(),
                                    "entries {} and {} overlap or out of order: [{},{}] vs [{},{}]", i, i + 1,
                                    ki.lba_start(), ki.end_lba(), kn.lba_start(), kn.end_lba());
                }
            }
        }
#endif

        // 7. Advance range cursor
        rpreq.shift_working_range();
        return btree_status_t::success;
    } else {
        // Non-extent types should never reach here
        DEBUG_ASSERT(false, "mutate_extents_in_leaf called for non-extent key type");
        return btree_status_t::not_found;
    }
}

template < typename K, typename V >
template < typename ReqT >
btree_status_t Btree< K, V >::mutate_write_leaf_node(const BtreeNodePtr& my_node, ReqT& req) {
    btree_status_t ret = btree_status_t::success;
    if constexpr (std::is_same_v< ReqT, BtreeRangePutRequest< K > >) {
        if constexpr (requires(K k) { k.lba_start(); k.end_lba(); k.nlba(); }) {
            // Extent path — mutate_extents_in_leaf handles overlapping extents with splits
            ret = mutate_extents_in_leaf(my_node, req);
        } else {
            // Default path — multi_put for interval keys (prefix) and fixed keys (range update)
            K last_failed_key;
            ret = to_variant_node(my_node)->multi_put(req.working_range(), req.input_range().start_key(), *req.m_newval,
                                                      req.m_put_type, &last_failed_key, req.m_filter_cb,
                                                      req.m_app_context);
            if (ret == btree_status_t::has_more) {
                req.shift_working_range(std::move(last_failed_key), true /* make it including last_failed_key */);
            } else if (ret == btree_status_t::success) {
                req.shift_working_range();
            }
        }
    } else if constexpr (std::is_same_v< ReqT, BtreeSinglePutRequest >) {
        ret =
            to_variant_node(my_node)->put(req.key(), req.value(), req.m_put_type, req.m_existing_val, req.m_filter_cb);
        COUNTER_INCREMENT(m_metrics, btree_obj_count, 1);
    }

    if ((ret == btree_status_t::success) || (ret == btree_status_t::has_more)) {
        if (req.route_tracing) { append_route_trace(req, my_node, btree_event_t::MUTATE); }
        write_node(my_node, req.m_op_context);
    }
    return ret;
}

template < typename K, typename V >
template < typename ReqT >
btree_status_t Btree< K, V >::check_split_root(ReqT& req) {
    K split_key;
    BtreeNodePtr child_node = nullptr;
    btree_status_t ret = btree_status_t::success;
    BtreeNodePtr root;
    BtreeNodePtr new_root;

    m_btree_lock.lock();
    ret = read_and_lock_node(m_root_node_info.bnode_id(), root, locktype_t::WRITE, locktype_t::WRITE, req.m_op_context);
    if (ret != btree_status_t::success) { goto done; }

    if (!is_split_needed(root, req)) {
        unlock_node(root, locktype_t::WRITE);
        goto done;
    }

    new_root = alloc_interior_node();
    if (new_root == nullptr) {
        ret = btree_status_t::space_not_avail;
        unlock_node(root, locktype_t::WRITE);
        goto done;
    }
    new_root->set_level(root->level() + 1);

    BT_NODE_LOG(DEBUG, root, "Root node={} is full, creating new root node={}", root->node_id(), new_root->node_id());
    child_node = std::move(root);
    root = std::move(new_root);

    // We need to notify about the root change, before splitting the node, so that correct dependencies are set
    ret = on_root_changed(root, req.m_op_context);
    if (ret != btree_status_t::success) {
        free_node(root, locktype_t::WRITE, req.m_op_context);
        unlock_node(child_node, locktype_t::WRITE);
        goto done;
    }

    ret = split_node(root, child_node, root->total_entries(), &split_key, req.m_op_context);
    if (ret != btree_status_t::success) {
        free_node(root, locktype_t::WRITE, req.m_op_context);
        root = std::move(child_node);
        on_root_changed(root, req.m_op_context); // Revert it back
        unlock_node(root, locktype_t::WRITE);
    } else {
        if (req.route_tracing) { append_route_trace(req, child_node, btree_event_t::SPLIT); }
        m_root_node_info = BtreeLinkInfo{root->node_id(), root->link_version()};
        this->m_btree_depth = root->level();
        unlock_node(child_node, locktype_t::WRITE);
        COUNTER_INCREMENT(m_metrics, btree_depth, 1);
    }

done:
    m_btree_lock.unlock();
    return ret;
}

template < typename K, typename V >
btree_status_t Btree< K, V >::split_node(const BtreeNodePtr& parent_node, const BtreeNodePtr& child_node,
                                         uint32_t parent_ind, K* out_split_key, void* context) {
    BtreeNodePtr child_node1 = child_node;
    BtreeNodePtr child_node2;
    child_node2.reset(child_node1->is_leaf() ? alloc_leaf_node().get() : alloc_interior_node().get());

    if (child_node2 == nullptr) { return (btree_status_t::space_not_avail); }

    btree_status_t ret = btree_status_t::success;

    child_node2->set_next_bnode(child_node1->next_bnode());
    child_node1->set_next_bnode(child_node2->node_id());
    child_node2->set_level(child_node1->level());
    uint32_t child1_filled_size = child_node1->node_data_size() - child_node1->available_size();

    auto split_size = m_bt_cfg.split_size(child1_filled_size);
    uint32_t res = child_node1->move_out_to_right_by_size(m_bt_cfg, *child_node2, split_size);

    BT_NODE_REL_ASSERT_GT(res, 0, child_node1,
                          "Unable to split entries in the child node"); // means cannot split entries
    BT_NODE_DBG_ASSERT_GT(child_node1->total_entries(), 0, child_node1);

    // Insert the last entry in first child to parent node
    *out_split_key = child_node1->get_last_key< K >();

    BT_NODE_LOG(TRACE, parent_node, "Available space for split entry={}", parent_node->available_size());

    child_node1->inc_link_version();

    // Update the existing parent node entry to point to second child ptr.
    // Don't change the order. First update the parent node and then insert the new key. This is important for casee
    // where the split key is the last key in the parent node. In this case, the split key should be inserted in the
    // parent node. If we insert the split key first, then the split key will be inserted in the parent node and the
    // last key in the parent node will be lost. This will lead to inconsistency in the tree. In case of empty parent
    // (i.e., new root) or updating the edge, this order made sure that edge is updated.
    parent_node->update(parent_ind, child_node2->link_info());
    parent_node->insert(parent_ind, *out_split_key, child_node1->link_info());

    BT_NODE_DBG_ASSERT_GT(child_node2->get_first_key< K >().compare(*out_split_key), 0, child_node2);
    BT_NODE_LOG(DEBUG, parent_node, "Split child_node={} with new_child_node={}, split_key={}", child_node1->node_id(),
                child_node2->node_id(), out_split_key->to_string());
    BT_NODE_LOG(DEBUG, child_node1, "Left child");
    BT_NODE_LOG(DEBUG, child_node2, "Right child");

    ret = transact_nodes({child_node2}, {}, child_node1, parent_node, context);

    // NOTE: Do not access parentInd after insert, since insert would have
    // shifted parentNode to the right.
    return ret;
}

template < typename K, typename V >
template < typename ReqT >
bool Btree< K, V >::is_split_needed(const BtreeNodePtr& node, ReqT& req) const {
    if (!node->is_leaf()) { // if internal node, size is atmost one additional entry, size of K/V
        return !node->has_room_for_put(btree_put_type::UPSERT, K::get_max_size(), BtreeLinkInfo::get_fixed_size());
    } else if constexpr (std::is_same_v< ReqT, BtreeRangePutRequest< K > >) {
        // For extent keys, mutate_extents_in_leaf may produce up to 3 entries
        // (left trim + new extent + right trim). Ensure room for at least 3.
        if constexpr (requires(K k) { k.lba_start(); k.end_lba(); k.nlba(); }) {
            uint32_t entry_size = K::get_fixed_size() + V::get_fixed_size();
            return node->available_size() < 3 * entry_size;
        }
        return !node->has_room_for_put(req.m_put_type, req.first_key_size(), req.m_newval->serialized_size());
    } else if constexpr (std::is_same_v< ReqT, BtreeSinglePutRequest >) {
        return !node->has_room_for_put(req.m_put_type, req.key().serialized_size(), req.value().serialized_size());
    } else {
        return false;
    }
}
} // namespace homestore
