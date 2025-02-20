/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <Storages/MergeTree/CloudMergeTreeBlockOutputStream.h>

#include <CloudServices/CnchDedupHelper.h>
#include <CloudServices/CnchPartsHelper.h>
#include <CloudServices/CnchDataWriter.h>
#include <Interpreters/PartLog.h>
#include <MergeTreeCommon/MergeTreeDataDeduper.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Storages/StorageCloudMergeTree.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/BitEngine/BitEngineDictionaryManager.h>
#include <Transaction/CnchWorkerTransaction.h>
#include <WorkerTasks/ManipulationType.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int CNCH_LOCK_ACQUIRE_FAILED;
    extern const int INSERTION_LABEL_ALREADY_EXISTS;
    extern const int LOGICAL_ERROR;
    extern const int UNIQUE_KEY_STRING_SIZE_LIMIT_EXCEEDED;
}

CloudMergeTreeBlockOutputStream::CloudMergeTreeBlockOutputStream(
    MergeTreeMetaBase & storage_,
    StorageMetadataPtr metadata_snapshot_,
    ContextPtr context_,
    bool to_staging_area_)
    : storage(storage_)
    , log(storage.getLogger())
    , metadata_snapshot(std::move(metadata_snapshot_))
    , context(std::move(context_))
    , to_staging_area(to_staging_area_)
    , writer(storage, IStorage::StorageLocation::AUXILITY)
    , cnch_writer(storage, context, ManipulationType::Insert)
{
    if (!metadata_snapshot->hasUniqueKey() && to_staging_area)
        throw Exception("Table doesn't have UNIQUE KEY specified, can't write to staging area", ErrorCodes::LOGICAL_ERROR);
}

Block CloudMergeTreeBlockOutputStream::getHeader() const
{
    return metadata_snapshot->getSampleBlock();
}

void CloudMergeTreeBlockOutputStream::writePrefix()
{
    auto max_threads = context->getSettingsRef().max_threads_for_cnch_dump;
    LOG_DEBUG(log, "dump with {} threads", max_threads);
    cnch_writer.initialize(max_threads);
}

void CloudMergeTreeBlockOutputStream::write(const Block & block)
{
    Stopwatch watch;
    LOG_DEBUG(storage.getLogger(), "Start to write new block of size: {}", block.rows());
    auto temp_parts = convertBlockIntoDataParts(block);
    /// Generate delete bitmaps, delete bitmap is valid only when using delete_flag info for unique table
    LocalDeleteBitmaps bitmaps;
    const auto & txn = context->getCurrentTransaction();
    for (const auto & part : temp_parts)
    {
        auto delete_bitmap = part->getDeleteBitmap(/*allow_null*/ true);
        if (delete_bitmap && delete_bitmap->cardinality())
        {
            bitmaps.emplace_back(LocalDeleteBitmap::createBase(
                part->info, std::const_pointer_cast<Roaring>(delete_bitmap), txn->getPrimaryTransactionID().toUInt64()));
            part->delete_flag = true;
        }
    }
    LOG_DEBUG(storage.getLogger(), "Finish converting block into parts, elapsed {} ms", watch.elapsedMilliseconds());
    watch.restart();

    IMutableMergeTreeDataPartsVector temp_staged_parts;
    if (to_staging_area)
    {
        temp_staged_parts.swap(temp_parts);
    }

    cnch_writer.schedule(temp_parts, bitmaps, temp_staged_parts);
}

MergeTreeMutableDataPartsVector CloudMergeTreeBlockOutputStream::convertBlockIntoDataParts(const Block & block, bool use_inner_block_id)
{
    auto part_log = context->getGlobalContext()->getPartLog(storage.getDatabaseName());
    auto merge_tree_settings = storage.getSettings();
    auto settings = context->getSettingsRef();

    BlocksWithPartition part_blocks;

    /// For unique table, need to ensure that each part does not contain duplicate keys
    /// - when unique key is partition-level, split into sub-blocks first and then dedup the sub-block for each partition
    /// - when unique key is table-level
    /// -   if without version column, should dedup the input block first because split may change row order
    /// -   if use partition value as version, split first because `dedupWithUniqueKey` doesn't evaluate partition key expression
    /// -   if use explicit version, both approach work
    if (metadata_snapshot->hasUniqueKey() && !merge_tree_settings->partition_level_unique_keys
        && !storage.merging_params.partitionValueAsVersion())
    {
        FilterInfo filter_info = dedupWithUniqueKey(block);
        part_blocks = writer.splitBlockIntoParts(
            filter_info.num_filtered ? CnchDedupHelper::filterBlock(block, filter_info) : block,
            settings.max_partitions_per_insert_block,
            metadata_snapshot,
            context);
    }
    else
        part_blocks = writer.splitBlockIntoParts(block, settings.max_partitions_per_insert_block, metadata_snapshot, context);

    IMutableMergeTreeDataPartsVector parts;
    LOG_DEBUG(storage.getLogger(), "size of part_blocks {}", part_blocks.size());

    const auto & txn = context->getCurrentTransaction();
    auto primary_txn_id = txn->getPrimaryTransactionID();

    // Get all blocks of partition by expression
    for (auto & block_with_partition : part_blocks)
    {
        Row original_partition{block_with_partition.partition};

        /// We need to dedup in block before split block by cluster key when unique table supports cluster key because cluster key may be different with unique key. Otherwise, we will lost the insert order.
        if (metadata_snapshot->hasUniqueKey()
            && (merge_tree_settings->partition_level_unique_keys || storage.merging_params.partitionValueAsVersion()))
        {
            FilterInfo filter_info = dedupWithUniqueKey(block_with_partition.block);
            if (filter_info.num_filtered)
                block_with_partition.block = CnchDedupHelper::filterBlock(block_with_partition.block, filter_info);
        }

        auto bucketed_part_blocks = writer.splitBlockPartitionIntoPartsByClusterKey(
            block_with_partition, context->getSettingsRef().max_partitions_per_insert_block, metadata_snapshot, context);
        LOG_TRACE(storage.getLogger(), "size of bucketed_part_blocks {}", bucketed_part_blocks.size());

        for (auto & bucketed_block_with_partition : bucketed_part_blocks)
        {
            Stopwatch watch;
            bucketed_block_with_partition.partition = Row(original_partition);

            if (storage.isBitEngineMode())
                writeImplicitColumnForBitEngine(bucketed_block_with_partition, {});

            auto block_id = use_inner_block_id ? increment.get() : context->getTimestamp();

            MergeTreeMutableDataPartPtr temp_part
                = writer.writeTempPart(bucketed_block_with_partition, metadata_snapshot, context, block_id, primary_txn_id);

            if (txn->isSecondary())
                temp_part->secondary_txn_id = txn->getTransactionID();
            if (part_log)
                part_log->addNewPart(context, temp_part, watch.elapsed());
            LOG_DEBUG(
                storage.getLogger(),
                "Write part {}, {} rows, elapsed {} ms",
                temp_part->name,
                bucketed_block_with_partition.block.rows(),
                watch.elapsedMilliseconds());
            parts.push_back(std::move(temp_part));
        }
    }

    return parts;
}

void CloudMergeTreeBlockOutputStream::writeSuffix()
{
    cnch_writer.finalize();
    auto & dumped_data = cnch_writer.res;

    if (!dumped_data.parts.empty())
    {
        preload_parts = std::move(dumped_data.parts);
    }

    if (!dumped_data.staged_parts.empty())
    {
        std::move(std::begin(dumped_data.staged_parts), std::end(dumped_data.staged_parts), std::back_inserter(preload_parts));
    }

    try
    {
        writeSuffixImpl();
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::INSERTION_LABEL_ALREADY_EXISTS)
        {
            LOG_DEBUG(storage.getLogger(), e.displayText());
            return;
        }
        throw;
    }
}

void CloudMergeTreeBlockOutputStream::writeSuffixImpl()
{
    cnch_writer.preload(preload_parts);

    if (!metadata_snapshot->hasUniqueKey() || to_staging_area)
    {
        /// case1(normal table): commit all the temp parts as visible parts
        /// case2(unique table with async insert): commit all the temp parts as staged parts,
        ///     which will be converted to visible parts later by dedup worker
        /// insert is lock-free and faster than upsert due to its simplicity.
        writeSuffixForInsert();
    }
    else
    {
        /// case(unique table with sync insert): acquire the necessary locks to avoid write-write conflicts
        /// and then remove duplicate keys between visible parts and temp parts.
        writeSuffixForUpsert();
    }
}

void CloudMergeTreeBlockOutputStream::writeSuffixForInsert()
{
    // Commit for insert values in server side.
    auto txn = context->getCurrentTransaction();
    if (dynamic_pointer_cast<CnchServerTransaction>(txn) && !disable_transaction_commit)
    {
        txn->setMainTableUUID(storage.getStorageUUID());
        txn->commitV2();
        LOG_DEBUG(storage.getLogger(), "Finishing insert values commit in cnch server.");
    }
    else if (auto worker_txn = dynamic_pointer_cast<CnchWorkerTransaction>(txn))
    {
        if (worker_txn->hasEnableExplicitCommit())
            return;

        auto kafka_table_id = txn->getKafkaTableID();
        if (!kafka_table_id.empty() && !worker_txn->hasEnableExplicitCommit())
        {
            txn->setMainTableUUID(UUIDHelpers::toUUID(storage.getSettings()->cnch_table_uuid.value));
            Stopwatch watch;
            txn->commitV2();
            LOG_TRACE(
                storage.getLogger(), "Committed Kafka transaction {} elapsed {} ms", txn->getTransactionID(), watch.elapsedMilliseconds());
        }
        else if (context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
        {
            /// INITIAL_QUERY means the query is sent from client (and to worker directly), so commit it instantly.
            Stopwatch watch;
            txn->commitV2();
            LOG_TRACE(
                storage.getLogger(),
                "Committed transaction {} elapsed {} ms.", txn->getTransactionID(), watch.elapsedMilliseconds());
        }
        else
        {
            /// TODO: I thought the multiple branches should be unified.
            /// And a exception should be threw in the last `else` clause, otherwise there might be some potential bugs.
        }
    }
}

namespace
{
    struct BlockUniqueKeyComparator
    {
        const ColumnsWithTypeAndName & keys;
        explicit BlockUniqueKeyComparator(const ColumnsWithTypeAndName & keys_) : keys(keys_) { }

        bool operator()(size_t lhs, size_t rhs) const
        {
            for (auto & key : keys)
            {
                int cmp = key.column->compareAt(lhs, rhs, *key.column, /*nan_direction_hint=*/1);
                if (cmp < 0)
                    return true;
                if (cmp > 0)
                    return false;
            }
            return false;
        }
    };

    /// Check whether all parts is the same table definition, otherwise we need to use normal lock instead of bucket lock.
    bool checkBucketParts(
        const MergeTreeMetaBase & storage,
        const MergeTreeDataPartsCNCHVector & visible_parts,
        const MergeTreeDataPartsCNCHVector & staged_parts)
    {
        if (!storage.getInMemoryMetadataPtr()->checkIfClusterByKeySameWithUniqueKey())
            return false;
        auto table_definition_hash = storage.getTableHashForClusterBy();
        auto checkIfBucketPartValid = [&table_definition_hash](const MergeTreeDataPartsCNCHVector & parts) -> bool {
            auto it = std::find_if(parts.begin(), parts.end(), [&](const auto & part) {
                return part->bucket_number == -1 || part->table_definition_hash != table_definition_hash;
            });
            return it == parts.end();
        };
        return checkIfBucketPartValid(visible_parts) && checkIfBucketPartValid(staged_parts);
    }

    CnchDedupHelper::DedupScope
    getDedupScope(const MergeTreeMetaBase & storage, const MutableMergeTreeDataPartsCNCHVector & preload_parts, bool force_normal_dedup)
    {
        auto checkIfUseBucketLock = [&]() -> bool {
            if (force_normal_dedup || !storage.getInMemoryMetadataPtr()->checkIfClusterByKeySameWithUniqueKey())
                return false;
            LOG_DEBUG(storage.getLogger(), "checkIfUseBucketLock: in");
            auto table_definition_hash = storage.getTableHashForClusterBy();
            /// Check whether all parts has same table_definition_hash.
            auto it = std::find_if(preload_parts.begin(), preload_parts.end(), [&](const auto & part) {
                return part->bucket_number == -1 || part->table_definition_hash != table_definition_hash;
            });
            return it == preload_parts.end();
        };
        LOG_DEBUG(storage.getLogger(), "checkIfUseBucketLock: {}", checkIfUseBucketLock());

        if (checkIfUseBucketLock())
        {
            if (storage.getSettings()->partition_level_unique_keys)
            {
                CnchDedupHelper::DedupScope::BucketWithPartitionSet bucket_with_partition_set;
                for (const auto & part : preload_parts)
                    bucket_with_partition_set.insert({part->info.partition_id, part->bucket_number});
                return CnchDedupHelper::DedupScope::PartitionDedupWithBucket(bucket_with_partition_set);
            }
            else
            {
                CnchDedupHelper::DedupScope::BucketSet buckets;
                for (const auto & part : preload_parts)
                    buckets.insert(part->bucket_number);
                return CnchDedupHelper::DedupScope::TableDedupWithBucket(buckets);
            }
        }
        else
        {
            /// acquire locks for all the written partitions
            NameOrderedSet sorted_partitions;
            for (const auto & part : preload_parts)
                sorted_partitions.insert(part->info.partition_id);

            return storage.getSettings()->partition_level_unique_keys ? CnchDedupHelper::DedupScope::PartitionDedup(sorted_partitions)
                                                                : CnchDedupHelper::DedupScope::TableDedup();
        }
    }
}

void CloudMergeTreeBlockOutputStream::writeSuffixForUpsert()
{
    auto txn = context->getCurrentTransaction();
    if (!txn)
        throw Exception("Transaction is not set", ErrorCodes::LOGICAL_ERROR);

    /// prefer to get cnch table uuid from settings as CloudMergeTree has no uuid for Kafka task
    String uuid_str = storage.getSettings()->cnch_table_uuid.value;
    if (uuid_str.empty())
        uuid_str = UUIDHelpers::UUIDToString(storage.getStorageUUID());

    txn->setMainTableUUID(UUIDHelpers::toUUID(uuid_str));
    if (auto worker_txn = dynamic_pointer_cast<CnchWorkerTransaction>(txn); worker_txn && !worker_txn->tryGetServerClient())
    {
        /// case: server initiated "insert select/infile" txn, need to set server client here in order to commit from worker
        if (const auto & client_info = context->getClientInfo(); client_info.rpc_port)
            worker_txn->setServerClient(context->getCnchServerClient(client_info.current_address.host().toString(), client_info.rpc_port));
        else
            throw Exception("Missing rpc_port, can't obtain server client to commit txn", ErrorCodes::LOGICAL_ERROR);
    }
    else
    {
        /// no need to set server client
        /// case: server initiated "insert values" txn, server client not required
        /// case: worker initiated "insert values|select|infile" txn, server client already set
    }

    auto catalog = context->getCnchCatalog();
    /// must use cnch table to construct staged parts.
    TxnTimestamp ts = context->getTimestamp();
    auto table = catalog->tryGetTableByUUID(*context, uuid_str, ts);
    if (!table)
        throw Exception("Table " + storage.getStorageID().getNameForLogs() + " has been dropped", ErrorCodes::ABORTED);
    auto cnch_table = dynamic_pointer_cast<StorageCnchMergeTree>(table);
    if (!cnch_table)
        throw Exception("Table " + storage.getStorageID().getNameForLogs() + " is not cnch merge tree", ErrorCodes::LOGICAL_ERROR);

    if (preload_parts.empty())
    {
        Stopwatch watch;
        txn->commitV2();
        LOG_INFO(
            log,
            "Committed transaction {} in {} ms, preload_parts is empty",
            txn->getTransactionID(),
            watch.elapsedMilliseconds(),
            preload_parts.size());
        return;
    }

    CnchLockHolderPtr cnch_lock;
    MergeTreeDataPartsCNCHVector visible_parts, staged_parts;
    bool force_normal_dedup = false;
    Stopwatch lock_watch;
    do
    {
        CnchDedupHelper::DedupScope scope = getDedupScope(storage, preload_parts, force_normal_dedup);

        std::vector<LockInfoPtr> locks_to_acquire = CnchDedupHelper::getLocksToAcquire(
            scope, txn->getTransactionID(), storage, storage.getSettings()->unique_acquire_write_lock_timeout.value.totalMilliseconds());
        lock_watch.restart();
        cnch_lock = txn->createLockHolder(std::move(locks_to_acquire));
        if (!cnch_lock->tryLock())
            throw Exception("Failed to acquire lock for txn " + txn->getTransactionID().toString(), ErrorCodes::CNCH_LOCK_ACQUIRE_FAILED);

        lock_watch.restart();
        ts = context->getTimestamp(); /// must get a new ts after locks are acquired
        visible_parts = CnchDedupHelper::getVisiblePartsToDedup(scope, *cnch_table, ts);
        staged_parts = CnchDedupHelper::getStagedPartsToDedup(scope, *cnch_table, ts);

        /// In some case, visible parts or staged parts doesn't have same bucket definition or not a bucket part, we need to convert bucket lock to normal lock.
        /// Otherwise, it may lead to duplicated data.
        if (scope.isBucketLock() && !checkBucketParts(storage, visible_parts, staged_parts))
        {
            force_normal_dedup = true;
            cnch_lock->unlock();
            LOG_TRACE(log, "Check bucket parts failed, switch to normal lock to dedup.");
            continue;
        }
        else
            break;
    } while (true);

    MergeTreeDataDeduper deduper(storage, context);
    LocalDeleteBitmaps bitmaps_to_dump = deduper.dedupParts(
        txn->getTransactionID(),
        CnchPartsHelper::toIMergeTreeDataPartsVector(visible_parts),
        CnchPartsHelper::toIMergeTreeDataPartsVector(staged_parts),
        {preload_parts.begin(), preload_parts.end()});

    Stopwatch watch;
    cnch_writer.publishStagedParts(staged_parts, bitmaps_to_dump);
    LOG_DEBUG(log, "Publishing staged parts take {} ms", watch.elapsedMilliseconds());

    watch.restart();
    txn->commitV2();
    LOG_INFO(
        log,
        "Committed transaction {} in {} ms (with {} ms holding lock)",
        txn->getTransactionID(),
        watch.elapsedMilliseconds(),
        lock_watch.elapsedMilliseconds());
}

CloudMergeTreeBlockOutputStream::FilterInfo CloudMergeTreeBlockOutputStream::dedupWithUniqueKey(const Block & block)
{
    if (!metadata_snapshot->hasUniqueKey())
        return FilterInfo{};

    const ColumnWithTypeAndName * version_column = nullptr;
    if (metadata_snapshot->hasUniqueKey() && storage.merging_params.hasExplicitVersionColumn())
        version_column = &block.getByName(storage.merging_params.version_column);

    Block block_copy = block;
    metadata_snapshot->getUniqueKeyExpression()->execute(block_copy);

    ColumnsWithTypeAndName keys;
    ColumnsWithTypeAndName string_keys;
    for (auto & name : metadata_snapshot->getUniqueKeyColumns())
    {
        auto & col = block_copy.getByName(name);
        keys.push_back(col);
        if (col.type->getTypeId() == TypeIndex::String)
            string_keys.push_back(col);
    }

    BlockUniqueKeyComparator comparator(keys);
    /// first rowid of key -> rowid of the last occurrence of the same key
    std::map<size_t, size_t, decltype(comparator)> index(comparator);

    auto block_size = block_copy.rows();
    FilterInfo res;
    res.filter.assign(block_size, UInt8(1));

    ColumnWithTypeAndName delete_flag_column;
    if (version_column && block.has(StorageInMemoryMetadata::DELETE_FLAG_COLUMN_NAME))
        delete_flag_column = block.getByName(StorageInMemoryMetadata::DELETE_FLAG_COLUMN_NAME);

    auto is_delete_row = [&](int rowid) { return delete_flag_column.column && delete_flag_column.column->getBool(rowid); };

    /// In the case that engine has been set version column, if version is set by user(not zero), the delete row will obey the rule of version.
    /// Otherwise, the delete row will ignore comparing version, just doing the deletion directly.
    auto delete_ignore_version
        = [&](int rowid) { return is_delete_row(rowid) && version_column && !version_column->column->getUInt(rowid); };

    /// If there are duplicated keys, only keep the last one
    for (size_t rowid = 0; rowid < block_size; ++rowid)
    {
        if (auto it = index.find(rowid); it != index.end())
        {
            /// When there is no explict version column, use rowid as version number,
            /// Otherwise use value from version column
            size_t old_pos = it->second;
            size_t new_pos = rowid;
            if (version_column && !delete_ignore_version(rowid)
                && version_column->column->getUInt(old_pos) > version_column->column->getUInt(new_pos))
                std::swap(old_pos, new_pos);

            res.filter[old_pos] = 0;
            it->second = new_pos;
            res.num_filtered++;
        }
        else
            index[rowid] = rowid;

        /// Check the length limit for string type.
        size_t unique_string_keys_size = 0;
        for (auto & key : string_keys)
            unique_string_keys_size += static_cast<const ColumnString &>(*key.column).getDataAt(rowid).size;
        if (unique_string_keys_size > context->getSettingsRef().max_string_size_for_unique_key)
            throw Exception("The size of unique string keys out of limit", ErrorCodes::UNIQUE_KEY_STRING_SIZE_LIMIT_EXCEEDED);
    }
    return res;
}

void CloudMergeTreeBlockOutputStream::writeImplicitColumnForBitEngine(
    BlockWithPartition & partition_block,
    const BitengineWriteSettings & write_settings)
{
    auto & block = partition_block.block;
    ColumnsWithTypeAndName columns = block.getColumnsWithTypeAndName();
    ColumnsWithTypeAndName encoded_columns;
    Names source_column_names;  /// used in a feature to discard source bitmap columns

    for (auto & column : columns)
    {
        if (!isBitmap64(column.type))
            continue;

        /// check whether the column is a legal BitEngine column in table
        if (!storage.isBitEngineEncodeColumn(column.name))
            continue;

        source_column_names.emplace_back(column.name);

        try
        {
            auto encoded_column = storage.getBitEngineDictionaryManager()->encodeColumn(
                    column, column.name, partition_block.bucket_info.bucket_number,
                    context, write_settings);

            encoded_columns.push_back(encoded_column);
        }
        catch(Exception & e)
        {
            // LOG_ERROR(&Logger::get("MergedBlockOutputStream"), "BitEngine encode column exception: " << e.message());
            // tryLogCurrentException(&Logger::get("MergedBlockOutputStream"), __PRETTY_FUNCTION__);
            throw Exception("BitEngine encode exception. reason: " + String(e.message()), ErrorCodes::LOGICAL_ERROR);
        }
    }

    /// add newly encoded column to the block
    /// only_recode is used for PREATTACH PARTITION, only source_column is read and only the encoded_column is written to disk
    /// and if only_recode = false, both source_column and encode_column will be written to disk
    if (!encoded_columns.empty())
    {
        for (auto & encoded_column : encoded_columns)
        {
            block.insertUnique(encoded_column);
        }
    }

    /// used for discard the source_column, we use an empty column to replace the original source_column
    /// and there's an empty column on the disk, in this case, only_recode = false
    if (!encoded_columns.empty())
    {
        auto replace_source_column = [&](const String & col_name) {
            size_t rows = block.rows();
            ColumnWithTypeAndName & col = block.getByName(col_name);
            auto new_column = col.type->createColumn();
            new_column->insertManyDefaults(rows);
            col.column = std::move(new_column);
        };

        if (storage.getSettings()->bitengine_discard_source_bitmap)
        {
            std::for_each(source_column_names.begin(), source_column_names.end(),
                replace_source_column);
        }
        else
        {
            for (auto & name : source_column_names)
            {
                if (storage.getBitEngineDictionaryManager()->isKeyStringDictioanry(name))
                    replace_source_column(name);
            }
        }
    }
}
}
