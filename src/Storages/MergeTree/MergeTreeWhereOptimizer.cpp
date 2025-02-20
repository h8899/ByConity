/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include <Storages/MergeTree/MergeTreeWhereOptimizer.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/formatAST.h>
#include <Interpreters/misc.h>
#include <Common/typeid_cast.h>
#include <DataTypes/NestedUtils.h>
#include <common/map.h>

#include <Interpreters/PartitionPredicateVisitor.h>
#include <Interpreters/TreeRewriter.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/SymbolsExtractor.h>
#include <Optimizer/Utils.h>
#include <Parsers/queryToString.h>
#include <QueryPlan/SymbolMapper.h>

namespace DB
{

namespace ErrorCodes
{
}

/// Conditions like "x = N" are considered good if abs(N) > threshold.
/// This is used to assume that condition is likely to have good selectivity.
static constexpr auto threshold = 2;


MergeTreeWhereOptimizer::MergeTreeWhereOptimizer(
    SelectQueryInfo & query_info,
    ContextPtr context,
    std::unordered_map<std::string, UInt64> column_sizes_,
    const StorageMetadataPtr & metadata_snapshot_,
    const Names & queried_columns_,
    Poco::Logger * log_)
    : table_columns{collections::map<std::unordered_set>(
        metadata_snapshot_->getColumns().getAllPhysical(), [](const NameAndTypePair & col) { return col.name; })}
    , queried_columns{queried_columns_}
    , sorting_key_names{NameSet(
          metadata_snapshot_->getSortingKey().column_names.begin(), metadata_snapshot_->getSortingKey().column_names.end())}
    , block_with_constants{KeyCondition::getBlockWithConstants(query_info.query->clone(), query_info.syntax_analyzer_result, context)}
    , log{log_}
    , column_sizes{std::move(column_sizes_)}
    , metadata_snapshot{metadata_snapshot_}
{
    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    if (!primary_key.column_names.empty())
        first_primary_key_column = primary_key.column_names[0];

    for (const auto & name : queried_columns)
    {
        auto it = column_sizes.find(name);
        if (it != column_sizes.end())
            total_size_of_queried_columns += it->second;
    }

    determineArrayJoinedNames(query_info.query->as<ASTSelectQuery &>());
    optimize(query_info.query->as<ASTSelectQuery &>());
}


static void collectIdentifiersNoSubqueries(const ASTPtr & ast, NameSet & set)
{
    if (auto opt_name = tryGetIdentifierName(ast))
        return (void)set.insert(*opt_name);

    if (ast->as<ASTSubquery>())
        return;

    for (const auto & child : ast->children)
        collectIdentifiersNoSubqueries(child, set);
}

static bool isConditionGood(const ASTPtr & condition)
{
    const auto * function = condition->as<ASTFunction>();
    if (!function)
        return false;

    /** we are only considering conditions of form `equals(one, another)` or `one = another`,
        * especially if either `one` or `another` is ASTIdentifier */
    if (function->name != "equals")
        return false;

    auto * left_arg = function->arguments->children.front().get();
    auto * right_arg = function->arguments->children.back().get();

    /// try to ensure left_arg points to ASTIdentifier
    if (!left_arg->as<ASTIdentifier>() && right_arg->as<ASTIdentifier>())
        std::swap(left_arg, right_arg);

    if (left_arg->as<ASTIdentifier>())
    {
        /// condition may be "good" if only right_arg is a constant and its value is outside the threshold
        if (const auto * literal = right_arg->as<ASTLiteral>())
        {
            const auto & field = literal->value;
            const auto type = field.getType();

            /// check the value with respect to threshold
            if (type == Field::Types::UInt64)
            {
                const auto value = field.get<UInt64>();
                return value > threshold;
            }
            else if (type == Field::Types::Int64)
            {
                const auto value = field.get<Int64>();
                return value < -threshold || threshold < value;
            }
            else if (type == Field::Types::Float64)
            {
                const auto value = field.get<Float64>();
                return value < threshold || threshold < value;
            }
        }
    }

    return false;
}


void MergeTreeWhereOptimizer::analyzeImpl(Conditions & res, const ASTPtr & node, bool is_final) const
{
    if (const auto * func_and = node->as<ASTFunction>(); func_and && func_and->name == "and")
    {
        for (const auto & elem : func_and->arguments->children)
            analyzeImpl(res, elem, is_final);
    }
    else
    {
        Condition cond;
        cond.node = node;

        collectIdentifiersNoSubqueries(node, cond.identifiers);

        cond.columns_size = getIdentifiersColumnSize(cond.identifiers);

        cond.viable =
            /// Condition depend on some column. Constant expressions are not moved.
            !cond.identifiers.empty()
            && !cannotBeMoved(node, is_final)
            /// Do not take into consideration the conditions consisting only of the first primary key column
            && !hasPrimaryKeyAtoms(node)
            /// Only table columns are considered. Not array joined columns. NOTE We're assuming that aliases was expanded.
            && isSubsetOfTableColumns(cond.identifiers)
            /// Do not move conditions involving all queried columns.
            && cond.identifiers.size() < queried_columns.size();

        if (cond.viable)
            cond.good = isConditionGood(node);

        res.emplace_back(std::move(cond));
    }
}

/// Transform conjunctions chain in WHERE expression to Conditions list.
MergeTreeWhereOptimizer::Conditions MergeTreeWhereOptimizer::analyze(const ASTPtr & expression, bool is_final) const
{
    Conditions res;
    analyzeImpl(res, expression, is_final);
    return res;
}

/// Transform Conditions list to WHERE or PREWHERE expression.
ASTPtr MergeTreeWhereOptimizer::reconstruct(const Conditions & conditions)
{
    if (conditions.empty())
        return {};

    if (conditions.size() == 1)
        return conditions.front().node;

    const auto function = std::make_shared<ASTFunction>();

    function->name = "and";
    function->arguments = std::make_shared<ASTExpressionList>();
    function->children.push_back(function->arguments);

    for (const auto & elem : conditions)
        function->arguments->children.push_back(elem.node);

    return function;
}

void MergeTreeWhereOptimizer::optimize(ASTSelectQuery & select) const
{
    if (!select.where() || select.prewhere())
        return;

    Conditions where_conditions = analyze(select.where(), select.final());
    Conditions prewhere_conditions;

    UInt64 total_size_of_moved_conditions = 0;
    UInt64 total_number_of_moved_columns = 0;

    /// Move condition and all other conditions depend on the same set of columns.
    auto move_condition = [&](Conditions::iterator cond_it)
    {
        prewhere_conditions.splice(prewhere_conditions.end(), where_conditions, cond_it);
        total_size_of_moved_conditions += cond_it->columns_size;
        total_number_of_moved_columns += cond_it->identifiers.size();

        /// Move all other viable conditions that depend on the same set of columns.
        for (auto jt = where_conditions.begin(); jt != where_conditions.end();)
        {
            if (jt->viable && jt->columns_size == cond_it->columns_size && jt->identifiers == cond_it->identifiers)
                prewhere_conditions.splice(prewhere_conditions.end(), where_conditions, jt++);
            else
                ++jt;
        }
    };

    /// Move conditions unless the ratio of total_size_of_moved_conditions to the total_size_of_queried_columns is less than some threshold.
    while (!where_conditions.empty())
    {
        /// Move the best condition to PREWHERE if it is viable.

        auto it = std::min_element(where_conditions.begin(), where_conditions.end());

        if (!it->viable)
            break;

        bool moved_enough = false;
        if (total_size_of_queried_columns > 0)
        {
            /// If we know size of queried columns use it as threshold. 10% ratio is just a guess.
            moved_enough = total_size_of_moved_conditions > 0
                && (total_size_of_moved_conditions + it->columns_size) * 10 > total_size_of_queried_columns;
        }
        else
        {
            /// Otherwise, use number of moved columns as a fallback.
            /// It can happen, if table has only compact parts. 25% ratio is just a guess.
            moved_enough = total_number_of_moved_columns > 0
                && (total_number_of_moved_columns + it->identifiers.size()) * 4 > queried_columns.size();
        }

        if (moved_enough)
            break;
        move_condition(it);
    }

    /// Nothing was moved.
    if (prewhere_conditions.empty())
        return;

    /// Rewrite the SELECT query.

    select.setExpression(ASTSelectQuery::Expression::WHERE, reconstruct(where_conditions));
    select.setExpression(ASTSelectQuery::Expression::PREWHERE, reconstruct(prewhere_conditions));

    LOG_DEBUG(log, "MergeTreeWhereOptimizer: condition \"{}\" moved to PREWHERE", select.prewhere());
}


UInt64 MergeTreeWhereOptimizer::getIdentifiersColumnSize(const NameSet & identifiers) const
{
    UInt64 size = 0;

    for (const auto & identifier : identifiers)
        if (column_sizes.count(identifier))
            size += column_sizes.at(identifier);

    return size;
}


bool MergeTreeWhereOptimizer::hasPrimaryKeyAtoms(const ASTPtr & ast) const
{
    if (const auto * func = ast->as<ASTFunction>())
    {
        const auto & args = func->arguments->children;

        if ((func->name == "not" && 1 == args.size()) || func->name == "and" || func->name == "or")
        {
            for (const auto & arg : args)
                if (hasPrimaryKeyAtoms(arg))
                    return true;

            return false;
        }
    }

    return isPrimaryKeyAtom(ast);
}


bool MergeTreeWhereOptimizer::isPrimaryKeyAtom(const ASTPtr & ast) const
{
    if (const auto * func = ast->as<ASTFunction>())
    {
        if (!KeyCondition::atom_map.count(func->name))
            return false;

        const auto & args = func->arguments->children;
        if (functionIsEscapeLikeOperator(func->name))
        {
            if (args.size() != 3)
                return false;
        }
        else {
            if (args.size() != 2)
                return false;
        }

        const auto & first_arg_name = args.front()->getColumnName();
        const auto & second_arg_name = args.back()->getColumnName();

        if ((first_primary_key_column == first_arg_name && isConstant(args[1]))
            || (first_primary_key_column == second_arg_name && isConstant(args[0]))
            || (first_primary_key_column == first_arg_name && functionIsInOrGlobalInOperator(func->name)))
            return true;
    }

    return false;
}


bool MergeTreeWhereOptimizer::isSortingKey(const String & column_name) const
{
    return sorting_key_names.count(column_name);
}


bool MergeTreeWhereOptimizer::isConstant(const ASTPtr & expr) const
{
    const auto column_name = expr->getColumnName();

    return expr->as<ASTLiteral>()
        || (block_with_constants.has(column_name) && isColumnConst(*block_with_constants.getByName(column_name).column));
}


bool MergeTreeWhereOptimizer::isSubsetOfTableColumns(const NameSet & identifiers) const
{
    for (const auto & identifier : identifiers)
        if (table_columns.count(identifier) == 0)
            return false;

    return true;
}


bool MergeTreeWhereOptimizer::cannotBeMoved(const ASTPtr & ptr, bool is_final) const
{
    if (const auto * function_ptr = ptr->as<ASTFunction>())
    {
        /// disallow arrayJoin expressions to be moved to PREWHERE for now
        if ("arrayJoin" == function_ptr->name)
            return true;

        /// disallow GLOBAL IN, GLOBAL NOT IN
        if ("globalIn" == function_ptr->name
            || "globalNotIn" == function_ptr->name)
            return true;

        /// indexHint is a special function that it does not make sense to transfer to PREWHERE
        if ("indexHint" == function_ptr->name)
            return true;
    }
    else if (auto opt_name = IdentifierSemantic::getColumnName(ptr))
    {
        /// disallow moving result of ARRAY JOIN to PREWHERE
        if (array_joined_names.count(*opt_name) ||
            array_joined_names.count(Nested::extractTableName(*opt_name)) ||
            (is_final && !isSortingKey(*opt_name)))
            return true;
    }

    for (const auto & child : ptr->children)
        if (cannotBeMoved(child, is_final))
            return true;

    return false;
}


void MergeTreeWhereOptimizer::determineArrayJoinedNames(ASTSelectQuery & select)
{
    auto array_join_expression_list = select.arrayJoinExpressionList();

    /// much simplified code from ExpressionAnalyzer::getArrayJoinedColumns()
    if (!array_join_expression_list)
        return;

    for (const auto & ast : array_join_expression_list->children)
        array_joined_names.emplace(ast->getAliasOrColumnName());
}

void optimizePartitionPredicate(ASTPtr & query, StoragePtr storage, SelectQueryInfo & query_info, ContextPtr context)
{
    ASTSelectQuery * select = query->as<ASTSelectQuery>();
    if (!select || !select->where() || query_info.partition_filter || !storage)
        return;

    if (!dynamic_cast<MergeTreeData *>(storage.get()))
        return;

    ASTs conjuncts = PredicateUtils::extractConjuncts(select->where()->clone());

    // construct push filter by mapping symbol to origin column
    std::unordered_map<String, String> column_to_alias;
    for (const auto & item : query_info.syntax_analyzer_result->aliases)
        column_to_alias.emplace(item.second->getColumnName(), item.first);
    auto alias_to_column = Utils::reverseMap(column_to_alias);
    ASTPtr push_filter;
    if (!alias_to_column.empty())
    {
        auto mapper = SymbolMapper::simpleMapper(alias_to_column);
        std::vector<ConstASTPtr> mapped_pushable_conjuncts;
        for (auto & conjunct : conjuncts)
        {
            bool all_in = true;
            auto symbols = SymbolsExtractor::extract(conjunct);
            for (const auto & item : symbols)
                all_in &= alias_to_column.contains(item);
            if (all_in)
               mapped_pushable_conjuncts.push_back(mapper.map(conjunct));
            else
               mapped_pushable_conjuncts.push_back(conjunct);
        }

        push_filter = PredicateUtils::combineConjuncts(mapped_pushable_conjuncts);
    }
    if (!PredicateUtils::isTruePredicate(push_filter))
    {
        if (auto * merge_tree_data = dynamic_cast<MergeTreeData *>(storage.get()))
        {
            ASTs push_predicates;
            ASTs remain_predicates;
            Names partition_key_names = merge_tree_data->getInMemoryMetadataPtr()->getPartitionKey().column_names;
            Names virtual_key_names = merge_tree_data->getSampleBlockWithVirtualColumns().getNames();
            partition_key_names.insert(partition_key_names.end(), virtual_key_names.begin(), virtual_key_names.end());
            auto iter = std::stable_partition(conjuncts.begin(), conjuncts.end(), [&](const auto & predicate) {
                PartitionPredicateVisitor::Data visitor_data{context, partition_key_names};
                PartitionPredicateVisitor(visitor_data).visit(predicate);
                return visitor_data.getMatch();
            });

            push_predicates.insert(push_predicates.end(), conjuncts.begin(), iter);
            remain_predicates.insert(remain_predicates.end(), iter, conjuncts.end());

            ASTPtr new_partition_filter;

            if (query_info.partition_filter)
            {
                push_predicates.push_back(query_info.partition_filter);
                new_partition_filter = PredicateUtils::combineConjuncts(push_predicates);
            }
            else
            {
                new_partition_filter = PredicateUtils::combineConjuncts<false>(push_predicates);
            }

            if (!PredicateUtils::isTruePredicate(new_partition_filter))
                query_info.partition_filter = std::move(new_partition_filter);

            ASTPtr new_where = PredicateUtils::combineConjuncts<false>(remain_predicates);
            if (!PredicateUtils::isTruePredicate(new_where))
                select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(new_where));
            else
                select->setExpression(ASTSelectQuery::Expression::WHERE, nullptr);
            query_info.query = query;
        }
    }
    if (query_info.partition_filter)
    {
        LOG_TRACE(&Poco::Logger::get("optimizePartitionPredicate"), "Optimize partition prediate push down query rewrited to {} , partiton filter-{} ",
                queryToString(query), queryToString(query_info.partition_filter));
    }
}
}
