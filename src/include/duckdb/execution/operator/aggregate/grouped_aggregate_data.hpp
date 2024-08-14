//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/aggregate/grouped_aggregate_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/parser/group_by_node.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

class GroupedAggregateData {
public:
	GroupedAggregateData() {
	}
	// group by中的列表达式
	//! The groups
	vector<unique_ptr<Expression>> groups;
	//! The set of GROUPING functions
	vector<vector<idx_t>> grouping_functions;
	// 分组列类型
	//! The group types
	vector<LogicalType> group_types;

	// 保存select list 中的聚合表达式
	//! The aggregates that have to be computed
	vector<unique_ptr<Expression>> aggregates;
	// 保存聚合函数中的列类型
	//! The payload types
	vector<LogicalType> payload_types;
	// 保存聚合函数的返回类型
	//! The aggregate return types
	vector<LogicalType> aggregate_return_types;
	// 保存表达式实际类型的指针
	//! Pointers to the aggregates
	vector<BoundAggregateExpression *> bindings;
	// 过滤条件的统计
	idx_t filter_count;

public:
	idx_t GroupCount() const;

	const vector<vector<idx_t>> &GetGroupingFunctions() const;

	void InitializeGroupby(vector<unique_ptr<Expression>> groups, vector<unique_ptr<Expression>> expressions,
	                       vector<vector<idx_t>> grouping_functions);

	//! Initialize a GroupedAggregateData object for use with distinct aggregates
	void InitializeDistinct(const unique_ptr<Expression> &aggregate, const vector<unique_ptr<Expression>> *groups_p);

private:
	void InitializeDistinctGroups(const vector<unique_ptr<Expression>> *groups);
	void InitializeGroupbyGroups(vector<unique_ptr<Expression>> groups);
	void SetGroupingFunctions(vector<vector<idx_t>> &functions);
};

} // namespace duckdb
