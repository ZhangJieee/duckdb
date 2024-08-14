#include "duckdb/execution/operator/helper/physical_result_collector.hpp"

#include "duckdb/execution/operator/helper/physical_batch_collector.hpp"
#include "duckdb/execution/operator/helper/physical_materialized_collector.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

PhysicalResultCollector::PhysicalResultCollector(PreparedStatementData &data)
    : PhysicalOperator(PhysicalOperatorType::RESULT_COLLECTOR, {LogicalType::BOOLEAN}, 0),
      statement_type(data.statement_type), properties(data.properties), plan(*data.plan), names(data.names) {
	this->types = data.types;
}

unique_ptr<PhysicalResultCollector> PhysicalResultCollector::GetResultCollector(ClientContext &context,
                                                                                PreparedStatementData &data) {
	if (!PhysicalPlanGenerator::PreserveInsertionOrder(context, *data.plan)) {
		std::cout << "construct PhysicalMaterializedCollector Pipeline Sink true" << std::endl;
		// the plan is not order preserving, so we just use the parallel materialized collector
		return make_uniq_base<PhysicalResultCollector, PhysicalMaterializedCollector>(data, true);
	} else if (!PhysicalPlanGenerator::UseBatchIndex(context, *data.plan)) {
		std::cout << "construct PhysicalMaterializedCollector Pipeline Sink false" << std::endl;
		// the plan is order preserving, but we cannot use the batch index: use a single-threaded result collector
		return make_uniq_base<PhysicalResultCollector, PhysicalMaterializedCollector>(data, false);
	} else {
		std::cout << "construct PhysicalBatchCollector Pipeline Sink" << std::endl;
		// we care about maintaining insertion order and the sources all support batch indexes
		// use a batch collector
		return make_uniq_base<PhysicalResultCollector, PhysicalBatchCollector>(data);
	}
}

vector<const_reference<PhysicalOperator>> PhysicalResultCollector::GetChildren() const {
	return {plan};
}

void PhysicalResultCollector::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	// operator is a sink, build a pipeline
	sink_state.reset();

	D_ASSERT(children.empty());

	// single operator: the operator becomes the data source of the current pipeline
	auto &state = meta_pipeline.GetState();
	// 设置 Pipeline 的数据source,数据从source获取，流向sink(就是该函数的入参 - current)
	state.SetPipelineSource(current, *this);

	// we create a new pipeline starting from the child
	// 创建Pipeline (data source) 到 Pipeline (data sink)中,作为其的child
	auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
	// CreatePlan(LogicalProjection&) return => (PhysicalTableScan*)plan
	std::cout << "PhysicalResultCollector : " << int(plan.type) << std::endl;
	child_meta_pipeline.Build(plan);
}

} // namespace duckdb
