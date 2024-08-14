#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include <iostream>
#include <iostream>

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalProjection &op) {
	D_ASSERT(op.children.size() == 1);
	std::cout << "PhysicalPlanGenerator::CreatePlan : Logical Projection" << std::endl;
	auto plan = CreatePlan(*op.children[0]);

#ifdef DEBUG
	for (auto &expr : op.expressions) {
		D_ASSERT(!expr->IsWindow());
		D_ASSERT(!expr->IsAggregate());
	}
#endif
	std::cout << "PhysicalPlanGenerator::CreatePlan plan->types.size() == op.types.size() : " << plan->types.size() << "\t" << op.types.size() << std::endl;
	if (plan->types.size() == op.types.size()) {
		// check if this projection can be omitted entirely
		// this happens if a projection simply emits the columns in the same order
		// e.g. PROJECTION(#0, #1, #2, #3, ...)
		bool omit_projection = true;
		for (idx_t i = 0; i < op.types.size(); i++) {
			if (op.expressions[i]->type == ExpressionType::BOUND_REF) {
				auto &bound_ref = op.expressions[i]->Cast<BoundReferenceExpression>();
				std::cout << "bound ref expression.index : " << bound_ref.index << std::endl;
				if (bound_ref.index == i) {
					continue;
				}
			}
			omit_projection = false;
			break;
		}
		if (omit_projection) {
			// the projection only directly projects the child' columns: omit it entirely
			return plan;
		}
	}

	auto projection = make_uniq<PhysicalProjection>(op.types, std::move(op.expressions), op.estimated_cardinality);
	projection->children.push_back(std::move(plan));
	return std::move(projection);
}

} // namespace duckdb
