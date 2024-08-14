#include "duckdb/parallel/pipeline_complete_event.hpp"
#include "duckdb/execution/executor.hpp"
#include <iostream>
namespace duckdb {

PipelineCompleteEvent::PipelineCompleteEvent(Executor &executor, bool complete_pipeline_p)
    : Event(executor), complete_pipeline(complete_pipeline_p) {
}

void PipelineCompleteEvent::Schedule() {
}

void PipelineCompleteEvent::FinalizeFinish() {
	std::cout << "PipelineCompleteEvent::FinalizeFinish complete_pipeline : " << complete_pipeline << std::endl;
	if (complete_pipeline) {
		executor.CompletePipeline();
	}
}

} // namespace duckdb
