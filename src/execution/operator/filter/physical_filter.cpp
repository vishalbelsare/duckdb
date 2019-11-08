#include "execution/operator/filter/physical_filter.hpp"

#include "execution/expression_executor.hpp"

#include <fstream>

using namespace duckdb;
using namespace std;

void PhysicalFilter::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalFilterOperatorState *>(state_);
	//update metrics
	state->expr_executor.calls_to_get_chunk++;
	do {
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (state->child_chunk.size() == 0) {
			return;
		}

		assert(expressions.size() > 0);

		//required for metrics
		if (expressions.size() != state->expr_executor.selectivity_count.size()) {
			state->expr_executor.selectivity_count.resize(expressions.size());
			state->expr_executor.execution_count.resize(expressions.size());
		}

		if (expressions.size() > 1) {
			// switch between execution and exploration phase
			if (!state->expr_executor.exploration_phase) {
				// execution phase
				if (state->expr_executor.count == 100) {
					//end
					state->expr_executor.exploration_phase = true;
					state->expr_executor.count = 0;
				}
			} else {
				// exploration phase
				if (state->expr_executor.count == expressions.size()) {
					// end
					state->expr_executor.exploration_phase = false;
					state->expr_executor.count = 0;
					state->expr_executor.GetNewPermutation();
				}
			}
		}

		state->expr_executor.chunk = &(state->child_chunk);
		//update metrics
		state->expr_executor.calls_to_merge++;
		state->expr_executor.Merge(expressions);

		if (state->child_chunk.size() != 0) {
			// chunk gets the same selection vector as its child chunk
			chunk.sel_vector = state->child_chunk.sel_vector;
			for (index_t i = 0; i < chunk.column_count; i++) {
				// create a reference to the vector of the child chunk, same number of columns
				chunk.data[i].Reference(state->child_chunk.data[i]);
			}
			// chunk gets the same data as child chunk
			for (index_t i = 0; i < chunk.column_count; i++) {
				chunk.data[i].count = state->child_chunk.data[i].count;
				chunk.data[i].sel_vector = state->child_chunk.sel_vector;
			}
		}

		state->expr_executor.count++;

	} while (chunk.size() == 0);
}

string PhysicalFilter::ExtraRenderInformation() const {
	string extra_info;
	for (auto &expr : expressions) {
		extra_info += expr->GetName() + "\n";
	}
	return extra_info;
}

unique_ptr<PhysicalOperatorState> PhysicalFilter::GetOperatorState() {
	return make_unique<PhysicalFilterOperatorState>(children[0].get());
}

PhysicalFilterOperatorState::~PhysicalFilterOperatorState() {
	//NOTE: if enabled, a metrics.txt file will be created
	/*
	ofstream metrics("metrics.txt");
	metrics << "Calls to GetChunk: " << expr_executor.calls_to_get_chunk << endl;
	metrics << "Calls to Merge: " << expr_executor.calls_to_merge << endl;
	metrics << "Execution count:" << endl;
	for (const auto& item : expr_executor.execution_count) {
		metrics << item << endl;
	}
	metrics << "Selectivity count:" << endl;
	index_t sum = 0;
	for (const auto& item : expr_executor.selectivity_count) {
		metrics << item << endl;
		sum += item;
	}
	metrics << "Sum: " << sum << endl;
	metrics << "Permutations: " << endl;
	for (const auto& item : expr_executor.permutations) {
		for (const auto& elem : item) {
			metrics << elem << ", ";
		}
		metrics << endl;
	}
	metrics.close();
	*/
}
