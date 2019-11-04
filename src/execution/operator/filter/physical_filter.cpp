#include "execution/operator/filter/physical_filter.hpp"

#include "execution/expression_executor.hpp"

using namespace duckdb;
using namespace std;

void PhysicalFilter::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalFilterOperatorState *>(state_);
	do {
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (state->child_chunk.size() == 0) {
			return;
		}

		assert(expressions.size() > 0);

		if (expressions.size() > 1) {
			// switch between execution and exploration phase
			if (!state->expr_executor.exploration_phase) {
				// execution phase
				if (state->expr_executor.count == 100) {
					//end
					state->expr_executor.exploration_phase = true;
					state->expr_executor.count = 0;
					state->expr_executor.expr_runtimes.clear();
					state->expr_executor.expr_selectivity.clear();
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
