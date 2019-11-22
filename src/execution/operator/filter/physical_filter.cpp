#include "duckdb/execution/operator/filter/physical_filter.hpp"

#include "duckdb/execution/expression_executor.hpp"

#include <fstream>
#include <iostream>

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

		if (expressions.size() != state->expr_executor.selectivity_count.size()) {
			//required for metrics
			state->expr_executor.selectivity_count.resize(expressions.size());
			state->expr_executor.execution_count.resize(expressions.size());

			//initialize factorial and default permutation
			state->expr_executor.expr_size_factorial = 1;
			for (index_t i = 1; i <= expressions.size(); i++) {
				state->expr_executor.expr_size_factorial *= i;
				state->expr_executor.rank_0_permutation.permutation.push_back(i - 1);
			}
		}

		//update metrics
		state->expr_executor.calls_to_merge++;

		//execute expressions
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
	//NOTE: if enabled, metrics will be printed to std::cout
	/*
	std::cout << "Calls to GetChunk: " << expr_executor.calls_to_get_chunk << endl;
	std::cout << "Calls to Merge: " << expr_executor.calls_to_merge << endl;
	std::cout << "Execution count:" << endl;
	for (const auto& item : expr_executor.execution_count) {
		std::cout << item << endl;
	}
	std::cout << "Selectivity count:" << endl;
	index_t sum = 0;
	for (const auto& item : expr_executor.selectivity_count) {
		std::cout << item << endl;
		sum += item;
	}
	std::cout << "Sum: " << sum << endl;
	std::cout << "Permutations: " << endl;
	for (const auto& item : expr_executor.permutations) {
		for (const auto& elem : item) {
			std::cout << elem << ", ";
		}
		std::cout << endl;
	}
	*/
}
