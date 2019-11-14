#include "execution/expression_executor.hpp"

#include "common/types/static_vector.hpp"
#include "common/vector_operations/vector_operations.hpp"

#include <chrono>

using namespace duckdb;
using namespace std;

ExpressionExecutor::ExpressionExecutor() : chunk(nullptr) {
	count = 0;
	exploration_phase = true;
	calls_to_merge = 0;
	calls_to_get_chunk = 0;
	score = 0.0;
	threshold = 0.05;
}

ExpressionExecutor::ExpressionExecutor(DataChunk *child_chunk) : chunk(child_chunk) {
}

ExpressionExecutor::ExpressionExecutor(DataChunk &child_chunk) : chunk(&child_chunk) {
}

void ExpressionExecutor::GetNewPermutation() {
	int16_t old_perm = permutations.size() - 1;
	current_perm.clear();
	while (!scores.empty()) {
		current_perm.push_back(scores.top().idx);
		scores.pop();
    }
	permutations.push_back(current_perm);
	//adapt threshold if permutation did not change
	if (old_perm >= 0) {
		bool adapt_threshold = true;
		for (index_t i = 0; i < permutations[old_perm].size(); i++) {
			if ((permutations[old_perm])[i] != (permutations[old_perm + 1])[i]) {
				adapt_threshold = false;
				break;
			}
		}
		if (adapt_threshold) {
			threshold = (threshold + change_percentage) / 2;
		}
	}
}

void ExpressionExecutor::Execute(vector<unique_ptr<Expression>> &expressions, DataChunk &result) {
	assert(expressions.size() == result.column_count);
	assert(expressions.size() > 0);
	for (index_t i = 0; i < expressions.size(); i++) {
		ExecuteExpression(*expressions[i], result.data[i]);
		result.heap.MergeHeap(result.data[i].string_heap);
	}
	result.sel_vector = result.data[0].sel_vector;
	result.Verify();
}

void ExpressionExecutor::Execute(vector<Expression *> &expressions, DataChunk &result) {
	assert(expressions.size() == result.column_count);
	assert(expressions.size() > 0);
	for (index_t i = 0; i < expressions.size(); i++) {
		ExecuteExpression(*expressions[i], result.data[i]);
		result.heap.MergeHeap(result.data[i].string_heap);
	}
	result.sel_vector = result.data[0].sel_vector;
	result.Verify();
}

void ExpressionExecutor::Merge(vector<unique_ptr<Expression>> &expressions) {
	assert(expressions.size() > 0);

	//if there is only one expression, adaptive statistics are not needed
	if (expressions.size() == 1) {
		Vector intermediate;
		Execute(*expressions[0], intermediate);
		assert(intermediate.type == TypeId::BOOLEAN);
		//if constant and false/null, set count == 0 to fetch the next chunk
		if (intermediate.IsConstant()) {
			if (!intermediate.data[0] || intermediate.nullmask[0]) {
				chunk->data[0].count = 0;
			}
		} else {
			chunk->SetSelectionVector(intermediate);
		}
	} else {
		//more than one expression, apply adaptive algorithm
		index_t start_idx = 0;
		index_t end_idx = current_perm.size();
		index_t selectivity = 0;
		chrono::time_point<chrono::high_resolution_clock> start_time;
		chrono::time_point<chrono::high_resolution_clock> end_time;

		if (exploration_phase) {
			start_idx = count;
			end_idx = count + expressions.size();
		}

		//evaluate all expressions
		for (index_t i = start_idx; i < end_idx; i++) {
			//return if no more true rows
			if (chunk->size() != 0) {
				Vector intermediate;

				//evaluation of current expression
				if (exploration_phase) {
					//update metrics
					selectivity_count[i % expressions.size()] += chunk->data[0].count;
					execution_count[i % expressions.size()]++;
					selectivity += chunk->data[0].count;
					if (i == start_idx) {
						start_time = chrono::high_resolution_clock::now();
						Execute(*expressions[i % expressions.size()], intermediate);
						end_time = chrono::high_resolution_clock::now();
					} else {
						Execute(*expressions[i % expressions.size()], intermediate);
					}
				} else {
					//update metrics
					selectivity_count[current_perm[i]] += chunk->data[0].count;
					execution_count[current_perm[i]]++;
					selectivity += chunk->data[0].count;
					Execute(*expressions[current_perm[i]], intermediate);
				}

				assert(intermediate.type == TypeId::BOOLEAN);
				//if constant and false/null, set count == 0 to fetch the next chunk
				if (intermediate.IsConstant()) {
					if (!intermediate.data[0] || intermediate.nullmask[0]) {
						chunk->data[0].count = 0;
						if (exploration_phase && i == start_idx) {
							scores.push({i % expressions.size(), 0.0});
						}
						break;
					}
				} else {
					chunk->SetSelectionVector(intermediate);
					if (exploration_phase && i == start_idx) {
						auto runtime = chrono::duration_cast<chrono::duration<double>>(end_time - start_time).count();
						scores.push({i % expressions.size(), (double)chunk->size() * runtime});
					}
				}
			} else {
				break;
			}
		}

		if (exploration_phase) {
			if (count == expressions.size() - 1) {
				GetNewPermutation();
				exploration_phase = false;
				count = 0;
				// get the iteration of the next random exploration phase
				srand(time(nullptr));
				random_explore = 200 + rand() / (RAND_MAX / (200));
			} else {
				count++;
			}
		} else {
			auto normed_sel = (double)selectivity / (STANDARD_VECTOR_SIZE * expressions.size());
			if (count >= expressions.size() * 4 && count < random_explore) {
				auto mean = score / count;
				change_percentage = abs(normed_sel - mean) / mean;
				if (change_percentage > threshold) {
					exploration_phase = true;
					score = 0.0;
					count = 0;
				} else {
					score += normed_sel;
					count++;
				}
			} else if (count == random_explore) {
				// trigger random exploration phases to detect changes in runtime
				exploration_phase = true;
				score = 0.0;
				count = 0;
			} else {
				score += normed_sel;
				count++;
			}
		}
	}
}

void ExpressionExecutor::ExecuteExpression(Expression &expr, Vector &result) {
	Vector vector;
	Execute(expr, vector);
	if (chunk) {
		// we have an input chunk: result of this vector should have the same length as input chunk
		// check if the result is a single constant value
		if (vector.count == 1 && (chunk->size() > 1 || vector.sel_vector != chunk->sel_vector)) {
			// have to duplicate the constant value to match the rows in the
			// other columns
			result.count = chunk->size();
			result.sel_vector = chunk->sel_vector;
			VectorOperations::Set(result, vector.GetValue(0));
			result.Move(vector);
		} else if (vector.count != chunk->size()) {
			throw Exception("Computed vector length does not match expected length!");
		}
		assert(vector.sel_vector == chunk->sel_vector);
	}
	assert(result.type == vector.type);
	vector.Move(result);
}

Value ExpressionExecutor::EvaluateScalar(Expression &expr) {
	assert(expr.IsFoldable());
	// use an ExpressionExecutor to execute the expression
	ExpressionExecutor executor;
	Vector result(expr.return_type, true, false);
	executor.ExecuteExpression(expr, result);
	assert(result.count == 1);
	return result.GetValue(0);
}

void ExpressionExecutor::Verify(Expression &expr, Vector &vector) {
	assert(expr.return_type == vector.type);
	vector.Verify();
}

void ExpressionExecutor::Execute(Expression &expr, Vector &result) {
	switch (expr.expression_class) {
	case ExpressionClass::BOUND_REF:
		Execute((BoundReferenceExpression &)expr, result);
		break;
	case ExpressionClass::BOUND_CASE:
		Execute((BoundCaseExpression &)expr, result);
		break;
	case ExpressionClass::BOUND_CAST:
		Execute((BoundCastExpression &)expr, result);
		break;
	case ExpressionClass::COMMON_SUBEXPRESSION:
		Execute((CommonSubExpression &)expr, result);
		break;
	case ExpressionClass::BOUND_COMPARISON:
		Execute((BoundComparisonExpression &)expr, result);
		break;
	case ExpressionClass::BOUND_CONJUNCTION:
		Execute((BoundConjunctionExpression &)expr, result);
		break;
	case ExpressionClass::BOUND_CONSTANT:
		Execute((BoundConstantExpression &)expr, result);
		break;
	case ExpressionClass::BOUND_FUNCTION:
		Execute((BoundFunctionExpression &)expr, result);
		break;
	case ExpressionClass::BOUND_OPERATOR:
		Execute((BoundOperatorExpression &)expr, result);
		break;
	default:
		assert(expr.expression_class == ExpressionClass::BOUND_PARAMETER);
		Execute((BoundParameterExpression &)expr, result);
		break;
	}
	Verify(expr, result);
}
