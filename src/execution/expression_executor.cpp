#include "duckdb/execution/expression_executor.hpp"

#include "duckdb/common/types/static_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

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
	best.runtime = -1.0;
}

ExpressionExecutor::ExpressionExecutor(DataChunk *child_chunk) : chunk(child_chunk) {
}

ExpressionExecutor::ExpressionExecutor(DataChunk &child_chunk) : chunk(&child_chunk) {
}

void Swap(index_t &x, index_t &y) {
	index_t tmp = x;
	x = y;
	y = tmp;
}

void GetPermutationByRank(index_t rank, index_t n, vector<index_t> &permutation) {
    index_t q, r;
    if (n < 1) {
		//stop recursive calls to GetPermutationByRank
		return;
	}
    q = rank / n;
    r = rank % n;
	//recursively unrank permutation
    Swap(permutation[r], permutation[n - 1]);
    GetPermutationByRank(q, n - 1, permutation);
}

index_t GetRandomDistinctRank(index_t expr_size_factorial, set<index_t> &illegal_ranks) {
	set<index_t>::iterator it;
	index_t r;
	//all permutations are already exhaused
	if (illegal_ranks.size() == expr_size_factorial) {
		return expr_size_factorial;
	}

	//while there are still sufficient permutations left
	while ((illegal_ranks.size() < ((expr_size_factorial * 3) / 4)) || (expr_size_factorial < 7)) {
		//generate random permutation rank
		srand(time(nullptr));
		r = rand() / (RAND_MAX / expr_size_factorial);

		//test if this permutation was already tested
		it = illegal_ranks.find(r);
		if (it == illegal_ranks.end()) {
			//if not, return the rank
			illegal_ranks.insert(r);
			return r;
		}
	}
	return expr_size_factorial;
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

		if (exploration_phase) {
			index_t rank = GetRandomDistinctRank(expr_size_factorial, illegal_ranks);
			if (rank == expr_size_factorial) {
				count = 0;
				exploration_phase = false;

				// get the iteration of the next random exploration phase
				srand(time(nullptr));
				random_explore = 200 + rand() / (RAND_MAX / (200)); //TODO: better value, not randomly 200
			} else {
				//get a new random permutation
				current.permutation = rank_0_permutation.permutation;
				GetPermutationByRank(rank, expressions.size(), current.permutation);
			}
		}

		//more than one expression, apply adaptive algorithm
		index_t start_idx = 0;
		index_t end_idx;

		//statistics
		chrono::time_point<chrono::high_resolution_clock> start_time;
		chrono::time_point<chrono::high_resolution_clock> end_time;

		if (exploration_phase) {
			end_idx = current.permutation.size();
		} else {
			end_idx = best.permutation.size();
		}

		//get the selectivity and the runtime for the current permutation
		index_t selectivity = 0;
		start_time = chrono::high_resolution_clock::now();

		//evaluate all expressions
		for (index_t i = start_idx; i < end_idx; i++) {
			//return if no more true rows
			if (chunk->size() != 0) {
				Vector intermediate;

				//evaluation of current expression
				if (exploration_phase) {
					//update metrics
					selectivity_count[current.permutation[i]] += chunk->data[0].count;
					execution_count[current.permutation[i]]++;

					//evaluate expression
					selectivity += chunk->data[0].count;
					Execute(*expressions[current.permutation[i]], intermediate);

				} else {
					//update metrics
					selectivity_count[best.permutation[i]] += chunk->data[0].count;
					execution_count[best.permutation[i]]++;

					//evaluate expression
					selectivity += chunk->data[0].count;
					Execute(*expressions[best.permutation[i]], intermediate);
				}

				assert(intermediate.type == TypeId::BOOLEAN);

				//if constant and false/null, set count == 0 to fetch the next chunk
				if (intermediate.IsConstant()) {
					if (!intermediate.data[0] || intermediate.nullmask[0]) {
						chunk->data[0].count = 0;
						break;
					}
				} else {
					chunk->SetSelectionVector(intermediate);
				}
			} else {
				break;
			}
		}

		end_time = chrono::high_resolution_clock::now();

		if (exploration_phase) {
			//set statistics of the current permutation
			current.runtime = chrono::duration_cast<chrono::duration<double>>(end_time - start_time).count();
			current.selectivity = selectivity;

			//compare to 'best' permutation
			if (best.runtime == -1.0) {
				best = current;
			} else {
				if (current < best) {
					best = current;
					count = 0;
				} else {
					count++;
				}

				//TODO: what could be a good threshold? How many permutations should be tested before taking the 'best' one
				if (count == 3) {
					count = 0;
					exploration_phase = false;

					// get the iteration of the next random exploration phase
					srand(time(nullptr));
					random_explore = 200 + rand() / (RAND_MAX / (200));
				}
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
					best.runtime = -1.0;
					illegal_ranks.clear();

					//TODO: adaptively change threshold
					//TODO: add last permutation to metrics permutation vector
				} else {
					score += normed_sel;
					count++;
				}
			} else if (count == random_explore) {
				// trigger random exploration phases to detect changes in runtime
				exploration_phase = true;
				score = 0.0;
				count = 0;
				best.runtime = -1.0;
				illegal_ranks.clear();

				//TODO: adaptively change threshold
				//TODO: add last permutation to metrics permutation vector
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
