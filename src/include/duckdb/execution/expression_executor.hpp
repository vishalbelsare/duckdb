//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/expression_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/planner/bound_tokens.hpp"
#include "duckdb/planner/expression.hpp"

#include <queue>

namespace duckdb {

//! ExpressionExecutor is responsible for executing an arbitrary
//! Expression and returning a Vector
class ExpressionExecutor {
	public:
		ExpressionExecutor();
		ExpressionExecutor(DataChunk *child_chunk);
		ExpressionExecutor(DataChunk &child_chunk);

		//! Executes a set of expressions and stores them in the result chunk
		void Execute(vector<unique_ptr<Expression>> &expressions, DataChunk &result);
		void Execute(vector<Expression *> &expressions, DataChunk &result);
		//! Executes a column expression and merges the selection vector
		void Merge(vector<std::unique_ptr<Expression>> &expressions);
		//! Execute a single abstract expression and store the result in result
		void ExecuteExpression(Expression &expr, Vector &result);
		//! Evaluate a scalar expression and fold it into a single value
		static Value EvaluateScalar(Expression &expr);
		//! Calculates a new expression execution order
		void GetNewPermutation();

		struct AdaptiveScore {
			index_t idx;
			double score;

			bool operator==(const AdaptiveScore &p) const {
				return idx == p.idx && score == p.score;
			}
			bool operator<(const AdaptiveScore &p) const {
				return score > p.score || (score == p.score && idx < p.score);
			}
		};

		//! The data chunk of the current physical operator, used to resolve
		//! column references and determines the output cardinality
		DataChunk *chunk;

		//! The current permutation to be used in the execution phase
		std::vector<index_t> current_perm;
		//! Insert the scores of the individual expressions
		std::priority_queue<AdaptiveScore> scores;

		//! Used to switch between execution and exploration phase
		bool exploration_phase;
		//! Count the iterations of the execution phase
		index_t count;
		//! A random iteration index to trigger the exploration phase
		index_t random_explore;
		//! Sum of the selectivities of the current execution phase
		double score;
		//! Adaptive threshold to trigger exploration phase
		double threshold;
		//! Last change percentage
		double change_percentage;

		//! Stores the number of values that where evaluated for each expression
		std::vector<index_t> selectivity_count;
		//! Stores how often each expression was executed
		std::vector<index_t> execution_count;
		//! Stores how often expressions are evaluated for a chunk
		index_t calls_to_merge;
		//! Stores how often the selection vector still contains elements after all expressions were evaluated
		index_t calls_to_get_chunk;
		//! Stores the calculated permutation of each exploration phase
		std::vector<std::vector<index_t>> permutations;

	protected:
		void Execute(Expression &expr, Vector &result);

		void Execute(BoundReferenceExpression &expr, Vector &result);
		void Execute(BoundCaseExpression &expr, Vector &result);
		void Execute(BoundCastExpression &expr, Vector &result);
		void Execute(CommonSubExpression &expr, Vector &result);
		void Execute(BoundComparisonExpression &expr, Vector &result);
		void Execute(BoundConjunctionExpression &expr, Vector &result);
		void Execute(BoundConstantExpression &expr, Vector &result);
		void Execute(BoundFunctionExpression &expr, Vector &result);
		void Execute(BoundOperatorExpression &expr, Vector &result);
		void Execute(BoundParameterExpression &expr, Vector &result);

		//! Verify that the output of a step in the ExpressionExecutor is correct
		void Verify(Expression &expr, Vector &result);

	private:
		//! The cached result of already-computed Common Subexpression results
		unordered_map<Expression *, unique_ptr<Vector>> cached_cse;
	};
} // namespace duckdb
