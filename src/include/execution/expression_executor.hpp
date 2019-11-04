//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/expression_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/types/data_chunk.hpp"
#include "common/unordered_map.hpp"
#include "planner/bound_tokens.hpp"
#include "planner/expression.hpp"

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

	//! The data chunk of the current physical operator, used to resolve
	//! column references and determines the output cardinality
	DataChunk *chunk;

	//! The current permutation to be used in the execution phase
	std::vector<index_t> current_perm;
	//! Runtimes of each expression measured in the last exploration phase
	std::vector<double> expr_runtimes;
	//! Selectivity of each expression measured in the last exploration phase
	std::vector<index_t> expr_selectivity;

	//! Used to switch between execution and exploration phase
	bool exploration_phase;
	//! Count the iterations of the execution phase
	index_t count;

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
