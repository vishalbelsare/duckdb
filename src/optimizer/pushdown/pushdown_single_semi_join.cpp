#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

using namespace duckdb;
using namespace std;

using Filter = FilterPushdown::Filter;

unique_ptr<Expression> EquivalenceMapRewrite(unique_ptr<Expression> expr, expression_map_t<Expression*> &map) {
	auto entry = map.find(expr.get());
	if (entry != map.end()) {
		return entry->second->Copy();
	}
	ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> child) {
		return EquivalenceMapRewrite(move(child), map);
	});
	return move(expr);
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownSingleSemiJoin(unique_ptr<LogicalOperator> op,
                                                               unordered_set<idx_t> &left_bindings,
                                                               unordered_set<idx_t> &right_bindings) {
	auto &join = (LogicalJoin &) op;
	assert(join.join_type == JoinType::SINGLE || join.join_type == JoinType::SEMI);
	expression_map_t<Expression*> equivalence_map;
	if (op->type == LogicalOperatorType::DELIM_JOIN || op->type == LogicalOperatorType::COMPARISON_JOIN) {
		// for comparison joins we create an equivalence map from the LHS -> RHS
		// any comparisons involving the
		auto &comp_join = (LogicalComparisonJoin &) *op;
		for(auto &cond : comp_join.conditions) {
			if (cond.comparison == ExpressionType::COMPARE_EQUAL) {
				equivalence_map[cond.left.get()] = cond.right.get();
			}
		}
	}
	FilterPushdown left_pushdown(optimizer), right_pushdown(optimizer);
	// now check the set of filters
	for (idx_t i = 0; i < filters.size(); i++) {
		auto side = JoinSide::GetJoinSide(filters[i]->bindings, left_bindings, right_bindings);
		if (side == JoinSide::LEFT) {
			if (equivalence_map.size() > 0) {
				// condition on LHS, check if we can rewrite it using the equivalence set to a filter on the RHS
				auto new_filter = EquivalenceMapRewrite(filters[i]->filter->Copy(), equivalence_map);
				unordered_set<idx_t> new_bindings;
				LogicalJoin::GetExpressionBindings(*new_filter, new_bindings);
				if (JoinSide::GetJoinSide(new_bindings, left_bindings, right_bindings) == JoinSide::RIGHT) {
					// rewriting using the equivalence set turned the condition into a condition on the RHS!
					// push the equivalent condition into the right hand side
					right_pushdown.AddFilter(move(new_filter));
				}
			}

			// bindings match left side: push into left
			left_pushdown.filters.push_back(move(filters[i]));
			// erase the filter from the list of filters
			filters.erase(filters.begin() + i);
			i--;
		}
	}
	right_pushdown.GenerateFilters();

	op->children[0] = left_pushdown.Rewrite(move(op->children[0]));
	op->children[1] = right_pushdown.Rewrite(move(op->children[1]));
	return FinishPushdown(move(op));
}
