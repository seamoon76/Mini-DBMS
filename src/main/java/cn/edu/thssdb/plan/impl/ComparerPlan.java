package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.query.QueryResult;

public class ComparerPlan extends LogicalPlan {

  public ComparisonBlock comparisonBlock;

  public ComparerPlan(ComparisonBlock comparisonBlock) {
    super(LogicalPlanType.COMPARER);
    this.comparisonBlock = comparisonBlock;
  }

  @Override
  public QueryResult execute() {
    return new QueryResult("");
  }
}
