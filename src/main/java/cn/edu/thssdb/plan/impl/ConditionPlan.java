package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.query.QueryResult;

public class ConditionPlan extends LogicalPlan {

  public ConditionBlock conditionBlock;

  public ConditionPlan(ConditionBlock conditionBlock) {
    super(LogicalPlanType.COMPARER);
    this.conditionBlock = conditionBlock;
  }

  @Override
  public QueryResult execute() {
    return new QueryResult("");
  }
}
