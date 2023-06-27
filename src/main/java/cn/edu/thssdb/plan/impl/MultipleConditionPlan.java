package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.query.QueryResult;

public class MultipleConditionPlan extends LogicalPlan {

  public MultiConditionBlock mConditionItem;

  public MultipleConditionPlan(MultiConditionBlock mConditionItem) {
    super(LogicalPlanType.COMPARER);
    this.mConditionItem = mConditionItem;
  }

  @Override
  public QueryResult execute() {
    return new QueryResult("");
  }
}
