package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.query.QueryResult;

public class MessagePlan extends LogicalPlan {
  private String message;

  public MessagePlan(String message) {
    super(LogicalPlanType.MESSAGE);
    this.message = message;
  }

  public String getMessage() {
    return message;
  }

  // execute
  @Override
  public QueryResult execute() {
    System.out.println(message);
    return new QueryResult(message + "\n");
  }

  @Override
  public String toString() {
    return "MessagePlan{" + "message='" + message + '\'' + '}';
  }
}
