package cn.edu.thssdb.plan;

import cn.edu.thssdb.query.QueryResult;

public abstract class LogicalPlan {

  protected LogicalPlanType type;

  public LogicalPlan(LogicalPlanType type) {
    this.type = type;
  }

  public LogicalPlanType getType() {
    return type;
  }

  public enum LogicalPlanType {
    // TODO: add more LogicalPlanType
    CREATE_DB,
    DROP_DB,
    CREATE_TABLE,
    DROP_TABLE,
    INSERT,
    DELETE,
    SELECT,
    UPDATE,
    COMMIT,
    ROLLBACK,
    QUIT,
    SHOW_DB,
    SHOW_META,
    USE_DB,
    COMPARER,
    MESSAGE,
    SHOW_TABLE,
    ALTER_TABLE,
  }

  public abstract QueryResult execute();
}
