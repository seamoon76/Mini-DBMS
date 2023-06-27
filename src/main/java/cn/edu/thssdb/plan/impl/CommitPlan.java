package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.Config;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;

public class CommitPlan extends LogicalPlan {
  private Long session;
  private Manager manager;

  public CommitPlan(Long session, Manager manager) {
    super(LogicalPlanType.COMMIT);
    this.session = session;
    this.manager = manager;
  }

  @Override
  public String toString() {
    return "CommitPlan{}";
  }

  @Override
  public QueryResult execute() {
    try {
      if (manager.containSession(session)) {
        // contains(session)
        try (Database.DatabaseProxy databaseProxy = manager.getCurrentDatabase(true, false)) {
          String databaseName = databaseProxy.getDatabase().getDBName();
          manager.removeSession(session);
          //                    manager.hotSessions.remove(session);
          databaseProxy.getDatabase().getLockManager().freeAllWriteLocks(session);
          if (Config.ISOLATION_LEVEL == "SERIALIZATION") {
            databaseProxy.getDatabase().getLockManager().freeAllReadLocks(session);
          }
          if (databaseProxy.getDatabase().getLogLength() >= 60000) {
            databaseProxy.getDatabase().eraseLog();
            manager.persistDB(databaseName);
          }
        }
      } else {
        System.out.println("session not in a transaction.");
        return new QueryResult("Session: " + session + " not in a transaction.");
      }
    } catch (Exception e) {
      //        logicalPlans.add(new MessagePlan(e.getMessage()));
      //        return logicalPlans;
      return new QueryResult("Session: " + session + " commit success." + e.getMessage());
    }

    return new QueryResult("Session: " + session + " commit success.");
  }
}
