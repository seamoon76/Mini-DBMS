package cn.edu.thssdb.service;

import cn.edu.thssdb.plan.LogicalGenerator;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.impl.CommitPlan;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.rpc.thrift.ConnectReq;
import cn.edu.thssdb.rpc.thrift.ConnectResp;
import cn.edu.thssdb.rpc.thrift.DisconnectReq;
import cn.edu.thssdb.rpc.thrift.DisconnectResp;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.rpc.thrift.GetTimeReq;
import cn.edu.thssdb.rpc.thrift.GetTimeResp;
import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.StatusUtil;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class IServiceHandler implements IService.Iface {

  private static final AtomicInteger sessionCnt = new AtomicInteger(0);
  private Manager manager = new Manager();

  @Override
  public GetTimeResp getTime(GetTimeReq req) throws TException {
    GetTimeResp resp = new GetTimeResp();
    resp.setTime(new Date().toString());
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public ConnectResp connect(ConnectReq req) throws TException {
    return new ConnectResp(StatusUtil.success(), sessionCnt.getAndIncrement());
  }

  @Override
  public DisconnectResp disconnect(DisconnectReq req) throws TException {
    return new DisconnectResp(StatusUtil.success());
  }

  @Override
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
    ExecuteStatementResp executeStatementResp = new ExecuteStatementResp();
    if (req.getSessionId() < 0) {
      return new ExecuteStatementResp(
          StatusUtil.fail("You are not connected. Please connect first."), false);
    }
    String cmd = req.statement;
    String[] sqls = cmd.split(";");
    ArrayList<QueryResult> resList = new ArrayList<>();

    for (String sqlStr : sqls) {
      sqlStr = sqlStr.trim();
      if (sqlStr.length() == 0) continue;
      String chead = cmd.split("\\s+")[0];
      QueryResult queryResult = new QueryResult("");
      List<String> temp = Arrays.asList("insert", "delete", "update", "select");
      if ((temp.contains(chead.toLowerCase())) && !manager.containSession(req.getSessionId())) {
        //      if ((temp.contains(chead.toLowerCase()))
        //          && !manager.hotSessions.contains(req.getSessionId())) {
        LogicalGenerator.generate("begin transaction", req.sessionId, manager, false);
        LogicalPlan x = LogicalGenerator.generate(sqlStr, req.sessionId, manager, false);
        queryResult = x.execute();
        CommitPlan cx =
            (CommitPlan) LogicalGenerator.generate("commit", req.sessionId, manager, false);
        cx.execute();
      } else {
        LogicalPlan x = LogicalGenerator.generate(sqlStr, req.sessionId, manager, false);
        queryResult = x.execute();
      }
      if (queryResult == null) {
        //        executeStatementResp.setStatus(new Status(Global.SUCCESS_CODE));
        //        return executeStatementResp;
        return new ExecuteStatementResp(StatusUtil.fail("null"), false);
      }
      resList.add(queryResult);
    }

    QueryResult qr = resList.get(0);

    if (qr.getResultType() == QueryResult.QueryReturnType.SELECT) {
      ExecuteStatementResp e = new ExecuteStatementResp(StatusUtil.success(), true);
      e.setRowList(qr.getRowStringList());
      e.setColumnsList(qr.getColumnNames());
      return e;
    } else {
      if (qr.errorMessage != null && qr.errorMessage.contains("success")) {
        return new ExecuteStatementResp(StatusUtil.success(), false);
      } else {
        return new ExecuteStatementResp(StatusUtil.fail(qr.errorMessage), false);
      }
    }
  }
}
