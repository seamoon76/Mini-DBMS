/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.thssdb.plan;

import cn.edu.thssdb.Config;
import cn.edu.thssdb.parser.SQLParseError;
import cn.edu.thssdb.parser.ThssDBSQLVisitor;
import cn.edu.thssdb.plan.impl.CommitPlan;
import cn.edu.thssdb.plan.impl.MessagePlan;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.sql.SQLLexer;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.utils.Global;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.Arrays;
import java.util.List;

public class LogicalGenerator {

  public static LogicalPlan generate(String sql, long session, Manager manager, boolean recovering)
      throws ParseCancellationException {
    ThssDBSQLVisitor dbsqlVisitor = new ThssDBSQLVisitor(session, manager);

    // Process "begin transaction" and "commit"
    String head = sql.split("\\s+")[0];
    boolean isCreateTable = false;
    if (head.equalsIgnoreCase("create") && sql.split("\\s+")[1].equalsIgnoreCase("table")) {
      isCreateTable = true;
    }
    List<String> temp =
        Arrays.asList("insert", "delete", "update", "begin", "commit", "create", "drop");
    if ((temp.contains(head.toLowerCase()) || isCreateTable) && session != -1) {
      if (!Config.CLOSE_LOG) {
        try (Database.DatabaseProxy db = manager.getCurrentDatabase(true, false)) {
          if (!recovering) {
            db.getDatabase().writeLog(Arrays.asList(sql));
          }
        } catch (Exception e) {

        }
      }
    }
    if (Config.DEBUG_OUT_LOG) {
      System.out.println("session:" + session + "  " + sql);
    }
    // Process "begin transaction"
    if (sql.equals(Global.LOG_BEGIN_TRANSACTION) || sql.equals(Global.LOG_BEGIN_TRANSACTION2)) {
      try {
        if (!manager.containSession(session)) {
          manager.addSession(session);
        } else {
          if (Config.DEBUG_OUT_LOG) {
            System.out.println("session already in a transaction.");
          }
        }
      } catch (Exception e) {
        return new MessagePlan(e.getMessage());
      }
      return new MessagePlan("Session: " + session + " begin transaction success.");
    }

    // Process "commit" statement
    if (sql.equals(Global.LOG_COMMIT_TRANSACTION) || sql.equals(Global.LOG_COMMIT_TRANSACTION2)) {
      return new CommitPlan(session, manager);
    }

    CharStream charStream1 = CharStreams.fromString(sql);
    SQLLexer lexer1 = new SQLLexer(charStream1);
    lexer1.removeErrorListeners();
    lexer1.addErrorListener(SQLParseError.INSTANCE);

    CommonTokenStream tokens = new CommonTokenStream(lexer1);

    SQLParser parser = new SQLParser(tokens);
    parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
    parser.removeErrorListeners();
    parser.addErrorListener(SQLParseError.INSTANCE);

    ParseTree tree;
    try {
      // STAGE 1: try with simpler/faster SLL(*)
      tree = parser.sqlStmt();
    } catch (Exception ex) {
      CharStream charStream2 = CharStreams.fromString(sql);

      SQLLexer lexer2 = new SQLLexer(charStream2);
      lexer2.removeErrorListeners();
      lexer2.addErrorListener(SQLParseError.INSTANCE);

      CommonTokenStream tokens2 = new CommonTokenStream(lexer2);

      SQLParser parser2 = new SQLParser(tokens2);
      parser2.getInterpreter().setPredictionMode(PredictionMode.LL);
      parser2.removeErrorListeners();
      parser2.addErrorListener(SQLParseError.INSTANCE);

      // STAGE 2: parser with full LL(*)
      tree = parser2.sqlStmt();
    }
    return dbsqlVisitor.visit(tree);
  }
}
