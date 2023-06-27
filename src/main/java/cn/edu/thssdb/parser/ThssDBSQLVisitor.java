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
package cn.edu.thssdb.parser;

import cn.edu.thssdb.Config;
import cn.edu.thssdb.exception.DuplicateKeyException;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.exception.MyException;
import cn.edu.thssdb.exception.NoPrimaryException;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.impl.*;
import cn.edu.thssdb.plan.impl.ComparisonBlock;
import cn.edu.thssdb.plan.impl.ConditionBlock;
import cn.edu.thssdb.plan.impl.MultiConditionBlock;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.sql.SQLBaseVisitor;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.type.ComparerType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ThssDBSQLVisitor extends SQLBaseVisitor<LogicalPlan> {

  private long session;
  private Manager manager;

  public ThssDBSQLVisitor(long session, Manager manager) {
    this.session = session;
    this.manager = manager;
  }

  @Override
  public LogicalPlan visitCreateDbStmt(SQLParser.CreateDbStmtContext ctx) {
    return new CreateDatabasePlan(ctx.databaseName().getText(), manager);
  }

  @Override
  public LogicalPlan visitDropDbStmt(SQLParser.DropDbStmtContext ctx) {
    return new LogicalPlan(LogicalPlan.LogicalPlanType.DROP_DB) {
      @Override
      public QueryResult execute() {
        try {
          manager.deleteDatabase((ctx.databaseName().getText()));
          return new QueryResult("Delete DateBase" + ctx.databaseName().getText() + " success.");
        } catch (Exception e) {
          return new QueryResult(
              "Delete DateBase " + ctx.databaseName().getText() + "failed." + e.getMessage());
        } finally {
          //
        }
      }
    };
  }

  @Override
  public LogicalPlan visitCreateTableStmt(SQLParser.CreateTableStmtContext ctx) {
    return new LogicalPlan(LogicalPlan.LogicalPlanType.CREATE_TABLE) {
      @Override
      public QueryResult execute() {
        try {
          String tName = ctx.tableName().IDENTIFIER().getSymbol().getText();
          ArrayList<Column> columnArrayList = new ArrayList<>();
          boolean hasPriInColumn = false;
          int priInColumn = 0;
          for (int i = 0; i < ctx.columnDef().size(); i++) {
            SQLParser.ColumnDefContext columnDefContext = ctx.columnDef(i);
            String colName = columnDefContext.columnName().getText();
            String cTypeStr = null;
            ColumnType cType = null;
            int maxStringLength = 0;
            if (columnDefContext.typeName().getChildCount() == 1) {
              cTypeStr = columnDefContext.typeName().getText();
              cType = ColumnType.str2Type(cTypeStr);
            } else if (columnDefContext.typeName().getChildCount() > 1) {
              cType = ColumnType.str2Type(columnDefContext.typeName().getChild(0).getText());
              maxStringLength = Integer.parseInt(columnDefContext.typeName().getChild(2).getText());
            } else cType = ColumnType.str2Type(cTypeStr);
            boolean isNotNull = false;
            int isPrimaryKey = 0;
            for (int j = 0; j < columnDefContext.columnConstraint().size(); j++) {
              if (columnDefContext
                  .columnConstraint(j)
                  .getChild(1)
                  .getText()
                  .equalsIgnoreCase("NULL")) {
                isNotNull = true;
              }
              if (columnDefContext.columnConstraint(j).getText().equalsIgnoreCase("KEY")) {
                isPrimaryKey = 1;
                if (!hasPriInColumn) {
                  hasPriInColumn = true;
                  priInColumn += 1;
                }
              }
            }
            Column column = new Column(colName, cType, isPrimaryKey, isNotNull, maxStringLength);
            columnArrayList.add(column);
            // update isNotNull value of column which is related to primary key
            for (Column c : columnArrayList) {
              if (c.isPrimary() == 1) {
                c.setNotNull(1);
              }
            }
          }

          // support multi primary key
          if (!hasPriInColumn) // 如果前面没有指定主键
          {
            if (ctx.tableConstraint() == null) {
              throw new NoPrimaryException(tName);
            }
          }
          boolean multi_p = priInColumn > 1;
          if (ctx.tableConstraint() != null) {
            multi_p = ctx.tableConstraint().columnName().size() > 1;
            for (int i = 0; i < ctx.tableConstraint().columnName().size(); i++) {
              String constrainedColumnName = ctx.tableConstraint().columnName(i).getText();
              // find the column in columnArrayList whose name is constrainedColumnName
              for (Column c : columnArrayList) {
                if (c.getName().equalsIgnoreCase(constrainedColumnName)) {
                  c.setPrimary(1);
                  c.setNotNull(1);
                }
              }
            }
            if (multi_p) {
              columnArrayList.add(
                  new Column("multi_primary_key", ColumnType.MULTI_PRIMARY, 1, true, 100));
            }
          }

          Column[] resCols = columnArrayList.toArray(new Column[0]);

          Database.DatabaseProxy now_database_proxy = manager.getCurrentDatabase(false, true);
          now_database_proxy.getDatabase().create(tName, resCols, multi_p);
          return new QueryResult("Create Table " + ctx.tableName().getText() + " success.");
        } catch (Exception e) {
          return new QueryResult(
              "Create Table " + ctx.tableName().getText() + " failed. " + e.getMessage());
        } finally {

        }
      }
    };
  }

  @Override
  public LogicalPlan visitDropTableStmt(SQLParser.DropTableStmtContext ctx) {
    return new LogicalPlan(LogicalPlan.LogicalPlanType.DROP_TABLE) {
      @Override
      public QueryResult execute() {
        try (Database.DatabaseProxy now_database = manager.getCurrentDatabase(false, true)) {
          String tableToDropName = ctx.tableName().IDENTIFIER().getSymbol().getText();
          now_database.getDatabase().drop(tableToDropName, session);
          return new QueryResult("Drop Table " + ctx.tableName().getText() + "success.");
        } catch (Exception e) {
          return new QueryResult("Drop Table" + ctx.tableName().getText() + " failed.");
        } finally {

        }
      }
    };
  }

  @Override
  public LogicalPlan visitUseDbStmt(SQLParser.UseDbStmtContext ctx) {
    return new LogicalPlan(LogicalPlan.LogicalPlanType.USE_DB) {
      @Override
      public QueryResult execute() {
        try {
          String databaseName = ctx.databaseName().getText();
          manager.switchDatabase(databaseName);
          return new QueryResult("Use Database " + ctx.databaseName().getText() + "success.");
        } catch (Exception e) {
          return new QueryResult("Use Database" + ctx.databaseName().getText() + " failed.");
        } finally {

        }
      }
    };
  }

  @Override
  public LogicalPlan visitShowDbStmt(SQLParser.ShowDbStmtContext ctx) {
    return new LogicalPlan(LogicalPlan.LogicalPlanType.SHOW_DB) {
      @Override
      public QueryResult execute() {
        try {
          String result = manager.getAllDBInfo();
          return new QueryResult(result);
        } catch (Exception e) {
          return new QueryResult("Show Database failed.");
        } finally {

        }
      }
    };
  }

  @Override
  public LogicalPlan visitShowTableStmt(SQLParser.ShowTableStmtContext ctx) {
    return new LogicalPlan(LogicalPlan.LogicalPlanType.SHOW_TABLE) {
      @Override
      public QueryResult execute() {
        try (Database.DatabaseProxy now_database = manager.getCurrentDatabase(true, false)) {

          String tableName = ctx.tableName().getText();
          String result = now_database.getDatabase().getTableInfo(tableName);
          return new QueryResult(result);
        } catch (Exception e) {
          return new QueryResult("Show Table failed.");
        } finally {

        }
      }
    };
  }

  // handle quit
  @Override
  public LogicalPlan visitQuitStmt(SQLParser.QuitStmtContext ctx) {
    return new LogicalPlan(LogicalPlan.LogicalPlanType.QUIT) {
      @Override
      public QueryResult execute() {
        try {
          manager.quit();
          return new QueryResult("Quit success.");
        } catch (Exception e) {
          return new QueryResult("Quit failed.");
        } finally {

        }
      }
    };
  }

  // handle alter table
  @Override
  public LogicalPlan visitAlterTableStmt(SQLParser.AlterTableStmtContext ctx) {
    return new LogicalPlan(LogicalPlan.LogicalPlanType.ALTER_TABLE) {
      @Override
      public QueryResult execute() {
        try (Database.DatabaseProxy now_database = manager.getCurrentDatabase(true, false)) {
          String tableName = ctx.tableName().getText();
          try (Table.TableProxy tableProxy = now_database.getDatabase().getTable(tableName)) {
            Table table = tableProxy.getmTable();
            ArrayList<Column> columnArrayList = table.columns;
            // add column
            if (ctx.addColumn() != null) {
              SQLParser.AddColumnContext addColumnContext = ctx.addColumn();
              String columnName = addColumnContext.columnDef().columnName().getText();
              String columnTypeName = null;
              ColumnType columnType = null;
              int maxStringLength = 0;
              if (addColumnContext.columnDef().typeName().getChildCount() == 1) {
                columnTypeName = addColumnContext.columnDef().typeName().getText();
                columnType = ColumnType.str2Type(columnTypeName);
              } else if (addColumnContext.columnDef().typeName().getChildCount() > 1) {
                columnType =
                    ColumnType.str2Type(
                        addColumnContext.columnDef().typeName().getChild(0).getText());
                maxStringLength =
                    Integer.parseInt(addColumnContext.columnDef().typeName().getChild(2).getText());
              } else columnType = ColumnType.str2Type(columnTypeName);

              int isPrimaryKey = 0;
              boolean isNotNull = false;
              for (int j = 0; j < addColumnContext.columnDef().columnConstraint().size(); j++) {
                if (addColumnContext
                    .columnDef()
                    .columnConstraint(j)
                    .getChild(1)
                    .getText()
                    .equalsIgnoreCase("NULL")) {
                  isNotNull = true;
                }
                if (addColumnContext
                    .columnDef()
                    .columnConstraint(j)
                    .getChild(1)
                    .getText()
                    .equalsIgnoreCase("KEY")) {
                  isPrimaryKey = 1;
                }
              }
              if (isNotNull || isPrimaryKey == 1) {
                // 要加入非null列或者主键，不允许
                throw new MyException("Cannot add not null column or primary key column.");
              }
              Column column =
                  new Column(columnName, columnType, isPrimaryKey, isNotNull, maxStringLength);
              columnArrayList.add(column);
              // update isNotNull value of column which is related to primary key
              for (Column c : columnArrayList) {
                if (c.isPrimary() == 1) {
                  c.setNotNull(1);
                }
              }
              table.alterByAddColumn(columnArrayList);
            } else if (ctx.dropColumn() != null) {
              String columnName = ctx.dropColumn().columnName().getText();
              int indexToRemove = -1;
              for (int i = 0; i < columnArrayList.size(); i++) {
                if (columnArrayList.get(i).getName().equalsIgnoreCase(columnName)) {
                  columnArrayList.remove(i);
                  indexToRemove = i;
                  break;
                }
              }
              if (indexToRemove != -1) {
                table.alterByDropColumn(columnArrayList, indexToRemove);
              }
            } else if (ctx.renameColumn() != null) {
              String columnName = ctx.renameColumn().columnName().getText();
              String newColumnName = ctx.renameColumn().newColumnName().getText();
              int indexToRename = -1;
              for (int i = 0; i < columnArrayList.size(); i++) {
                if (columnArrayList.get(i).getName().equalsIgnoreCase(columnName)) {
                  columnArrayList.get(i).setName(newColumnName);
                  indexToRename = i;
                  break;
                }
              }
              if (indexToRename != -1) {
                table.alterByRenameColumn(columnArrayList, indexToRename);
              }
            } else if (ctx.addColumnConstraint() != null) {
              SQLParser.AddColumnConstraintContext addColumnConstraintContext =
                  ctx.addColumnConstraint();
              String columnName = addColumnConstraintContext.columnName().getText();
              SQLParser.ColumnConstraintContext columnConstraintContext =
                  addColumnConstraintContext.columnConstraint();
              int isPrimaryKey = 0;
              boolean isNotNull = false;

              if (columnConstraintContext.getChild(1).getText().equalsIgnoreCase("NULL")) {
                isNotNull = true;
              }
              if (columnConstraintContext.getChild(1).getText().equalsIgnoreCase("KEY")) {
                isPrimaryKey = 1;
              }
              for (int i = 0; i < columnArrayList.size(); i++) {
                if (columnArrayList.get(i).getName().equalsIgnoreCase(columnName)) {
                  columnArrayList.get(i).setPrimary(isPrimaryKey);
                  columnArrayList.get(i).setNotNull(isNotNull ? 1 : 0);
                  break;
                }
              }
              table.alterByChangeConstraint(columnArrayList);
            } else if (ctx.dropColumnConstraint() != null) {
              SQLParser.DropColumnConstraintContext dropColumnConstraintContext =
                  ctx.dropColumnConstraint();
              SQLParser.ColumnConstraintContext columnConstraintContext =
                  dropColumnConstraintContext.columnConstraint();
              String columnName = dropColumnConstraintContext.columnName().getText();
              int isPrimaryKey = 0;
              boolean isNotNull = false;
              if (columnConstraintContext.getChild(1).getText().equalsIgnoreCase("NULL")) {
                isNotNull = false;
              }
              //                if (columnConstraintContext
              //                        .getChild(1)
              //                        .getText()
              //                        .equalsIgnoreCase("KEY")) {
              //                  isPrimaryKey = 0;
              //                }
              for (int i = 0; i < columnArrayList.size(); i++) {
                if (columnArrayList.get(i).getName().equalsIgnoreCase(columnName)) {
                  if (columnArrayList.get(i).isPrimary() == 1) {
                    throw new RuntimeException("Can't drop primary key");
                  } else if (columnArrayList.get(i).cannotBeNull()) {
                    columnArrayList.get(i).setNotNull(isNotNull ? 1 : 0);
                  }
                  break;
                }
              }
              table.alterByChangeConstraint(columnArrayList);

            } else {

            }
            return new QueryResult("Alter Table success.");
          }
        }
      }
    };
  }

  // handle insert
  @Override
  public LogicalPlan visitInsertStmt(SQLParser.InsertStmtContext ctx) {
    return new LogicalPlan(LogicalPlan.LogicalPlanType.INSERT) {
      @Override
      public QueryResult execute() {
        try (Database.DatabaseProxy now_database = manager.getCurrentDatabase(true, false)) {
          String tableName = ctx.tableName().getText();
          try (Table.TableProxy tableProxy = now_database.getDatabase().getTable(tableName)) {
            Table tableToInsert = tableProxy.getmTable();
            ArrayList<ArrayList<String>> valueList = new ArrayList<>();
            for (SQLParser.ValueEntryContext valueCtx : ctx.valueEntry()) {
              ArrayList<String> strArray = new ArrayList<>();
              for (SQLParser.LiteralValueContext valueStrCtx : valueCtx.literalValue()) {
                String str = valueStrCtx.getText();
                strArray.add(str);
              }
              valueList.add(strArray);
            }

            for (ArrayList<String> stringArrayList : valueList) {
              ArrayList<Entry> entries = new ArrayList<>();
              if (ctx.columnName().size() == 0) {
                for (int i = 0; i < stringArrayList.size(); i++) {
                  String cType = tableToInsert.columns.get(i).getType().name().toUpperCase();
                  switch (cType) {
                    case "INT":
                      {
                        int v = Integer.parseInt(stringArrayList.get(i));
                        Entry e = new Entry(v);
                        entries.add(e);
                        break;
                      }
                    case "LONG":
                      {
                        long v = Long.parseLong(stringArrayList.get(i));
                        Entry e = new Entry(v);
                        entries.add(e);
                        break;
                      }
                    case "FLOAT":
                      {
                        float f = Float.parseFloat(stringArrayList.get(i));
                        Entry e = new Entry(f);
                        entries.add(e);
                        break;
                      }
                    case "DOUBLE":
                      {
                        double d = Double.parseDouble(stringArrayList.get(i));
                        Entry e = new Entry(d);
                        entries.add(e);
                        break;
                      }
                    case "STRING":
                      {
                        String str = stringArrayList.get(i);
                        // remove single comma in the str beginning and end
                        if (str.charAt(0) == '\'' && str.charAt(str.length() - 1) == '\'') {
                          str = str.substring(1, str.length() - 1);
                        }
                        Entry e = new Entry(str);
                        // Entry e = new Entry(valueEntryListStr.get(i));
                        entries.add(e);
                        break;
                      }
                    case "PRIMARY_KEY":
                      {
                        String str = stringArrayList.get(i);
                        // remove single comma in the str beginning and end
                        if (str.charAt(0) == '\'' && str.charAt(str.length() - 1) == '\'') {
                          str = str.substring(1, str.length() - 1);
                        }
                        Entry e = new Entry(str);
                        // Entry e = new Entry(valueEntryListStr.get(i));
                        entries.add(e);
                        break;
                      }
                    default:
                      throw new MyException("Insert invalid value type.");
                  }
                }
              } else {
                if (ctx.columnName().size() != stringArrayList.size()) {
                  int len = ctx.columnName().size();
                  int gotLen = stringArrayList.size();
                  throw new MyException(
                      "Schema length mismatch: expected " + len + ",got " + gotLen);
                }
                for (int i = 0; i < ctx.columnName().size() - 1; i++) {
                  for (int j = i + 1; j < ctx.columnName().size(); j++) {
                    if (ctx.columnName(i).getText().equals(ctx.columnName(j).getText())) {
                      throw new DuplicateKeyException();
                    }
                  }
                }
                // initialize value_entry
                for (int i = 0; i < tableToInsert.columns.size(); i++) {
                  Entry e = new Entry();
                  entries.add(e);
                }
                for (int i = 0; i < ctx.columnName().size(); i++) {
                  // 找到与column_name对应的列所在的index和type
                  int index = -1;
                  String targetType = "null";
                  for (int j = 0; j < tableToInsert.columns.size(); j++) {
                    if (tableToInsert
                        .columns
                        .get(j)
                        .getName()
                        .equalsIgnoreCase(ctx.columnName(i).getText())) {
                      index = j;
                      targetType = tableToInsert.columns.get(j).getType().name().toUpperCase();
                      break;
                    }
                  }
                  if (index < 0) {
                    throw new KeyNotExistException();
                  }
                  switch (targetType) {
                    case "INT":
                      {
                        int num = Integer.parseInt(stringArrayList.get(i));
                        Entry Entry = new Entry(num);
                        entries.set(index, Entry);
                        break;
                      }
                    case "LONG":
                      {
                        long num = Long.parseLong(stringArrayList.get(i));
                        Entry Entry = new Entry(num);
                        entries.set(index, Entry);
                        break;
                      }
                    case "FLOAT":
                      {
                        float f = Float.parseFloat(stringArrayList.get(i));
                        Entry Entry = new Entry(f);
                        entries.set(index, Entry);
                        break;
                      }
                    case "DOUBLE":
                      {
                        double d = Double.parseDouble(stringArrayList.get(i));
                        Entry e = new Entry(d);
                        entries.set(index, e);
                        break;
                      }
                    case "STRING":
                      {
                        String str = stringArrayList.get(i);
                        // remove single comma in the str beginning and end
                        if (str.charAt(0) == '\'' && str.charAt(str.length() - 1) == '\'') {
                          str = str.substring(1, str.length() - 1);
                        }
                        Entry Entry = new Entry(str);
                        entries.set(index, Entry);
                        break;
                      }
                    case "PRIMARY_KEY":
                      {
                        String str = stringArrayList.get(i);
                        // remove single comma in the str beginning and end
                        if (str.charAt(0) == '\'' && str.charAt(str.length() - 1) == '\'') {
                          str = str.substring(1, str.length() - 1);
                        }
                        Entry Entry = new Entry(str);
                        entries.set(index, Entry);
                        break;
                      }
                    default:
                      throw new MyException("Value Format Invalid, target type:" + targetType);
                  }
                }
              }
              Row r = new Row(entries);
              now_database.getDatabase().insertRowInTable(session, tableProxy, r);
              // table.insert(r);
            }
          }

        } catch (Exception e) {
          return new QueryResult("Insert failed." + e.getMessage());
        }
        return new QueryResult("Insert success.");
      }
    };
  }

  // delete
  @Override
  public LogicalPlan visitDeleteStmt(SQLParser.DeleteStmtContext ctx) {
    return new LogicalPlan(LogicalPlan.LogicalPlanType.DELETE) {
      @Override
      public QueryResult execute() {
        try (Database.DatabaseProxy now_database = manager.getCurrentDatabase(true, false)) {
          String tName = ctx.tableName().getText();
          try (Table.TableProxy tableProxy = now_database.getDatabase().getTable(tName)) {
            Table table = tableProxy.getmTable();
            if (ctx.K_WHERE() == null) {
              return new QueryResult("Exception: Delete without where");
            }
            MultiConditionBlock whereItem = null;
            if (ctx.multipleCondition() != null) {
              whereItem =
                  ((MultipleConditionPlan) visitMultipleCondition(ctx.multipleCondition()))
                      .mConditionItem;
            }
            ArrayList<String> columnNames = new ArrayList<>();
            ArrayList<Column> columns = table.columns;
            for (Column c : columns) {
              columnNames.add(c.getName());
            }

            if (whereItem == null) {
              return new QueryResult("Exception: Delete without where");
            }
            if (whereItem.leftItem == null
                && whereItem.conditionBlock.comparator.equalsIgnoreCase("=")
                && whereItem.conditionBlock.comp1.cName.equalsIgnoreCase("id")
                && whereItem.conditionBlock.comp2.type == ComparerType.NUMBER) {
              int id = Integer.parseInt(whereItem.conditionBlock.comp2.strValue);
              Entry idEntry = new Entry(id);
              if (table.index.contains(idEntry)) {
                Row row = table.getRow(idEntry);
                if (row != null) {
                  now_database.getDatabase().deleteRowInTable(session, tableProxy, row);
                }
              }

            } else {
              Iterator<Row> rowIterator = table.iterator();
              while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                if (whereItem.evaluate(row, columnNames)) {
                  // table.deleteRow(row);
                  now_database.getDatabase().deleteRowInTable(session, tableProxy, row);
                }
              }
              //              for (Row row : table) {
              //                if(row!=null) {
              //                  if (whereItem.evaluate(row, columnNames)) {
              //                    // table.deleteRow(row);
              //                    now_database.getDatabase().tableDelete(session, tp, row);
              //                  }
              //                }
              //              }

            }
          }
        } catch (Exception e) {
          return new QueryResult(
              "Delete from " + ctx.tableName().getText() + " failed." + e.getMessage());
        } finally {

        }
        return new QueryResult("Delete " + ctx.tableName().getText() + "success.");
      }
    };
  }

  // update
  @Override
  public LogicalPlan visitUpdateStmt(SQLParser.UpdateStmtContext ctx) {
    return new LogicalPlan(LogicalPlan.LogicalPlanType.UPDATE) {
      @Override
      public QueryResult execute() {

        try (Database.DatabaseProxy now_database = manager.getCurrentDatabase(true, false)) {
          String tName = ctx.tableName().getText();
          try (Table.TableProxy tableProxy = now_database.getDatabase().getTable(tName)) {
            Table table = tableProxy.getmTable();
            String cName = ctx.columnName().getText();

            ArrayList<String> cNames = new ArrayList<>();
            ArrayList<Column> columns = table.columns;
            for (Column col : columns) {
              cNames.add(col.getName());
            }

            MultiConditionBlock whereCondition = null;
            ArrayList<Row> targetRows = new ArrayList<>();
            if (ctx.multipleCondition() != null) {
              whereCondition =
                  ((MultipleConditionPlan) visitMultipleCondition(ctx.multipleCondition()))
                      .mConditionItem;
            }
            if (whereCondition == null) {
              throw new MyException("Update without where");
            }
            if (whereCondition.leftItem == null
                && whereCondition.conditionBlock.comparator.equalsIgnoreCase("=")
                && whereCondition.conditionBlock.comp1.cName.equalsIgnoreCase("id")
                && whereCondition.conditionBlock.comp2.type == ComparerType.NUMBER) {
              int id = Integer.parseInt(whereCondition.conditionBlock.comp2.strValue);
              Entry idEntry = new Entry(id);
              if (table.index.contains(idEntry)) {
                Row row = table.getRow(idEntry);
                if (row != null) {
                  targetRows.add(row);
                }
              }

            } else {
              Iterator<Row> rowRouter = table.iterator();
              if (whereCondition == null) {
                while (rowRouter.hasNext()) {
                  Row row = rowRouter.next();
                  targetRows.add(row);
                }
              } else {
                while (rowRouter.hasNext()) {
                  Row row = rowRouter.next();
                  if (whereCondition.evaluate(row, cNames)) {
                    targetRows.add(row);
                  }
                }
              }
            }
            int index = table.name2Index(cName);
            String indexedColumnTypeStr = table.columns.get(index).getType().name().toUpperCase();
            ComparisonBlock comparisonBlock =
                ((ComparerPlan) visitExpression(ctx.expression())).comparisonBlock;
            Entry entryToSet = new Entry();
            switch (indexedColumnTypeStr) {
              case "INT":
                {
                  int num = Integer.parseInt(comparisonBlock.strValue);
                  entryToSet = new Entry(num);
                  break;
                }
              case "LONG":
                {
                  long num = Long.parseLong(comparisonBlock.strValue);
                  entryToSet = new Entry(num);
                  break;
                }
              case "FLOAT":
                {
                  float f = Float.parseFloat(comparisonBlock.strValue);
                  entryToSet = new Entry(f);
                  break;
                }
              case "DOUBLE":
                {
                  double d = Double.parseDouble(comparisonBlock.strValue);
                  entryToSet = new Entry(d);
                  break;
                }
              case "STRING":
                {
                  String str = comparisonBlock.strValue;
                  // remove single comma in the str beginning and end
                  if (str.charAt(0) == '\'' && str.charAt(str.length() - 1) == '\'') {
                    str = str.substring(1, str.length() - 1);
                  }
                  entryToSet = new Entry(str);
                  break;
                }
              default:
                throw new MyException("Value Format Invalid, target type:" + indexedColumnTypeStr);
            }

            Entry newEntry = entryToSet;
            for (Row row : targetRows) {
              Row newRow = new Row();
              ArrayList<Entry> entries = row.getEntries();
              for (int i = 0; i < entries.size(); i++) {
                if (i == index) {
                  newRow.getEntries().add(newEntry);
                } else {
                  newRow.getEntries().add(entries.get(i));
                }
              }
              Entry primaryEntry = entries.get(table.getPrimaryIndex());
              // table.update(primaryEntry, newRow);
              now_database
                  .getDatabase()
                  .updateRowInTable(session, tableProxy, primaryEntry, newRow);
            }
          }
        } catch (Exception e) {
          return new QueryResult(
              "Update " + ctx.tableName().getText() + " failed. Error:" + e.getMessage());
        } finally {

        }
        return new QueryResult("Update " + ctx.tableName().getText() + "success.");
      }
    };
  }

  public class SelectPlan extends LogicalPlan {
    SQLParser.SelectStmtContext ctx;

    public SelectPlan(LogicalPlanType type, SQLParser.SelectStmtContext ctx) {
      super(type);
      this.ctx = ctx;
    }

    @Override
    public QueryResult execute() {
      return executeQuery();
    }

    public QueryResult executeQuery() {

      try (Database.DatabaseProxy now_database = manager.getCurrentDatabase(true, false)) {
        SQLParser.TableQueryContext tableCtx = ctx.tableQuery().get(0);

        // 遍历，加读锁
        if (!Config.CLOSE_LOCK) {
          if ((Config.ISOLATION_LEVEL == "SERIALIZATION"
              || Config.ISOLATION_LEVEL == "READ_COMMITTED")) {
            for (int i = 0; i < tableCtx.tableName().size(); i++) {
              String tableName = tableCtx.tableName(i).getText();
              try (Table.TableProxy tp = now_database.getDatabase().getTable(tableName)) {
                now_database.getDatabase().getLockManager().getRLock(session, tp);
              } catch (Exception e) {
                return new QueryResult("Table " + tableName + " not found.");
              } finally {
              }
            }
          }
        }
        boolean noJoin = true;
        String table1Name = tableCtx.tableName(0).getText();
        try (Table.TableProxy firstTP = now_database.getDatabase().getTable(table1Name)) {
          Table table1 = firstTP.getmTable();
          QueryTable workedTable = null;
          if (tableCtx.tableName().size() > 1) {

            // 需要join
            noJoin = false;
            // 判断是否有on条件
            MultiConditionBlock onCondition = null;
            if (tableCtx.multipleCondition() != null) {
              onCondition =
                  ((MultipleConditionPlan) visitMultipleCondition(tableCtx.multipleCondition()))
                      .mConditionItem;
            } else {
              // 没有on条件，不符合要求，不执行join
              return new QueryResult("No on condition.");
            }

            if (onCondition.leftItem == null
                && onCondition.conditionBlock.comparator.equalsIgnoreCase("=")
                && onCondition.conditionBlock.comp1.cName.contains("id")
                && onCondition.conditionBlock.comp2.cName.contains("id")) {
              noJoin = true; // on条件为id=id，此时不需要join，直接使用第一个表，where字句只会访问第一个表
            }
            if (!noJoin) {
              Table newFirstTable = table1.toFullTable();
              workedTable = new QueryTable(newFirstTable);
              for (int i = 1; i < tableCtx.tableName().size(); i++) {
                String tmpTableName = tableCtx.tableName(i).getText();
                try (Table.TableProxy nowTable =
                    now_database.getDatabase().getTable(tmpTableName)) {
                  workedTable = workedTable.joinTable(nowTable.getmTable());
                } catch (Exception e) {
                  return new QueryResult("Table " + tmpTableName + " not found.");
                } finally {
                }
              }
              if (onCondition != null) {
                Iterator<Row> rowRouter = workedTable.rows_results.iterator();
                ArrayList<String> colNames = new ArrayList<>();
                for (Column column : workedTable.columns) {
                  colNames.add(column.getName());
                }
                while (rowRouter.hasNext()) {
                  Row row = rowRouter.next();
                  if (!onCondition.evaluate(row, colNames)) {

                    rowRouter.remove();
                  }
                }
              }
            }
          }
          // where
          ArrayList<Row> rowToSelect = new ArrayList<>();
          if (ctx.multipleCondition() != null) {
            MultiConditionBlock whereCondition = null;

            whereCondition =
                ((MultipleConditionPlan) visitMultipleCondition(ctx.multipleCondition()))
                    .mConditionItem;

            if (whereCondition == null) {
              throw new MyException("Select without where");
            }
            if (whereCondition.leftItem == null
                && whereCondition.conditionBlock.comparator.equalsIgnoreCase("=")
                && whereCondition.conditionBlock.comp1.cName.contains("id")
                && whereCondition.conditionBlock.comp2.type == ComparerType.NUMBER
                && noJoin) {
              int id = Integer.parseInt(whereCondition.conditionBlock.comp2.strValue);
              Entry idEntry = new Entry(id);
              if (table1.index.contains(idEntry)) {
                Row row = table1.getRow(idEntry);
                if (row != null) {
                  rowToSelect.add(row);
                }
              }
            } else {
              // need join或者单表复杂where，用复杂算法
              whereCondition = ((visitMultipleCondition(ctx.multipleCondition())).mConditionItem);
              // 若不是join得到的表，是单表复杂情况，转为targetTable遍历
              if (workedTable == null) {
                workedTable = new QueryTable(table1);
              }
              Iterator<Row> rowRouter = workedTable.rows_results.iterator();
              ArrayList<String> columnNames = new ArrayList<>();
              for (Column column : workedTable.columns) {
                columnNames.add(column.getName());
              }
              if (tableCtx.tableName().size() > 1) {
                if (whereCondition.conditionBlock.comp1.type == ComparerType.COLUMN) {
                  whereCondition.conditionBlock.comp1.tName = tableCtx.tableName(0).getText();
                  whereCondition.conditionBlock.comp1.cName =
                      whereCondition.conditionBlock.comp1.tName
                          + "_"
                          + whereCondition.conditionBlock.comp1.cName;
                } else if (whereCondition.conditionBlock.comp2.type == ComparerType.COLUMN) {
                  whereCondition.conditionBlock.comp2.tName = tableCtx.tableName(0).getText();
                  whereCondition.conditionBlock.comp1.cName =
                      whereCondition.conditionBlock.comp1.tName
                          + "_"
                          + whereCondition.conditionBlock.comp1.cName;
                }
              }
              while (rowRouter.hasNext()) {
                Row row = rowRouter.next();
                if (row != null) {

                  if (whereCondition.evaluate(row, columnNames)) {
                    rowToSelect.add(row);
                  }
                }
              }
            }
          } else {
            // 没有where条件，直接使用第一个表
            if (noJoin) {
              // 遍历table1，把所有row加入rowToSelect
              for (Row row : table1) {
                rowToSelect.add(row);
              }
            } else {
              rowToSelect.addAll(workedTable.rows_results);
            }
          }

          // filter by select condition
          QueryTable resTable = new QueryTable();
          List<Column> selectedCols = resTable.columns;
          List<Row> rowList = resTable.rows_results;
          if (ctx.resultColumn().get(0).getText().equals("*")) {
            if (noJoin) {
              selectedCols.addAll(table1.columns);
              rowList.addAll(rowToSelect);
            } else {
              selectedCols.addAll(workedTable.columns);
              rowList.addAll(rowToSelect);
            }
          } else {
            // filter column
            ArrayList<String> selectColumnName = new ArrayList<>();
            for (SQLParser.ResultColumnContext columnContext : ctx.resultColumn()) {
              if (columnContext.columnFullName() != null) {
                String columnName = columnContext.columnFullName().columnName().getText();
                if (columnContext.columnFullName().tableName() != null
                    && tableCtx.tableName().size() > 1) {
                  columnName =
                      columnContext.columnFullName().tableName().getText() + "_" + columnName;
                } else if (columnContext.columnFullName().tableName() == null
                    && tableCtx.tableName().size() > 1) {
                  columnName = tableCtx.tableName(0).getText() + "_" + columnName;
                } else if (columnContext.columnFullName().tableName() == null
                    && tableCtx.tableName().size() == 1) {
                  columnName = table1Name + "_" + columnName;
                }
                selectColumnName.add(columnName);
              }
            }

            ArrayList<Integer> selectedIdxs = new ArrayList<>();
            if (noJoin) {
              ArrayList<String> colNames = new ArrayList<>();
              for (Column column : table1.columns) {
                colNames.add(table1Name + "_" + column.getName());
              }
              for (String columnName : selectColumnName) {
                int index = colNames.indexOf(columnName);
                if (index == -1) {
                  throw new Exception("column " + columnName + " not found");
                }
                selectedCols.add(table1.columns.get(index));
                selectedIdxs.add(index);
              }
            } else {
              for (String columnName : selectColumnName) {
                int index = workedTable.Column2Index(columnName);
                selectedCols.add(workedTable.columns.get(index));
                selectedIdxs.add(index);
              }
            }

            for (Row row : rowToSelect) {
              ArrayList<Entry> Entries = row.getEntries();
              ArrayList<Entry> newRowEntries = new ArrayList<>();
              for (int i = 0; i < Entries.size(); i++) {
                if (selectedIdxs.contains(i)) {
                  // System.out.println(i+"is in selectColumnIndex");
                  newRowEntries.add(Entries.get(i));
                }
              }
              Row newRow = new Row(newRowEntries.toArray(new Entry[0]));
              rowList.add(newRow);
            }
          }

          return new QueryResult(resTable);
        } catch (Exception e) {
          return new QueryResult(e.getMessage());
        } finally {
        }
      } catch (Exception e) {
        return new QueryResult(e.getMessage());
      } finally {

      }
    }
  }
  // select
  @Override
  public LogicalPlan visitSelectStmt(SQLParser.SelectStmtContext ctx) {
    return new SelectPlan(LogicalPlan.LogicalPlanType.SELECT, ctx);
  }

  @Override
  public ComparerPlan visitComparer(SQLParser.ComparerContext context) {

    if (context.columnFullName() != null) {
      String tName = null;
      if (context.columnFullName().tableName() != null) {
        tName = context.columnFullName().tableName().IDENTIFIER().getText();
      }
      String cName = context.columnFullName().columnName().IDENTIFIER().getText();
      return new ComparerPlan(new ComparisonBlock(ComparerType.COLUMN, tName, cName));
    } else if (context.literalValue() != null) {
      String valueStr = "null";
      if (context.literalValue().NUMERIC_LITERAL() != null) {
        valueStr = context.literalValue().NUMERIC_LITERAL().getText();
        return new ComparerPlan(new ComparisonBlock(ComparerType.NUMBER, valueStr));
      } else if (context.literalValue().STRING_LITERAL() != null) {
        valueStr = context.literalValue().STRING_LITERAL().getText();
        return new ComparerPlan(new ComparisonBlock(ComparerType.STRING, valueStr));
      }
      return new ComparerPlan(new ComparisonBlock(ComparerType.NULL, valueStr));
    }
    return null;
  }

  @Override
  public ComparerPlan visitExpression(SQLParser.ExpressionContext expressionContext) {
    if (expressionContext.comparer() != null) {
      return (ComparerPlan) visit(expressionContext.comparer());
    } else if (expressionContext.expression().size() == 1) {
      return (ComparerPlan) visit(expressionContext.getChild(1));
    } else {
      ComparisonBlock block1 =
          ((ComparerPlan) visit(expressionContext.getChild(0))).comparisonBlock;
      ComparisonBlock block2 =
          ((ComparerPlan) visit(expressionContext.getChild(2))).comparisonBlock;

      if ((block1.type != ComparerType.NUMBER && block1.type != ComparerType.COLUMN)
          || (block2.type != ComparerType.NUMBER && block2.type != ComparerType.COLUMN)) {
        String errMsg = block1.type.toString() + "not match" + ComparerType.NUMBER.toString();
        throw new MyException(errMsg);
      }
      ComparisonBlock newBlock =
          new ComparisonBlock(block1, block2, expressionContext.getChild(1).getText());
      newBlock.type = ComparerType.NUMBER;

      return new ComparerPlan(newBlock);
    }
  }

  @Override
  public ConditionPlan visitCondition(SQLParser.ConditionContext conditionContext) {
    ComparisonBlock block1 = ((ComparerPlan) visit(conditionContext.getChild(0))).comparisonBlock;
    ComparisonBlock block2 = ((ComparerPlan) visit(conditionContext.getChild(2))).comparisonBlock;
    return new ConditionPlan(
        new ConditionBlock(block1, block2, conditionContext.getChild(1).getText()));
  }

  @Override
  public MultipleConditionPlan visitMultipleCondition(SQLParser.MultipleConditionContext mc) {
    if (mc.getChildCount() == 1) {
      return new MultipleConditionPlan(
          new MultiConditionBlock(((ConditionPlan) visit(mc.getChild(0))).conditionBlock));
    }

    MultiConditionBlock block1 = ((MultipleConditionPlan) visit(mc.getChild(0))).mConditionItem;
    MultiConditionBlock block2 = ((MultipleConditionPlan) visit(mc.getChild(2))).mConditionItem;
    return new MultipleConditionPlan(
        new MultiConditionBlock(block1, block2, mc.getChild(1).getText()));
  }
}
