package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.sql.SQLParser;

public class SelectStmtPlan extends LogicalPlan {
  private SQLParser.SelectStmtContext ctx;

  public SelectStmtPlan(SQLParser.SelectStmtContext ctx) {
    super(LogicalPlanType.SELECT);
    this.ctx = ctx;
  }

  @Override
  public QueryResult execute() {
    return new QueryResult("Use executeQuery to execute select statement.");
  }

  //  public QueryResult executeQuery(Manager manager) {
  //    Database now_database = manager.getCurrentDatabase(true, false);
  //
  //    try {
  //      Database db = manager.getCurrentDatabase(true, false);
  //      SQLParser.TableQueryContext tableQuery = ctx.tableQuery().get(0);
  //      String firstTableName = tableQuery.tableName(0).getText();
  //      try {
  //        Table firstTable = db.getTable(firstTableName);
  //        // 生成from对应的查询表 targetTable
  //        // select from 不止一个表,将多表进行连接，获取目标表targetTable
  //        QueryTable targetTable = new QueryTable(firstTable);
  //        if (tableQuery.tableName().size() > 1) {
  //          Table newFirstTable = firstTable.getColumnFullNameTable();
  //          targetTable = new QueryTable(newFirstTable);
  //          for (int i = 1; i < tableQuery.tableName().size(); i++) {
  //            String nowTableName = tableQuery.tableName(i).getText();
  //            try {
  //              Table nowTable = db.getTable(nowTableName);
  //              targetTable = targetTable.join(nowTable);
  //            } catch (Exception e) {
  //              return new QueryResult("Table " + nowTableName + " not found.");
  //            } finally {
  //            }
  //          }
  //          System.out.println(targetTable.toString());
  //          // 按 On 的条件进行筛选，删除不满足的行
  //          if (tableQuery.multipleCondition() != null) {
  //            MultipleConditionItem onItem =
  //                (visitMultipleCondition(tableQuery.multipleCondition())
  //                    .multipleConditionItem);
  //            Iterator<Row> rowIterator = targetTable.rows_results.iterator();
  //            ArrayList<String> columnNames = new ArrayList<>();
  //            for (Column column : targetTable.columns) {
  //              columnNames.add(column.getColumnName());
  //            }
  //            List<Row> rowToDelete = new ArrayList<>();
  //            while (rowIterator.hasNext()) {
  //              Row row = rowIterator.next();
  //              if (!onItem.evaluate(row, columnNames)) {
  //                rowToDelete.add(row);
  //              }
  //            }
  //            targetTable.rows_results.removeAll(rowToDelete);
  //          }
  //        }
  //        // 按 where 条件进行筛选，删除不满足的行
  //        if (ctx.multipleCondition() != null) {
  //          MultipleConditionItem whereItem =
  //              ((visitMultipleCondition(ctx.multipleCondition())).multipleConditionItem);
  //          Iterator<Row> rowIterator = targetTable.rows_results.iterator();
  //          ArrayList<String> columnNames = new ArrayList<>();
  //          for (Column column : targetTable.columns) {
  //            columnNames.add(column.getColumnName());
  //          }
  //          List<Row> rowToDelete = new ArrayList<>();
  //          while (rowIterator.hasNext()) {
  //            Row row = rowIterator.next();
  //            if (!whereItem.evaluate(row, columnNames)) {
  //              rowToDelete.add(row);
  //            }
  //          }
  //          targetTable.rows_results.removeAll(rowToDelete);
  //        }
  //        // 按select进行列的筛选
  //        ArrayList<Column> selectColumns = new ArrayList<>();
  //        ArrayList<Row> rowList = new ArrayList<>();
  //        if (ctx.resultColumn().get(0).getText().equals("*")) {
  //          selectColumns.addAll(targetTable.columns);
  //          rowList.addAll(targetTable.rows_results);
  //        } else {
  //          // 先对列进行筛选
  //          ArrayList<String> selectColumnName = new ArrayList<>();
  //          for (SQLParser.ResultColumnContext columnContext : ctx.resultColumn()) {
  //            if (columnContext.columnFullName() != null) { // 按大作业说明，这种情况一定存在columnFullName
  //              String columnName = columnContext.columnFullName().columnName().getText();
  //              if (columnContext.columnFullName().tableName() != null
  //                  && tableQuery.tableName().size() > 1) {
  //                columnName =
  //                    columnContext.columnFullName().tableName().getText() + "_" + columnName;
  //              }
  //              selectColumnName.add(columnName);
  //            }
  //          }
  //
  //          System.out.print("selectColumnsName:");
  //          for (String columnName : selectColumnName) {
  //            System.out.print(columnName + " ");
  //          }
  //          System.out.println(" ");
  //
  //          // 获取selectColumnName对应的index
  //          ArrayList<Integer> selectColumnIndex = new ArrayList<>();
  //          for (String columnName : selectColumnName) {
  //            int index = targetTable.Column2Index(columnName);
  //            selectColumns.add(targetTable.columns.get(index));
  //            selectColumnIndex.add(index);
  //          }
  //
  //          System.out.print("selectColumnIndex:");
  //          for (Integer index : selectColumnIndex) {
  //            System.out.print(index + " ");
  //          }
  //
  //          // 再对行按列筛选
  //          for (Row row : targetTable.rows_results) {
  //            ArrayList<Entry> Entries = row.getEntries();
  //            ArrayList<Entry> newEntries = new ArrayList<>();
  //            for (int i = 0; i < Entries.size(); i++) {
  //              if (selectColumnIndex.contains(i)) {
  //                // System.out.println(i+"is in selectColumnIndex");
  //                newEntries.add(Entries.get(i));
  //              }
  //            }
  //            Row newRow = new Row(newEntries.toArray(new Entry[0]));
  //            rowList.add(newRow);
  //          }
  //        }
  //        // 得到ArrayList<Column> selectColumns 为列
  //        // 得到ArrayList<Row> rowList 为行
  //        // 测试值是否正确
  //
  //        for (Column column : selectColumns) {
  //          System.out.print(column.toString() + " ");
  //        }
  //        System.out.println(" ");
  //        for (Row row : rowList) {
  //          System.out.println(row.toString());
  //        }
  //
  //        QueryTable queryTable = new QueryTable(rowList, selectColumns);
  //        QueryTable[] queryTables = {queryTable};
  //        return new QueryResult(queryTables);
  //      } catch (Exception e) {
  //        return new QueryResult(e.getMessage());
  //      } finally {
  //      }
  //    } catch (Exception e) {
  //      return new QueryResult(e.getMessage());
  //    } finally {
  //
  //    }
  //    return new QueryResult("Select success.");
  //  }
};
