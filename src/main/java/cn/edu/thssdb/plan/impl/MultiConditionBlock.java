package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.schema.Row;

import java.util.ArrayList;

public class MultiConditionBlock {
  public String op;
  public MultiConditionBlock leftItem;
  public MultiConditionBlock rightItem;
  public ConditionBlock conditionBlock;
  public Boolean hasChild;

  public MultiConditionBlock(ConditionBlock conditionBlock) {
    this.conditionBlock = conditionBlock;
    this.hasChild = false;
  }

  public MultiConditionBlock(
      MultiConditionBlock leftItem, MultiConditionBlock rightItem, String op) {
    this.leftItem = leftItem;
    this.rightItem = rightItem;
    this.op = op;
    this.hasChild = true;
  }

  /**
   * 判断一行是否满足多个条件
   *
   * @param row 数据库表中的一行
   * @param ColumnName 列名列表
   * @return 是否满足多个条件
   */
  public Boolean evaluate(Row row, ArrayList<String> ColumnName) {
    if (!hasChild) {
      return conditionBlock.evaluate(row, ColumnName);
    } else {
      Boolean leftCond = leftItem.evaluate(row, ColumnName);
      Boolean rightCond = rightItem.evaluate(row, ColumnName);
      if (op.equalsIgnoreCase("and")) {
        return (leftCond && rightCond);
      } else {
        return (leftCond || rightCond);
      }
    }
  }
}
