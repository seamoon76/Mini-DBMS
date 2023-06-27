package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.exception.MyException;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.ComparerType;

import java.util.ArrayList;

public class ConditionBlock {
  public String comparator;
  public ComparisonBlock comp1;
  public ComparisonBlock comp2;

  public ConditionBlock(ComparisonBlock comp1, ComparisonBlock comp2, String op) {
    this.comparator = op;
    this.comp1 = comp1;
    this.comp2 = comp2;
  }

  /**
   * 判断某一行是否满足该条件
   *
   * @param row 数据库表中的一行
   * @param columnNames 列名列表
   * @return 是否满足条件
   */
  public Boolean evaluate(Row row, ArrayList<String> columnNames) {
    try {
      comp1.evaluate(row, columnNames);
      Object leftV = comp1.getValue(row, columnNames);

      comp2.evaluate(row, columnNames);
      Object rightV = comp2.getValue(row, columnNames);

      if (leftV == null || rightV == null) {
        if (comparator.equals("=")) {
          return leftV == rightV;
        } else if (comparator.equals("<>")) {
          return leftV != rightV;
        } else {
          return false;
        }
      }

      int resultOfComp = 0;

      boolean isString1 = leftV instanceof String;
      boolean isString2 = rightV instanceof String;
      if ((isString1 && !isString2) || (!isString1 && isString2)) {
        throw new MyException("Type not match " + ComparerType.NUMBER + " " + ComparerType.STRING);
      }

      // 存在Double无法和Integer转换的问题，所以单独讨论Number的部分（坑死了）
      if (leftV instanceof Integer || leftV instanceof Double) {
        Double newValue1 = Double.valueOf(leftV.toString());
        Double newValue2 = Double.valueOf(rightV.toString());
        resultOfComp = newValue1.compareTo(newValue2);
      } else {
        String newValue1 = leftV.toString();
        String newValue2 = rightV.toString();
        resultOfComp = newValue1.compareTo(newValue2);
      }

      boolean res = false;
      switch (comparator) {
        case ">":
          res = resultOfComp > 0;
          break;
        case "<":
          res = resultOfComp < 0;
          break;
        case ">=":
          res = resultOfComp >= 0;
          break;
        case "<=":
          res = resultOfComp <= 0;
          break;
        case "=":
          res = resultOfComp == 0;
          break;
        case "<>":
          res = resultOfComp != 0;
          break;
        default:
          throw new MyException("invalid comparator");
      }
      return res;
    } catch (Exception e) {
      System.out.println("Get Error " + e.getMessage());
      return null;
    }
  }
}
