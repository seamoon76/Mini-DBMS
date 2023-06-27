package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.exception.MyException;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.ComparerType;

import java.util.ArrayList;

public class ComparisonBlock {
  public String tName = null;
  public String cName;
  public String strValue;
  public ComparerType type;
  public ComparisonBlock leftChild;
  public ComparisonBlock rightChild;
  public String operator;
  public boolean ifNull;
  public boolean haveKid;

  public ComparisonBlock() {
    this.type = ComparerType.NULL;
    this.strValue = "null";
    this.ifNull = true;
    this.haveKid = false;
  }

  public ComparisonBlock(ComparerType type, String tName, String cName) {
    if (type == ComparerType.COLUMN) {
      this.type = type;
      this.tName = tName;
      this.cName = cName;
      this.haveKid = false;
    } else {
      throw new MyException("Type not match" + type.toString() + ComparerType.COLUMN);
    }
  }

  public ComparisonBlock(ComparerType type, String strValue) {
    this.type = type;
    this.strValue = strValue;
    this.haveKid = false;
    if (type == ComparerType.NULL) {
      this.ifNull = true;
    }
  }

  public ComparisonBlock(ComparisonBlock leftChild, ComparisonBlock rightChild, String operator) {
    this.leftChild = leftChild;
    this.rightChild = rightChild;
    this.operator = operator;
    this.haveKid = true;
  }

  public Object getValue(Row row, ArrayList<String> columnNames) {
    try {
      if (type == ComparerType.COLUMN) {
        int index = -1;
        if (this.tName != null) {
          String columnFullName = this.tName + "_" + this.cName;
          index = columnNames.indexOf(columnFullName);
        }
        if (index == -1) {
          index = columnNames.indexOf(this.cName);
        }
        Entry entry = row.getEntries().get(index);
        return entry.value;
      } else if (type == ComparerType.NUMBER) {
        if (strValue.contains(".")) {
          return Double.parseDouble(strValue);
        } else {
          return Integer.parseInt(strValue);
        }
      } else if (type == ComparerType.STRING) {
        return strValue;
      }
      return null;
    } catch (Exception e) {
      System.out.println("Error: " + e.getMessage());
      return null;
    }
  }

  public Object getValue() {
    if (type == ComparerType.COLUMN) {
      throw new MyException("Not match type " + ComparerType.NUMBER + ComparerType.COLUMN);
    } else if (type == ComparerType.NUMBER) {
      if (strValue.contains(".")) {
        return Double.parseDouble(strValue);
      } else {
        return Integer.parseInt(strValue);
      }
    } else if (type == ComparerType.STRING) {
      return strValue;
    }
    return null;
  }

  public Double evaluate(Row row, ArrayList<String> columnNames) {
    if (!haveKid) {
      Object value = getValue(row, columnNames);
      if (value == null || value instanceof String) {
        return null;
      }
      return Double.parseDouble(value.toString());
    } else {
      Double leftValue = this.leftChild.evaluate(row, columnNames);
      Double rightValue = this.rightChild.evaluate(row, columnNames);
      Double res;
      switch (operator) {
        case "+":
          res = leftValue + rightValue;
          break;
        case "-":
          res = leftValue - rightValue;
          break;
        case "*":
          res = leftValue * rightValue;
          break;
        case "/":
          res = leftValue / rightValue;
          break;
        default:
          res = 0.0;
      }
      String newStr =
          res.intValue() == res.doubleValue() ? String.valueOf(res.intValue()) : res.toString();
      this.strValue = newStr;
      return res;
    }
  }

  public boolean isNull() {
    return this.ifNull;
  }
}
