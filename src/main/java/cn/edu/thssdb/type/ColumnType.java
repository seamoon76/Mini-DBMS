package cn.edu.thssdb.type;

import cn.edu.thssdb.exception.KeyNotExistException;

public enum ColumnType {
  INT,
  LONG,
  FLOAT,
  DOUBLE,
  STRING,
  MULTI_PRIMARY;

  public static ColumnType str2Type(String s) {
    s = s.toUpperCase();
    switch (s) {
      case "INT":
        return INT;
      case "int":
        return INT;
      case "LONG":
        return LONG;
      case "long":
        return LONG;
      case "FLOAT":
        return FLOAT;
      case "float":
        return FLOAT;
      case "DOUBLE":
        return DOUBLE;
      case "double":
        return DOUBLE;
      case "STRING":
        return STRING;
      case "string":
        return STRING;
      case "MULTI_PRIMARY":
        return MULTI_PRIMARY;
      default:
        throw new KeyNotExistException();
    }
  }
}
