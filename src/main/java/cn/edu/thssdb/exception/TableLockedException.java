package cn.edu.thssdb.exception;

public class TableLockedException extends RuntimeException {
  @Override
  public String getMessage() {
    return "Exception: table is write locked!";
  }
}
