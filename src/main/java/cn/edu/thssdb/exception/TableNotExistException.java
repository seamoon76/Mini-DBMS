package cn.edu.thssdb.exception;

public class TableNotExistException extends RuntimeException {
  @Override
  public String getMessage() {
    return "Exception: deleting nonexistent table!";
  }
}
