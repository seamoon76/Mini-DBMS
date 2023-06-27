package cn.edu.thssdb.exception;

public class DuplicateTableException extends RuntimeException {
  private String tName;

  public DuplicateTableException(String n) {
    super();
    tName = n;
  }

  @Override
  public String getMessage() {
    return "Exception: create table \"" + tName + "\" caused duplicated tables!";
  }
}
