package cn.edu.thssdb.exception;

public class NoPrimaryException extends RuntimeException {
  private String table_name;

  public NoPrimaryException(String table_name) {
    super();
    this.table_name = table_name;
  }

  @Override
  public String getMessage() {
    return "Exception: no primary keys in table " + table_name + "!";
  }
}
