package cn.edu.thssdb.exception;

public class MultiPrimaryException extends RuntimeException {
  private String table_name;

  public MultiPrimaryException(String table_name) {
    super();
    this.table_name = table_name;
  }

  @Override
  public String getMessage() {
    return "Exception: multiple primary keys in table " + table_name + "!";
  }
}
