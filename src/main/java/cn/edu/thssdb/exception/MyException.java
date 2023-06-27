package cn.edu.thssdb.exception;

public class MyException extends RuntimeException {
  String message = "";

  public MyException(String message) {
    super(message);
    this.message = message;
  }

  @Override
  public String getMessage() {
    return "Exception: " + this.message;
  }
}
