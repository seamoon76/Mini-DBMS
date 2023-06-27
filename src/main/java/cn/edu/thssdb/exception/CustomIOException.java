package cn.edu.thssdb.exception;

public class CustomIOException extends RuntimeException {
  @Override
  public String getMessage() {
    return "Exception: An IOError occurred";
  }
}
