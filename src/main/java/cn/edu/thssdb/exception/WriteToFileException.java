package cn.edu.thssdb.exception;

public class WriteToFileException extends RuntimeException {
  @Override
  public String getMessage() {
    return "Exception: writing to file not succeed!";
  }
}
