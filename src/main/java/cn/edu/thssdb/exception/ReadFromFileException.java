package cn.edu.thssdb.exception;

public class ReadFromFileException extends RuntimeException {
  @Override
  public String getMessage() {
    return "Exception: reading from file not succeed!";
  }
}
