package cn.edu.thssdb.exception;

public class MakeFileException extends RuntimeException {
  @Override
  public String getMessage() {
    return "Exception: making file not succeed!";
  }
}
