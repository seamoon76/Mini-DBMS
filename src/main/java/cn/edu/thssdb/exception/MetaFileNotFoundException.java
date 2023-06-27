package cn.edu.thssdb.exception;

public class MetaFileNotFoundException extends RuntimeException {
  private String fname;

  public MetaFileNotFoundException(String fname) {
    super();
    this.fname = fname;
  }

  @Override
  public String getMessage() {
    return "Exception: meta file not found!";
  }
}
