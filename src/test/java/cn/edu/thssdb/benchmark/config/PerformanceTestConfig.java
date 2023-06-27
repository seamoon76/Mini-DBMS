package cn.edu.thssdb.benchmark.config;

public class PerformanceTestConfig {

  public static final int CLIENT_NUMBER = 5;

  public static final int TABLE_NUMBER = 3;

  public static final int OPERATION_NUMBER = 50000;

  //  public static final String OPERATION_RATIO = "80:5:5:5:0";
  // public static final String OPERATION_RATIO = "60:10:10:20:0";
  public static final String OPERATION_RATIO = "50:10:10:20:10";
  // "50:10:10:20:10"

  public static final int DATA_SEED = 667;
}
