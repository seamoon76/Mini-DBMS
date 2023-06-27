package cn.edu.thssdb.utils;

public class Global {
  public static int fanout = 129;

  public static int SUCCESS_CODE = 0;
  public static int FAILURE_CODE = -1;

  public static String DEFAULT_SERVER_HOST = "127.0.0.1";
  public static int DEFAULT_SERVER_PORT = 6667; // 19191;
  public static String DEFAULT_USER_NAME = "root";
  public static String DEFAULT_PASSWORD = "root";

  public static String CLI_PREFIX = "ThssDB2023>";
  public static final String SHOW_TIME = "show time;";
  public static final String CONNECT = "connect";
  public static final String DISCONNECT = "disconnect;";
  public static final String QUIT = "quit;";

  // begin transaction
  public static final String LOG_BEGIN_TRANSACTION = "begin transaction";
  public static final String LOG_BEGIN_TRANSACTION2 = "begin transaction;";
  // commit transaction
  public static final String LOG_COMMIT_TRANSACTION = "commit";
  public static final String LOG_COMMIT_TRANSACTION2 = "commit;";

  public static final String S_URL_INTERNAL = "jdbc:default:connection";
}
