package cn.edu.thssdb.schema;

import cn.edu.thssdb.Config;
import cn.edu.thssdb.exception.DuplicateTableException;
import cn.edu.thssdb.exception.MyException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.plan.LogicalGenerator;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.utils.DBLockManager;

import java.io.File;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static cn.edu.thssdb.schema.Manager.deleteFolder;

public class Database {

  private String name;
  private HashMap<String, Table> tables;
  private DBLockManager lockManager;
  ReentrantReadWriteLock lock;
  private Meta meta;
  private Log log;

  //  private Manager managerRef;

  public Database(String name) {
    this.name = name;
    this.tables = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
    this.lockManager = new DBLockManager(this);
    String dir = Config.DBMS_ROOT_DIR + File.separator + "DATA" + File.separator + name;
    String meta_name = name + "Meta.txt";
    this.meta = new Meta(dir, meta_name);
    String log_name = name + "log.txt";
    this.log = new Log(dir, log_name);
    recover();
  }

  public class DatabaseProxy implements AutoCloseable {
    private Database database;
    private Boolean hasRLock;
    private Boolean hasWLock;

    public DatabaseProxy(Database database, Boolean hasRead, Boolean hasWrite) {
      this.database = database;
      this.hasRLock = hasRead;
      this.hasWLock = hasWrite;
      if (hasRead) {
        this.database.lock.readLock().lock();
      }
      if (hasWrite) {
        this.database.lock.writeLock().lock();
      }
    }

    public Database getDatabase() {
      return this.database;
    }

    @Override
    public void close() {
      if (hasWLock) {
        this.database.lock.writeLock().unlock();
        hasWLock = false;
      }
      if (hasRLock) {
        this.database.lock.readLock().unlock();
        hasRLock = false;
      }
    }
  }

  private void persist() {
    ArrayList<String> keys = new ArrayList<>();
    for (String key : tables.keySet()) {
      tables.get(key).persist();
      keys.add(key);
    }
    this.meta.writeToFile(keys);
    this.log.eraseLog();
  }

  public DatabaseProxy getReadProxy() {
    if (Config.CLOSE_LOCK) {
      return new DatabaseProxy(this, false, false);
    }
    return new DatabaseProxy(this, true, false);
  }

  public DatabaseProxy getWriteProxy() {
    if (Config.CLOSE_LOCK) {
      return new DatabaseProxy(this, false, false);
    }
    return new DatabaseProxy(this, true, false);
  }

  public void create(String tName, Column[] columns, boolean multiPrimary) {
    if (this.tables.containsKey(tName)) throw new DuplicateTableException(tName);
    Table table = new Table(this.name, tName, columns, multiPrimary);
    this.tables.put(tName, table);
    this.persist();
  }

  public void drop(String name, Long session) {
    if (!tables.containsKey(name)) {
      throw new TableNotExistException();
    } else {
      try (Table.TableProxy tp = this.getTable(name)) {
        Table table = tp.getmTable();

        String folder_path = Paths.get(meta.dir_name, name).toString();
        System.out.println(folder_path);
        File folder = new File(folder_path);
        if (folder.exists()) {
          deleteFolder(folder);
          System.out.println("folder deleted");
        } else {
          System.out.println("folder not exist");
        }
        this.persist();
        if (!Config.CLOSE_LOCK) {
          //          lockManager.getWLock(session, tp);
        }
        tables.remove(name);
      } catch (Exception e) {
        throw e;
      }
    }
  }

  // get name
  public String getDBName() {
    return this.name;
  }

  public void deleteData() {
    for (Table table : tables.values()) {
      table.deleteAll();
    }
    tables.clear();
    this.meta.deleteFile();
    // Paths.get(Global.DATA_ROOT_FOLDER, name).toFile().delete();
  }

  public String select(QueryTable queryTable) {
    QueryResult queryResult = new QueryResult(queryTable);
    return null;
  }

  private void recover() {
    System.out.println("try to recover database: " + this.name);
    ArrayList<String[]> table_list = this.meta.readFromFile();
    // 从第1行开始
    for (String[] table_info : table_list) {
      tables.put(table_info[0], new Table(this.name, table_info[0]));
    }
  }

  public void quit() {
    try {
      if (!Config.CLOSE_LOCK) {
        this.lock.readLock().lock();
      }
      this.persist();
    } finally {
      if (!Config.CLOSE_LOCK) {
        this.lock.readLock().unlock();
      }
    }
  }

  public DBLockManager getLockManager() {
    return this.lockManager;
  }

  public String getAllTableInfo() {
    StringBuilder res = new StringBuilder("All Table Info:\n");
    for (Table table : tables.values()) {
      res.append(table.tableName).append("\n");
      for (Column column : table.columns) {
        res.append(column.toString()).append("\n");
      }
      res.append("**********\n");
    }
    return res.toString();
  }

  public String getTableInfo(String tableName) {
    StringBuilder res = new StringBuilder("Table Info:\n");
    if (!tables.containsKey(tableName)) {
      return "no such table named " + tableName + "\n";
    } else {
      for (Column column : tables.get(tableName).columns) {
        res.append(column.toString()).append("\n");
      }
      return res.toString();
    }
  }

  public Table.TableProxy getTable(String tableName) {
    if (!tables.containsKey(tableName)) {
      return null;
    } else {
      return tables.get(tableName).getTableProxy();
    }
  }

  public void insertRowInTable(Long session, Table.TableProxy proxy, Row row) {
    if (!Config.CLOSE_LOCK) {
      lockManager.getWLock(session, proxy);
    }
    proxy.getmTable().insert(row);
  }

  public void deleteRowInTable(Long session, Table.TableProxy proxy, Row row) {
    if (!Config.CLOSE_LOCK) {
      lockManager.getWLock(session, proxy);
    }
    proxy.getmTable().deleteRow(row);
  }

  public void updateRowInTable(Long session, Table.TableProxy proxy, Entry primaryEntry, Row row) {
    if (!Config.CLOSE_LOCK) {
      // System.out.println("111111");
      lockManager.getWLock(session, proxy);
      // System.out.println("222222");
    }
    proxy.getmTable().update(primaryEntry, row);
  }

  public void logRecover(Manager manager) {
    if (!Config.CLOSE_LOG) {
      ArrayList<String> logs = this.log.readLog();

      boolean suc = true;
      QueryResult queryResult = new QueryResult("");
      for (String log : logs) {
        ExecuteStatementReq req = new ExecuteStatementReq();

        //      Manager manager = this.managerRef;
        //      manager.switchDatabase(this.getDBName());
        LogicalPlan x = LogicalGenerator.generate(log, req.sessionId, manager, true);
        queryResult = x.execute();
        //        if (queryResult.errorMessage.contains("fail")) {
        //          suc = false;
        //          break;
        //        }
      }
      if (!suc) {
        throw new MyException("recover log failed." + queryResult.getResults());
      } else {
        this.persist();
      }
    }
  }

  public void writeLog(List<String> lines) {
    if (!Config.CLOSE_LOG) {
      this.log.writeLog(lines);
    }
  }

  public void eraseLog() {
    if (!Config.CLOSE_LOG) {
      this.log.eraseLog();
    }
  }

  public int getLogLength() {
    if (!Config.CLOSE_LOG) {
      int lineNumber = 0;

      try {
        File file = new File(log.getFull_path());
        if (file.exists()) {
          FileReader fr = new FileReader(file);
          LineNumberReader lnr = new LineNumberReader(fr);
          while (null != lnr.readLine()) {
            lineNumber += 1;
          }
          System.out.println("log row numbers = " + lineNumber);
          lnr.close();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
      return lineNumber;
    }
    return 0;
  }
}
