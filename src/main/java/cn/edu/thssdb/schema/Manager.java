package cn.edu.thssdb.schema;

import cn.edu.thssdb.Config;
import cn.edu.thssdb.exception.*;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
  private HashMap<String, Database> databases;
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private Meta meta;
  private ArrayList<String> databasesList;
  private String currentDBName;

  public ArrayList<Long> hotSessions;

  // public final String DATABASE_MANAGER_FILE_PATH = Config.DBMS_ROOT_DIR + File.separator + "DATA"
  // + File.separator + "databaseMeta.txt";

  public Manager() {
    databases = new HashMap<>();
    lock = new ReentrantReadWriteLock();
    // create Manager folder
    meta = new Meta(Config.DBMS_ROOT_DIR + File.separator + "DATA", "managerMeta.txt");
    databasesList = new ArrayList<>();
    hotSessions = new ArrayList<>();
    // recover from persist storage
    this.recoverManagerData();
  }

  private void recoverManagerData() {
    ArrayList<String[]> db_str_list = this.meta.readFromFile();
    if (Config.DEBUG_OUT_LOG) {
      System.out.println(db_str_list);
    }
    for (String[] db_name : db_str_list) {
      databasesList.add(db_name[0]);
      Database d = new Database(db_name[0]);
      databases.put(db_name[0], d);
      this.currentDBName = db_name[0];
      d.logRecover(this);
    }
  }

  public void writeMeta() {
    this.meta.writeToFile(this.databasesList);
  }

  public void createDatabaseIfNotExists(String db_name) {

    try {
      if (!Config.CLOSE_LOCK) {
        lock.writeLock().lock();
      }
      if (databases.containsKey(db_name)) {
        throw new DuplicateDatabaseException();
      } else {
        Database d = new Database(db_name);
        databases.put(db_name, d);
        databasesList.add(db_name);
        this.currentDBName = db_name;
        d.logRecover(this);

        System.out.println(databasesList);
        currentDBName = db_name;
        writeMeta();
      }
    } finally {
      if (!Config.CLOSE_LOCK) {
        lock.writeLock().unlock();
      }
    }
  }

  public void persistDB(String dbName) {
    try {
      lock.readLock().lock();
      try (Database.DatabaseProxy db = getDBProxy(dbName, true, false)) {
        db.getDatabase().quit();
      }
      writeMeta();
    } finally {
      lock.readLock().unlock();
    }
  }

  public void removeSession(long sessionId) {
    try {
      lock.writeLock().lock();
      hotSessions.remove(sessionId);
    } finally {
      lock.writeLock().unlock();
    }
  }

  // add session
  public void addSession(long sessionId) {
    try {
      lock.writeLock().lock();
      hotSessions.add(sessionId);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public boolean containSession(long sessionId) {
    // use read lock
    try {
      lock.readLock().lock();
      return hotSessions.contains(sessionId);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void deleteDatabase(String db_name) {
    try {
      if (!Config.CLOSE_LOCK) {
        lock.writeLock().lock();
      }
      if (!databases.containsKey(db_name)) {
        return;
        // throw new DatabaseNotExistException();
      }
      try (Database.DatabaseProxy dp = getDBProxy(db_name, false, true)) {
        String folder_path = Paths.get(meta.dir_name, db_name).toString();
        System.out.println(folder_path);
        File folder = new File(folder_path);
        if (folder.exists()) {
          deleteFolder(folder);
          if (Config.DEBUG_OUT_LOG) {
            System.out.println("folder has been deleted");
          }
        } else {
          if (Config.DEBUG_OUT_LOG) {
            System.out.println("folder does not exist");
          }
        }
      }
      databases.remove(db_name);
      databasesList.remove(db_name);
      writeMeta();
    } finally {
      if (!Config.CLOSE_LOCK) {
        lock.writeLock().unlock();
      }
    }
  }

  public static void deleteFolder(File folder) {
    // 如果是文件，直接删除
    if (folder.isFile()) {
      folder.delete();
      return;
    }

    // 如果是文件夹，递归删除子文件夹及其内容
    File[] files = folder.listFiles();
    if (files != null) {
      for (File file : files) {
        deleteFolder(file);
      }
    }

    // 删除文件夹本身
    folder.delete();
  }

  public void switchDatabase(String databaseName) {
    try {
      lock.writeLock().lock();
      if (!databases.containsKey(databaseName)) throw new DatabaseNotExistException();
      this.currentDBName = databaseName;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public Database.DatabaseProxy getCurrentDatabase(boolean isRead, boolean isWrite) {
    try {
      if (!Config.CLOSE_LOCK) {
        lock.readLock().lock();
      }
      if (currentDBName == null) throw new DatabaseNotExistException();
      return getDBProxy(currentDBName, isRead, isWrite);
    } finally {
      if (!Config.CLOSE_LOCK) {
        lock.readLock().unlock();
      }
    }
  }

  public Database.DatabaseProxy getDBProxy(String databaseName, Boolean isRead, Boolean isWrite) {
    try {
      if (!Config.CLOSE_LOCK) {
        lock.readLock().lock();
      }
      if (!databases.containsKey(databaseName)) throw new DatabaseNotExistException();
      if (isRead) return databases.get(databaseName).getReadProxy();
      else return databases.get(databaseName).getWriteProxy();
    } finally {
      if (!Config.CLOSE_LOCK) {
        lock.readLock().unlock();
      }
    }
  }

  public String getAllDBInfo() {
    try {
      if (!Config.CLOSE_LOCK) {
        lock.readLock().lock();
      }
      StringBuilder res = new StringBuilder("All databases:\n");
      for (String key : databases.keySet()) {
        res.append(key).append("\n");
      }
      return res.toString();
    } finally {
      if (!Config.CLOSE_LOCK) {
        lock.readLock().unlock();
      }
    }
  }

  public void quit() {
    try {
      if (!Config.CLOSE_LOCK) {
        lock.writeLock().lock();
      }
      for (String databaseName : databases.keySet()) {
        try (Database.DatabaseProxy db = getDBProxy(databaseName, false, true)) {
          db.getDatabase().quit();
        }
      }
      writeMeta();
      databases.clear();
      this.currentDBName = null;
    } finally {
      if (!Config.CLOSE_LOCK) {
        lock.writeLock().unlock();
      }
    }
  }
}
