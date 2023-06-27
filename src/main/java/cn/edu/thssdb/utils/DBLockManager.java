package cn.edu.thssdb.utils;

import cn.edu.thssdb.Config;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DBLockManager {
  private Database mDatabase;
  public HashMap<Long, ArrayList<ReentrantReadWriteLock.ReadLock>> readLockMap;
  public HashMap<Long, ArrayList<ReentrantReadWriteLock.WriteLock>> writeLockMap;

  public DBLockManager(Database database) {
    this.mDatabase = database;
    this.readLockMap = new HashMap<>(6, 1);
    this.writeLockMap = new HashMap<>(6, 1);
  }

  public void getWLock(Long sessionID, Table.TableProxy proxy) {
    if (Config.CLOSE_LOCK) {
      return;
    }
    if (Config.ISOLATION_LEVEL == "SERIALIZATION") {
      // 释放所有本线程读锁
      freeAllReadLocks(sessionID);
    }
    Boolean wLock = proxy.setWLock();
    if (!wLock) return;
    if (!writeLockMap.containsKey(sessionID)) {
      writeLockMap.put(sessionID, new ArrayList<>());
    }
    ArrayList<ReentrantReadWriteLock.WriteLock> nowSesWriteLocks =
        writeLockMap.get(sessionID) == null ? new ArrayList<>() : writeLockMap.get(sessionID);
    nowSesWriteLocks.add(proxy.getmTable().lock.writeLock());
    writeLockMap.put(sessionID, nowSesWriteLocks);
  }

  public void getRLock(Long sessionID, Table.TableProxy proxy) {
    if (Config.CLOSE_LOCK) {
      return;
    }
    Boolean rLock = proxy.setRLock();
    if (!rLock) return;
    if (!readLockMap.containsKey(sessionID)) {
      readLockMap.put(sessionID, new ArrayList<>());
    }
    ArrayList<ReentrantReadWriteLock.ReadLock> nowSesReadLocks =
        readLockMap.get(sessionID) == null ? new ArrayList<>() : readLockMap.get(sessionID);
    nowSesReadLocks.add(proxy.getmTable().lock.readLock());
    readLockMap.put(sessionID, nowSesReadLocks);
  }

  public void freeAllWriteLocks(Long sessionID) {
    if (Config.CLOSE_LOCK) {
      return;
    }
    if (!writeLockMap.containsKey(sessionID)) {
      return;
    }
    ArrayList<ReentrantReadWriteLock.WriteLock> writeLocks = writeLockMap.get(sessionID);
    for (ReentrantReadWriteLock.WriteLock lock : writeLocks) {
      lock.unlock();
      if (Config.DEBUG_OUT_LOG) {
        System.out.println(
            "release write lock of sessionID:"
                + sessionID
                + " from lock manager"
                + lock.toString());
      }
    }
    writeLocks.clear();
    writeLockMap.remove(sessionID);
    //    writeLockMap.put(sessionID, writeLocks);
    if (writeLockMap.get(sessionID).size() > 0) {
      System.out.println("Error size");
    }
  }

  public void freeAllReadLocks(Long sessionID) {
    if (Config.CLOSE_LOCK) {
      return;
    }
    if (!readLockMap.containsKey(sessionID)) {
      return;
    }
    ArrayList<ReentrantReadWriteLock.ReadLock> readLocks = readLockMap.get(sessionID);
    for (ReentrantReadWriteLock.ReadLock lock : readLocks) {
      lock.unlock();
    }
    readLocks.clear();
    readLockMap.remove(sessionID);
    //    readLockMap.put(sessionID, readLocks);
  }
}
