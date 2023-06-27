package cn.edu.thssdb.utils;

import cn.edu.thssdb.Config;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DBLockManagerIntVersion {
  private Database mDatabase;
  private HashMap<Long, Integer> writeSessionMap; // 把session映射到0-4
  private HashMap<Long, Integer> readSessionMap; // 把session映射到0-4
  private List<ArrayList<ReentrantReadWriteLock.ReadLock>> readLockListArray;
  private List<ArrayList<ReentrantReadWriteLock.WriteLock>> writeLockListArray;

  //  public HashMap<Long, ArrayList<ReentrantReadWriteLock.ReadLock>> readLockMap;
  //  public HashMap<Long, ArrayList<ReentrantReadWriteLock.WriteLock>> writeLockMap;

  public DBLockManagerIntVersion(Database database) {
    this.mDatabase = database;
    readSessionMap = new HashMap<>();
    writeSessionMap = new HashMap<>();
    readLockListArray = new ArrayList<>();
    writeLockListArray = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      readLockListArray.add(new ArrayList<ReentrantReadWriteLock.ReadLock>());
      writeLockListArray.add(new ArrayList<ReentrantReadWriteLock.WriteLock>());
    }

    //    this.readLockMap = new HashMap<>(6,1);
    //    this.writeLockMap = new HashMap<>(6,1);
  }

  public void getWLock(Long session, Table.TableProxy tb) {
    if (Config.CLOSE_LOCK) {
      return;
    }
    if (Config.ISOLATION_LEVEL == "SERIALIZATION") {
      // 释放所有本线程读锁 todo?
      releaseSessionAllReadLock(session);
    }
    Boolean newSetLock = tb.setWLock();
    if (!newSetLock) return;
    //    if (!writeLockMap.containsKey(session)) {
    //      writeLockMap.put(session, new ArrayList<>());
    //    }
    //    ArrayList<ReentrantReadWriteLock.WriteLock> currentSessionWriteLockList =
    //        writeLockMap.get(session)==null?new ArrayList<>():writeLockMap.get(session);
    //    currentSessionWriteLockList.add(tb.getTable().lock.writeLock());
    //    writeLockMap.put(session, currentSessionWriteLockList);
    if (!writeSessionMap.containsKey(session)) {
      // i从0遍历到9，看writeSessionMap中是否有i这个值，若没有这个值，在map中插入键值对<session,i>
      for (int i = 0; i < 10; i++) {
        if (!writeSessionMap.containsValue(i)) {
          writeSessionMap.put(session, i);
          if (writeLockListArray.get(i) == null) writeLockListArray.set(i, new ArrayList<>());
          break;
        }
      }
    }
    ArrayList<ReentrantReadWriteLock.WriteLock> currentSessionWriteLockList =
        writeLockListArray.get(writeSessionMap.get(session)) == null
            ? new ArrayList<>()
            : writeLockListArray.get(writeSessionMap.get(session));
    currentSessionWriteLockList.add(tb.getmTable().lock.writeLock());
    writeLockListArray.set(writeSessionMap.get(session), currentSessionWriteLockList);
  }

  public void getRLock(Long session, Table.TableProxy tb) {
    if (Config.CLOSE_LOCK) {
      return;
    }
    Boolean newSetLock = tb.setRLock();
    if (!newSetLock) return;
    //    if (!readLockMap.containsKey(session)) {
    //      readLockMap.put(session, new ArrayList<>());
    //    }
    //    ArrayList<ReentrantReadWriteLock.ReadLock> currentSessionReadLockList =
    //        readLockMap.get(session)==null?new ArrayList<>():readLockMap.get(session);
    //    currentSessionReadLockList.add(tb.getTable().lock.readLock());
    //    readLockMap.put(session, currentSessionReadLockList);
    if (!readSessionMap.containsKey(session)) {
      // i从0遍历到9，看readSessionMap中是否有i这个值，若没有这个值，在map中插入键值对<session,i>
      for (int i = 0; i < 10; i++) {
        if (!readSessionMap.containsValue(i)) {
          readSessionMap.put(session, i);
          if (readLockListArray.get(i) == null) readLockListArray.set(i, new ArrayList<>());
          break;
        }
      }
    }
    ArrayList<ReentrantReadWriteLock.ReadLock> currentSessionReadLockList =
        readLockListArray.get(readSessionMap.get(session)) == null
            ? new ArrayList<>()
            : readLockListArray.get(readSessionMap.get(session));
    currentSessionReadLockList.add(tb.getmTable().lock.readLock());
    readLockListArray.set(readSessionMap.get(session), currentSessionReadLockList);
  }

  public void releaseWriteLock(Long session, Table.TableProxy tb) {
    ReentrantReadWriteLock.WriteLock writeLock = tb.getmTable().lock.writeLock();
    if (Config.DEBUG_OUT_LOG) {
      System.out.println("release write lock of session:" + session + " from lock manager");
    }
    //    System.out.println(writeLockMap.toString());
    //    if (writeLockMap.containsKey(session) && writeLockMap.get(session).remove(writeLock)) {
    //      writeLock.unlock();
    //    }
    if (writeSessionMap.containsKey(session)
        && writeLockListArray.get(writeSessionMap.get(session)).remove(writeLock)) {
      writeLock.unlock();
    }
  }

  public void releaseSessionAllWriteLock(Long session) {
    if (Config.CLOSE_LOCK) {
      return;
    }
    //    if (!writeLockMap.containsKey(session)) {
    //      return;
    //    }
    if (!writeSessionMap.containsKey(session)) {
      return;
    }
    //    ArrayList<ReentrantReadWriteLock.WriteLock> sessionLockList = writeLockMap.get(session);
    ArrayList<ReentrantReadWriteLock.WriteLock> sessionLockList =
        writeLockListArray.get(writeSessionMap.get(session));
    for (ReentrantReadWriteLock.WriteLock lock : sessionLockList) {
      lock.unlock();
      if (Config.DEBUG_OUT_LOG) {
        System.out.println(
            "release write lock of session:" + session + " from lock manager" + lock.toString());
      }
    }
    sessionLockList.clear();
    writeLockListArray.set(writeSessionMap.get(session), sessionLockList);
    writeSessionMap.remove(session);

    //    writeLockMap.put(session, sessionLockList);
    if (writeLockListArray.get(writeSessionMap.get(session)).size() > 0) {
      System.out.println("wrong size");
    }
  }

  public void releaseSessionAllReadLock(Long session) {
    if (Config.CLOSE_LOCK) {
      return;
    }
    //    if (!readLockMap.containsKey(session)) {
    //      return;
    //    }
    if (!readSessionMap.containsKey(session)) {
      return;
    }
    ArrayList<ReentrantReadWriteLock.ReadLock> sessionLockList =
        readLockListArray.get(readSessionMap.get(session));
    for (ReentrantReadWriteLock.ReadLock lock : sessionLockList) {
      lock.unlock();
    }
    sessionLockList.clear();
    //    readLockMap.remove(session);
    readLockListArray.set(readSessionMap.get(session), sessionLockList);
    readSessionMap.remove(session);

    //    readLockMap.put(session, sessionLockList);
  }

  public void releaseSessionAllLock(Long session) {
    releaseSessionAllWriteLock(session);
    releaseSessionAllReadLock(session);
  }
}
