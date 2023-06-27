package cn.edu.thssdb.schema;

import cn.edu.thssdb.Config;
import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.query.MetaInfo;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Pair;
import cn.edu.thssdb.utils.StorageUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static cn.edu.thssdb.type.ColumnType.STRING;

public class Table implements Iterable<Row> {
  public ReentrantReadWriteLock lock;
  private String databaseName;
  public String tableName;
  public ArrayList<Column> columns;
  public BPlusTree<Entry, Row> index;
  public boolean useMultiPrimary;
  private ArrayList<Integer> multiPrimaryIndex;
  private int primaryIndex;
  private StorageUtil<Row> storageUtil;

  private Meta tableMeta;

  public Table(String databaseName, String tableName, Column[] columns, boolean multiPrimary) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    String dir =
        Config.DBMS_ROOT_DIR
            + File.separator
            + "DATA"
            + File.separator
            + databaseName
            + File.separator
            + tableName;
    String meta_filename = tableName + "Meta.txt";
    String data_filename = tableName + ".gz";
    this.storageUtil = new StorageUtil<>(dir, data_filename, Config.USE_GZIP);
    this.tableMeta = new Meta(dir, meta_filename);
    this.lock = new ReentrantReadWriteLock();
    this.columns = new ArrayList<>(Arrays.asList(columns));
    //    this.primaryIndex = primaryIndex;
    this.index = new BPlusTree<>();
    this.primaryIndex = -1;
    this.useMultiPrimary = multiPrimary;
    this.multiPrimaryIndex = new ArrayList<>();

    if (!useMultiPrimary) {
      for (int i = 0; i < this.columns.size(); i++) {
        if (this.columns.get(i).isPrimary() == 1) {
          if (this.primaryIndex >= 0) throw new MultiPrimaryException(this.tableName);
          this.primaryIndex = i;
        }
      }
    } else {
      for (int i = 0; i < this.columns.size(); i++) {
        if (this.columns.get(i).isPrimary() == 1) {
          this.multiPrimaryIndex.add(i);
          if (this.columns.get(i).getType().equals(ColumnType.MULTI_PRIMARY)) {
            this.primaryIndex = i;
          }
          // throw new MultiPrimaryException(this.tableName);
        }
      }
    }

    if (this.primaryIndex < 0) throw new NoPrimaryException(this.tableName);
  }

  // 通过database的meta恢复的表
  public Table(String databaseName, String tableName) {
    // 在这里根据表的meta文件读取出column信息
    this.databaseName = databaseName;
    this.tableName = tableName;
    String dir =
        Config.DBMS_ROOT_DIR
            + File.separator
            + "DATA"
            + File.separator
            + databaseName
            + File.separator
            + tableName;
    String meta_filename = tableName + "Meta.txt";
    String data_filename = tableName + ".gz";
    this.storageUtil = new StorageUtil<>(dir, data_filename, Config.USE_GZIP);
    this.tableMeta = new Meta(dir, meta_filename);
    this.lock = new ReentrantReadWriteLock();
    this.multiPrimaryIndex = new ArrayList<>();
    this.columns = new ArrayList<>();
    this.index = new BPlusTree<>();
    recoverTableMeta();
    recover();
  }

  private void recoverTableMeta() {

    ArrayList<String[]> table_meta_content = this.tableMeta.readFromFile();
    try {
      String[] host_database_name = table_meta_content.get(0);
      if (!host_database_name[0].equals(Config.DATABASE_NAME_FORMAT)) {
        throw new ReadFromFileException();
      }
      if (!this.databaseName.equals(host_database_name[1])) {
        throw new ReadFromFileException();
      }
      String[] table_name = table_meta_content.get(1);
      if (!table_name[0].equals(Config.TABLE_NAME_FORMAT)) {
        throw new ReadFromFileException();
      }
      if (!this.tableName.equals(table_name[1])) {
        throw new ReadFromFileException();
      }
      String[] primary_key = table_meta_content.get(2);
      if (!primary_key[0].equals(Config.PRIMARY_KEY_INDEX_FORMAT)) {
        throw new ReadFromFileException();
      }
      if (primary_key.length > 2) {
        this.useMultiPrimary = true;
        for (int i = 1; i < primary_key.length; i++) {
          this.multiPrimaryIndex.add(Integer.parseInt(primary_key[i]));
        }
        this.primaryIndex = this.multiPrimaryIndex.get(this.multiPrimaryIndex.size() - 1);
      } else if (primary_key.length == 2) {
        this.useMultiPrimary = false;
        this.primaryIndex = Integer.parseInt(primary_key[1]);
      } else {
        throw new ReadFromFileException();
      }
      // this.primaryIndex = Integer.parseInt(primary_key[1]);
      for (int i = 3; i < table_meta_content.size(); i++) {
        String[] col_meta = table_meta_content.get(i);
        String name = col_meta[0];
        ColumnType ctype = ColumnType.str2Type(col_meta[1]);
        int primary = Integer.parseInt(col_meta[2]);
        //        boolean primary_bool = col_meta[2].equals("true");
        //        int primary = primary_bool ? 1 : 0;

        boolean notNull = col_meta[3].equals("true");
        int maxLength = Integer.parseInt(col_meta[4]);
        this.columns.add(new Column(name, ctype, primary, notNull, maxLength));
      }
    } catch (Exception e) {
      throw new ReadFromFileException();
    }
  }

  private void recover() {
    ArrayList<Row> rows = deserialize();
    for (Row row : rows) {
      // index.put(row.getEntries().get(primaryIndex), row);
      if (!this.useMultiPrimary) {
        index.put(row.getEntries().get(primaryIndex), row);
      } else {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < this.multiPrimaryIndex.size() - 1; i++) {
          // 特别注意，这是我们合成的主键，不是原来的主键，记住格式
          sb.append(row.getEntries().get(this.multiPrimaryIndex.get(i)).toString());
        }
        Entry e = new Entry(sb.toString());
        index.put(e, row);
      }
    }
  }

  public Row getRow(Entry primEntry) {
    return index.get(primEntry);
  }

  public void insert(Row row) {
    // multi primary
    if (this.useMultiPrimary) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < this.multiPrimaryIndex.size() - 1; i++) {
        // 特别注意，这是我们合成的主键，不是原来的主键，记住格式
        sb.append(row.getEntries().get(this.multiPrimaryIndex.get(i)).toString());
      }
      Entry e = new Entry(sb.toString());
      row.appendEntry(e);
    } else {
      this.checkRow(row);
    }

    if (this.index.contains(row.getEntries().get(this.primaryIndex)))
      throw new DuplicateKeyException();
    this.index.put(row.getEntries().get(this.primaryIndex), row);
  }

  // 删除所有行
  public void deleteAll() {
    index.clear();
    index = new BPlusTree<>();
  }

  public void alterByAddColumn(ArrayList<Column> columns) {
    // add the last column
    this.columns = columns;
    Column columnToAdd = this.columns.get(this.columns.size() - 1);
    // add the last column as null to evert row, use index iterator
    for (Pair<Entry, Row> entryRowPair : this.index) {
      Row row = entryRowPair.right;
      row.appendEntry(new Entry(null));
      this.index.update(entryRowPair.left, row);
    }
    //    this.deleteAll();
  }
  // drop column
  public void alterByDropColumn(ArrayList<Column> columns, int indexToDrop) {
    // set columns
    this.columns = columns;
    // drop the indexToDrop column
    for (Pair<Entry, Row> entryRowPair : this.index) {
      Row row = entryRowPair.right;
      row.getEntries().remove(indexToDrop);
      this.index.update(entryRowPair.left, row);
    }
    //    this.deleteAll();
  }

  // Rename column
  public void alterByRenameColumn(ArrayList<Column> columns, int indexToUpdate) {
    // set columns
    this.columns = columns;
    // update the indexToUpdate column, do nothing
  }

  public void alterByChangeColumn(ArrayList<Column> columns) {
    // set columns
    this.columns = columns;
    this.deleteAll();
  }

  public void alterByChangeConstraint(ArrayList<Column> columns) {
    // set columns
    this.columns = columns;
    this.deleteAll();
  }

  // 删除某一行
  public void deleteRow(Row row) {
    // multi primary
    if (this.useMultiPrimary) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < this.multiPrimaryIndex.size() - 1; i++) {
        // 特别注意，这是我们合成的主键，不是原来的主键，记住格式
        sb.append(row.getEntries().get(this.multiPrimaryIndex.get(i)).toString());
      }
      Entry e = new Entry(sb.toString());
      row.appendEntry(e);
    } else {
      this.checkRow(row);
    }
    if (!this.index.contains(row.getEntries().get(this.primaryIndex)))
      throw new KeyNotExistException();
    this.index.remove(row.getEntries().get(this.primaryIndex));
  }

  public int name2Index(String colName) {
    MetaInfo metaInfo = new MetaInfo(this.tableName, columns);
    return metaInfo.columnFind(colName);
  }

  public void update(Entry primaryEntry, Row newRow) {
    // multi primary
    if (this.useMultiPrimary) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < this.multiPrimaryIndex.size() - 1; i++) {
        // 特别注意，这是我们合成的主键，不是原来的主键，记住格式
        sb.append(newRow.getEntries().get(this.multiPrimaryIndex.get(i)).toString());
      }
      Entry e = new Entry(sb.toString());
      newRow.appendEntry(e);
    } else {
      this.checkRow(newRow);
    }
    Row oldRow = this.index.get(primaryEntry);
    if (oldRow == null) throw new KeyNotExistException();
    this.index.remove(primaryEntry);
    this.index.put(newRow.getEntries().get(this.primaryIndex), newRow);
  }

  private void serialize() {
    this.storageUtil.serialize(iterator());
  }

  private ArrayList<Row> deserialize() {
    return this.storageUtil.deserialize();
  }

  public void persist() {
    serialize();
    ArrayList<String> table_meta_content = new ArrayList<>();
    table_meta_content.add(Config.DATABASE_NAME_FORMAT + " " + databaseName);
    table_meta_content.add(Config.TABLE_NAME_FORMAT + " " + tableName);
    if (this.useMultiPrimary) {
      StringBuilder str = new StringBuilder("" + Config.PRIMARY_KEY_INDEX_FORMAT);
      for (Integer integer : this.multiPrimaryIndex) {
        str.append(" ").append(integer);
      }
      table_meta_content.add(str.toString());
    } else {
      table_meta_content.add(Config.PRIMARY_KEY_INDEX_FORMAT + " " + primaryIndex);
    }
    for (Column column : columns) {
      table_meta_content.add(column.toString().replace(",", " "));
    }
    this.tableMeta.writeToFile(table_meta_content);
  }

  public Table toFullTable() {
    ArrayList<Column> columnArrayList = new ArrayList<>();
    for (Column col : columns) {
      String newName = this.tableName + "_" + col.getName();
      Column newCol =
          new Column(
              newName, col.getType(), col.isPrimary(), col.cannotBeNull(), col.getMaxLength());
      columnArrayList.add(newCol);
    }
    Column[] modifiedColumns = columnArrayList.toArray(new Column[0]);
    Table resTable = new Table(this.databaseName, this.tableName, modifiedColumns, false);
    resTable.index = this.index;
    return resTable;
    //    return new Table(databaseName, tableName, columns.toArray(new Column[0]));
  }

  private class TableIterator implements Iterator<Row> {
    private Iterator<Pair<Entry, Row>> iterator;

    TableIterator(Table table) {
      this.iterator = table.index.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Row next() {
      return iterator.next().right;
    }
  }

  @Override
  public Iterator<Row> iterator() {
    return new TableIterator(this);
  }

  public class TableProxy implements AutoCloseable {
    private Table mTable;
    private Boolean hasRLock;
    private Boolean hasWLock;

    public TableProxy(Table table, Boolean hasRead, Boolean hasWrite) {
      this.mTable = table;
      this.hasRLock = hasRead;
      this.hasWLock = hasWrite;
      if (hasRead) {
        if (Config.DEBUG_OUT_LOG) {
          System.out.println("access read lock" + this.mTable.tableName);
        }
        this.mTable.lock.readLock().lock();
      }
      if (hasWrite) {
        if (Config.DEBUG_OUT_LOG) {
          System.out.println("access write lock" + this.mTable.tableName);
        }

        this.mTable.lock.writeLock().lock();
      }
    }

    public Boolean setWLock() {
      if (this.hasRLock) {
        this.mTable.lock.readLock().unlock();
        if (Config.DEBUG_OUT_LOG) {
          System.out.println("release read lock " + this.mTable.tableName);
        }
        this.hasRLock = false;
      }
      if (this.mTable.lock.isWriteLockedByCurrentThread()) {
        return false;
      }

      this.mTable.lock.writeLock().lock();
      this.hasWLock = true;
      if (Config.DEBUG_OUT_LOG) {
        System.out.println("get write lock " + this.mTable.tableName);
      }
      return true;
    }

    public Boolean setRLock() {
      if (this.mTable.lock.isWriteLockedByCurrentThread()) {
        return false;
      }
      if (Config.DEBUG_OUT_LOG) {
        System.out.println("get read lock " + this.mTable.tableName);
      }

      this.mTable.lock.readLock().lock();
      this.hasRLock = true;
      return true;
    }

    public Table getmTable() {
      return this.mTable;
    }

    @Override
    public void close() {
      if (Config.CLOSE_LOCK) {
        return;
      }
      // Read Committed
      if (Config.ISOLATION_LEVEL == "READ_COMMITTED") {
        if (this.hasRLock) {
          if (Config.DEBUG_OUT_LOG) {
            System.out.println("release read lock " + this.mTable.tableName);
          }
          this.mTable.lock.readLock().unlock();
          this.hasRLock = false;
        }
      } else if (Config.ISOLATION_LEVEL == "SERIALIZATION") {
        // 不释放读锁，等事务完成之后才释放
      }
    }
  }

  public TableProxy getTableProxy() {
    if (Config.CLOSE_LOCK) {
      return new TableProxy(this, false, false);
    }
    if (Objects.equals(Config.ISOLATION_LEVEL, "READ_COMMITTED")) {
      return new TableProxy(this, false, false);
    } else if (Objects.equals(Config.ISOLATION_LEVEL, "SERIALIZATION")) {
      return new TableProxy(this, false, false);
    } else {
      return new TableProxy(this, true, false);
    }
  }

  // get primary index
  public int getPrimaryIndex() {
    return primaryIndex;
  }

  private void checkRow(Row row) {
    if (useMultiPrimary) {
      if (row.getEntries().size() != this.columns.size() + 1)
        throw new MyException(
            "expected "
                + this.columns.size()
                + " length but got "
                + row.getEntries().size()
                + " \n ");
    } else {
      if (row.getEntries().size() != this.columns.size())
        throw new MyException(
            "expected "
                + this.columns.size()
                + " length but got "
                + row.getEntries().size()
                + " \n ");
    }
    for (int i = 0; i < row.getEntries().size() - 1; i++) {
      String entryType = row.getEntries().get(i).getType();
      Column col = this.columns.get(i);
      if (entryType.equals("null")) {
        if (col.cannotBeNull()) throw new MyException(col.getName() + " cannot be null");
      } else {
        if (!entryType.equals(col.getType().name()))
          throw new MyException("Entry Value Format Invalid! when check row in table");
        Comparable entryValue = row.getEntries().get(i).value;
        if (entryType.equals(STRING.name()))
          if (((String) entryValue).length() - 2 > col.getMaxLength()) {
            throw new MyException(
                "Entry String Exceed Max length ,"
                    + col.getName()
                    + ",length is"
                    + ((String) entryValue).length()
                    + "maxLength is "
                    + col.getMaxLength()
                    + "(when check row valid in table)");
          }
      }
    }
  }
}
