package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.ColumnType;

public class Column implements Comparable<Column> {
  private String name;
  private ColumnType type;
  private int primary;
  private boolean notNull;
  private int maxLength;

  public Column(String name, ColumnType type, int primary, boolean notNull, int maxLength) {
    this.name = name;
    this.type = type;
    this.primary = primary;
    this.notNull = notNull;
    this.maxLength = maxLength;
  }

  public int isPrimary() {
    return primary;
  }

  public void setPrimary(int primary) {
    this.primary = primary;
  }

  public void setNotNull(int isTrue) {
    this.notNull = isTrue == 1;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Override
  public int compareTo(Column e) {
    return name.compareTo(e.name);
  }

  public String toString() {
    return name + ',' + type + ',' + primary + ',' + notNull + ',' + maxLength;
  }

  public boolean cannotBeNull() {
    return notNull;
  }

  public ColumnType getType() {
    return type;
  }

  public int getMaxLength() {
    return maxLength;
  }
}
