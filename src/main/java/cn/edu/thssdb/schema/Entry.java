package cn.edu.thssdb.schema;

import java.io.Serializable;

public class Entry implements Comparable<Entry>, Serializable {
  private static final long serialVersionUID = -5809782578272943999L;
  public Comparable value;

  public Entry(Comparable value) {
    this.value = value;
  }

  public Entry() {
    this.value = null;
  }

  @Override
  public int compareTo(Entry e) {
    return value.compareTo(e.value);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) return false;
    if (this.getClass() != obj.getClass()) return false;
    Entry e = (Entry) obj;
    return value.equals(e.value);
  }

  public String toString() {
    if (value != null) {
      return value.toString();
    } else {
      return "null";
    }
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }

  public String getType() {
    if (this.value == null) return "null";
    String valueClassString = this.value.getClass().toString();
    if (valueClassString.contains("Integer")) return "INT";
    if (valueClassString.contains("Long")) return "LONG";
    if (valueClassString.contains("Float")) return "FLOAT";
    if (valueClassString.contains("Double")) return "DOUBLE";
    if (valueClassString.contains("String")) return "STRING";
    return "UNKNOWN";
  }
}
