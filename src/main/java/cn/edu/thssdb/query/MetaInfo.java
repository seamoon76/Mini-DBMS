package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;

import java.util.ArrayList;
import java.util.List;

public class MetaInfo {

  private String tableName;
  private List<Column> columns;

  public MetaInfo(String tableName, ArrayList<Column> columns) {
    this.tableName = tableName;
    this.columns = columns;
  }

  public int columnFind(String name) {
    ArrayList<String> colNames = new ArrayList<>();
    for (Column c : this.columns) {
      colNames.add(c.getName());
    }
    return colNames.indexOf(name);
  }
}
