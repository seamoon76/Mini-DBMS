package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class QueryTable implements Iterator<Row> {

  public List<Row> rows_results;
  public List<Column> columns;

  public QueryTable(Table table) {
    rows_results = new ArrayList<>();
    columns = new ArrayList<>();
    columns.addAll(table.columns);
    for (Row row : table) {
      rows_results.add(row);
    }
  }

  public QueryTable(List<Row> rows, List<Column> columns) {
    this.rows_results = new ArrayList<>();
    this.columns = new ArrayList<>();
    this.rows_results.addAll(rows);
    this.columns.addAll(columns);
  }

  public QueryTable() {
    this.rows_results = new ArrayList<>();
    this.columns = new ArrayList<>();
  }

  @Override
  public boolean hasNext() {
    return rows_results.iterator().hasNext();
  }

  @Override
  public Row next() {
    return rows_results.iterator().next();
  }

  public int Column2Index(String columnName) {
    ArrayList<String> colNames = new ArrayList<>();
    for (Column column : this.columns) {
      colNames.add(column.getName());
    }
    return colNames.indexOf(columnName);
  }

  public QueryTable appendQueryTable(QueryTable queryTable) {
    List<Row> newRows = new ArrayList<>();
    List<Column> newColumns = new ArrayList<>(this.columns);
    newColumns.addAll(queryTable.columns);
    for (Row row1 : this.rows_results) {
      for (Row row2 : queryTable.rows_results) {
        Row newRow = new Row(row1.getEntries().toArray(new Entry[0]));
        newRow.appendEntries(row2.getEntries());
        newRows.add(newRow);
      }
    }
    return new QueryTable(newRows, newColumns);
  }

  public QueryTable joinTable(Table table) {
    Table fullNameTable = table.toFullTable();
    QueryTable queryTable = new QueryTable(fullNameTable);
    return this.appendQueryTable(queryTable);
  }
}
