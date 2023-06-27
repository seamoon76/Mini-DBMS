package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.utils.Cell;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class QueryResult {

  private List<MetaInfo> metaInfoInfos;
  private List<Integer> index;
  private List<Cell> attrs;
  private List<Row> results;

  private List<String> columnNames;

  public enum QueryReturnType {
    SELECT,
    MESSAGE
  };

  public String errorMessage = "";

  public QueryReturnType resultType;

  public QueryResult(QueryTable queryTable) {
    this.index = new ArrayList<>();
    this.attrs = new ArrayList<>();
    this.resultType = QueryReturnType.SELECT;
    this.errorMessage = null;
    this.results = queryTable.rows_results;
    this.columnNames = new ArrayList<>();
    for (Column column : queryTable.columns) {
      columnNames.add(column.getName());
    }
  }

  public List<String> getColumnNames() {
    return this.columnNames;
  }

  public QueryReturnType getResultType() {
    return resultType;
  }

  public List<List<String>> getRowStringList() {
    List<List<String>> res = new ArrayList<>();
    for (int i = 0; i < this.results.size(); i++) {
      List<String> temp = new ArrayList<>();
      Row r = this.results.get(i);
      for (int j = 0; j < r.getEntries().size(); j++) {
        temp.add(r.getEntries().get(j).toString());
      }
      res.add(temp);
    }
    return res;
  }

  public QueryResult(String Message) {
    resultType = QueryReturnType.MESSAGE;
    this.errorMessage = Message;
  }

  public static Row combineRow(LinkedList<Row> rows) {
    // TODO
    return null;
  }

  // get results
  public List<Row> getResults() {
    return this.results;
  }

  public Row generateQueryRecord(Row row) {
    return null;
  }
}
