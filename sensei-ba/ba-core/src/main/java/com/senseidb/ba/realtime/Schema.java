package com.senseidb.ba.realtime;

import com.senseidb.ba.gazelle.ColumnType;

public class Schema {
  private String[] columnNames;
  private ColumnType[] types;
  public Schema(String[] columnNames, ColumnType[] types) {
    super();
    this.columnNames = columnNames;
    this.types = types;
  }
  public String[] getColumnNames() {
    return columnNames;
  }
  public ColumnType[] getTypes() {
    return types;
  }
  
}
