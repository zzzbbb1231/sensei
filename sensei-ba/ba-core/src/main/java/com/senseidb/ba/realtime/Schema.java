package com.senseidb.ba.realtime;

import org.json.JSONArray;
import org.json.JSONObject;

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

  public Object[] fromJson(JSONObject jsonObject) {
    try {
      Object[] ret = new Object[columnNames.length];
      for (int i = 0; i < columnNames.length; i++) {
        Object val = jsonObject.opt(columnNames[i]);
        if (val instanceof JSONArray) {
          JSONArray arr = (JSONArray) val;
          Object[] vals = new Object[arr.length()];
          for (int j = 0; j < arr.length(); j++) {
            vals[j] = arr.get(j);
          }
          val = vals;
        } else if (types[i].isMulti() && val instanceof String) {
          Object[] objects = ((String)val).split(",");
          if (types[i] != ColumnType.STRING_ARRAY) {
            for (int j = 0; j < objects.length; j++) {
              if ("".equals(objects[j])) {
                objects[j] = null;
              }
            }
          }
          val = objects;
        } else if ("".equals(val) && !types[i].isMulti() && types[i] != ColumnType.STRING) {
          val = null;
        }
         
        ret[i] = val;
        
      }
      return ret;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

}
