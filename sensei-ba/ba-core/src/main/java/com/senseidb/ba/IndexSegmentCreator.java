package com.senseidb.ba;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

public class IndexSegmentCreator {
    public static IndexSegment convert(String[] jsonDocs, Set<String> excludedColumns)  {
      Map<String, List> columnValues = new HashMap<String, List>();
      try {
      Map<String, ColumnType> columnTypes = getColumnTypes(jsonDocs, excludedColumns, 10);
      for (String column : columnTypes.keySet()) {
        columnValues.put(column, new ArrayList(jsonDocs.length));
      }
      for (String jsonDocStr : jsonDocs) {        
        
        JSONObject jsonDoc = new JSONObject(jsonDocStr);
        for (String column : columnTypes.keySet()) {
          Object value = jsonDoc.opt(column);
          if (value instanceof Integer && columnTypes.get(column) == ColumnType.LONG) {
            value = Long.valueOf((Integer) value);
          }
          columnValues.get(column).add(value);
        }
      } 
      IndexSegmentImpl offlineSegmentImpl = new IndexSegmentImpl(); 
      for (String column : columnTypes.keySet()) {
        ForwardIndexBackedByArray forwardIndexBackedByArray = new ForwardIndexBackedByArray(column, columnTypes.get(column));
        ColumnType type = columnTypes.get(column);
        if (type == ColumnType.INT) {
          forwardIndexBackedByArray.initByIntValues(columnValues.get(column));
        } else if (type == ColumnType.LONG) {
          forwardIndexBackedByArray.initByLongValues(columnValues.get(column));
        } else if (type == ColumnType.STRING) {
          forwardIndexBackedByArray.initByStringValues(columnValues.get(column));
        }
        offlineSegmentImpl.forwardIndexes.put(column, forwardIndexBackedByArray);
        offlineSegmentImpl.dictionaries.put(column, forwardIndexBackedByArray.getDictionary());
      }
      offlineSegmentImpl.length = jsonDocs.length;
      offlineSegmentImpl.setColumnTypes(columnTypes);
      return offlineSegmentImpl;
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }

    public static Map<String, ColumnType> getColumnTypes(String[] jsonDocs, Set<String> excludedColumns, int mod)
        throws JSONException {
      Map<String, ColumnType> columnTypes = new HashMap<String, ColumnType>();
      int i = 0; 
      for (String jsonDocStr : jsonDocs) {
        if (i++ % mod != 0) {
          continue;
        }
        JSONObject jsonDoc = new JSONObject(jsonDocStr);
        if (jsonDoc == null) {
          throw new IllegalStateException();
        }
        Iterator keys = jsonDoc.keys();
        while (keys.hasNext()) {
          String key = (String) keys.next();
          if (excludedColumns.contains(key)) {
            continue;
          }
          if (!columnTypes.containsKey(key) || columnTypes.get(key) == ColumnType.INT) {
            Object object = jsonDoc.get(key);
             if (columnTypes.get(key) == ColumnType.INT && object instanceof Long) {
               columnTypes.put(key, ColumnType.LONG);
             } else {            
                if (object instanceof String) columnTypes.put(key, ColumnType.STRING);
                if (object instanceof Integer) columnTypes.put(key, ColumnType.INT);
                if (object instanceof Long) columnTypes.put(key, ColumnType.LONG);
             }
          }
        }
      }
      return columnTypes;
    }
}
