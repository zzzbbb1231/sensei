package com.senseidb.search.req.mapred.functions;



import java.io.Serializable;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.search.req.mapred.CombinerStage;
import com.senseidb.search.req.mapred.FacetCountAccessor;
import com.senseidb.search.req.mapred.FieldAccessor;
import com.senseidb.search.req.mapred.SenseiMapReduce;

public class GroupByMapReduceJob implements SenseiMapReduce<Serializable, Serializable> {

  private String[] columns;
  private String metric;
  private String function;
  @Override
  public void init(JSONObject params) {
    params.put("mapReduce", new JSONObject().put("function", value).put("params", value));
    try {
      metric = params.getString("metric");
      function = params.getString("function");
      JSONArray columnsJson = params.getJSONArray("columns");
      columns = new String[columnsJson.length()];
      for (int i = 0; i < columnsJson.length(); i++) {
        columns[i] = columnsJson.getString(i);
      }
    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    }
    
  }

  @Override
  public Serializable map(int[] docIds, int docIdCount, long[] uids, FieldAccessor accessor, FacetCountAccessor facetCountsAccessor) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Serializable> combine(List<Serializable> mapResults, CombinerStage combinerStage) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Serializable reduce(List<Serializable> combineResults) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public JSONObject render(Serializable reduceResult) {
    // TODO Auto-generated method stub
    return null;
  }
  class GroupedValue implements Comparable {
    String key;
    Comparable value;

    public GroupedValue(String key, Comparable value) {
      super();
      this.key = key;
      this.value = value;
    }
    @Override
    public int compareTo(Object o) {
      return value.compareTo(o);
    }
    @Override
    public String toString() {
      return key + ", count=" + value;
    }
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + getOuterType().hashCode();
      result = prime * result + ((key == null) ? 0 : key.hashCode());
      result = prime * result + ((value == null) ? 0 : value.hashCode());
      return result;
    }
    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      GroupedValue other = (GroupedValue) obj;
      if (!getOuterType().equals(other.getOuterType()))
        return false;
      if (key == null) {
        if (other.key != null)
          return false;
      } else if (!key.equals(other.key))
        return false;
      if (value == null) {
        if (other.value != null)
          return false;
      } else if (!value.equals(other.value))
        return false;
      return true;
    }
    private GroupByMapReduceJob getOuterType() {
      return GroupByMapReduceJob.this;
    }
  }
  private String getKey(String[] columns, FieldAccessor fieldAccessor, int docId) {
    StringBuilder key = new StringBuilder(fieldAccessor.get(columns[0], docId).toString());
    for (int i = 1; i < columns.length; i++) {
      key.append(":").append(fieldAccessor.get(columns[i], docId).toString());
    }
    return key.toString();
  }
}
