package com.senseidb.search.req.mapred.functions.groupby;

import java.io.Serializable;
import java.util.HashMap;

import org.json.JSONObject;

import com.senseidb.search.req.mapred.FieldAccessor;

public interface AggregateFunction<T extends Serializable> {
  public GroupedValue<T> produceSingleValue(FieldAccessor accessor, int docId);
  public JSONObject toJson(HashMap<String, GroupedValue<Serializable>> reduceResult); 
  
  public static interface GroupedValue<T extends Serializable> extends Comparable<T> {
      //AggregateFunction getFunction(FieldAccessor accessor, int docId);
      void merge(GroupedValue<T> anotherValue);
      
  }
  
  
}
