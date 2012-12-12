package com.senseidb.search.req.mapred.functions.groupby;

import java.io.Serializable;
import java.util.HashMap;

import org.json.JSONObject;

import com.senseidb.search.req.mapred.FieldAccessor;
public interface AggregateFunction<T extends GroupedValue> extends Serializable {
  public T produceSingleValue(FieldAccessor accessor, int docId);
  public Object toJson(HashMap<String, T> reduceResult); 
  
}
