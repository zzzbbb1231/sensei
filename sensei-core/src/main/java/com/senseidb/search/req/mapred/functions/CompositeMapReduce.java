package com.senseidb.search.req.mapred.functions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.util.Assert;

import com.senseidb.search.req.mapred.CombinerStage;
import com.senseidb.search.req.mapred.FacetCountAccessor;
import com.senseidb.search.req.mapred.FieldAccessor;
import com.senseidb.search.req.mapred.SenseiMapReduce;
import com.senseidb.search.req.mapred.impl.MapReduceRegistry;
import com.senseidb.util.JSONUtil;

public class CompositeMapReduce implements SenseiMapReduce<Serializable, Serializable> {
  private Map<String, SenseiMapReduce> innerFunctions = new HashMap<String, SenseiMapReduce>();

  @Override
  public void init(JSONObject params) {
    Iterator<String> keys = params.keys();
    while (keys.hasNext()) {
      String key = keys.next();
      JSONObject innerFunctionParams = params.optJSONObject(key);
      SenseiMapReduce senseiMapReduce = MapReduceRegistry.get(key);
      Assert.notNull(senseiMapReduce, "Could not retrieve map reduce function by the identifier");
      innerFunctions.put(key, senseiMapReduce);
      senseiMapReduce.init(innerFunctionParams);
    }

  }

  @Override
  public Serializable map(int[] docIds, int docIdCount, long[] uids, FieldAccessor accessor, FacetCountAccessor facetCountsAccessor) {
    HashMap<String, Serializable> mapResults = new HashMap<String, Serializable>(innerFunctions.size());
    for (String function : innerFunctions.keySet()) {
      SenseiMapReduce senseiMapReduce = innerFunctions.get(function);
      mapResults.put(function, senseiMapReduce.map(docIds, docIdCount, uids, accessor, facetCountsAccessor));
    }
    return mapResults;
  }

  @Override
  public List<Serializable> combine(List<Serializable> mapResults, CombinerStage combinerStage) {
    if (mapResults.size() < 2) {
      return mapResults;
    }
    HashMap<String, Serializable> firstResult = (HashMap<String, Serializable>) mapResults.get(0);
    Set<String> keys = firstResult.keySet();
    HashMap<String, ArrayList<Serializable>> resultsPerFunction = aggregate(mapResults, keys);
    firstResult.clear();
    for (String key : keys) {
      SenseiMapReduce function = combinerStage == CombinerStage.partitionLevel ? innerFunctions.get(key) : MapReduceRegistry.get(key);
      firstResult.put(key, (Serializable) function.combine(resultsPerFunction.get(key), combinerStage));
    }
    ArrayList<Serializable> ret = new ArrayList<Serializable>();
    ret.add(firstResult);
    return ret;
  }



  @Override
  public Serializable reduce(List<Serializable> combineResults) {
    if (combineResults.size() == 0) {
      return null;
    }
    if (combineResults.size() == 1) {
      return combineResults.get(0);
    }
    HashMap<String, Serializable> firstResult = (HashMap<String, Serializable>) combineResults.get(0);
    Set<String> keys = firstResult.keySet();
    HashMap<String, ArrayList<Serializable>> resultsPerFunction = aggregate(combineResults, keys);
    firstResult.clear();
    for (String key : keys) {
      SenseiMapReduce function = MapReduceRegistry.get(key);
      firstResult.put(key, (Serializable) function.reduce(resultsPerFunction.get(key)));
    }

    return firstResult;
  }
 
  private HashMap<String, ArrayList<Serializable>> aggregate(List<Serializable> mapResults, Set<String> keys) {
    HashMap<String, ArrayList<Serializable>> resultsPerFunction = new HashMap<String, ArrayList<Serializable>>(keys.size());
    for (String key : keys) {
      resultsPerFunction.put(key, new ArrayList<Serializable>());
    }
    for (Serializable mapResultRaw : mapResults) {
      HashMap<String, Serializable> mapResult = (HashMap<String, Serializable>) mapResultRaw;
      for (String key : keys) {

        resultsPerFunction.get(key).add(mapResult.get(key));
      }
    }
    return resultsPerFunction;
  }
  
  @Override
  public JSONObject render(Serializable reduceResultRaw) {
    try {
      HashMap<String, Serializable> reduceResult = (HashMap<String, Serializable>) reduceResultRaw;
      JSONObject ret = new JSONUtil.FastJSONObject();
      for (String key : innerFunctions.keySet()) {
        ret.put(key, innerFunctions.get(key).render(reduceResult.get(key)));
      }
      return ret;
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

}
